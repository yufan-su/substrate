# Copyright 2026 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Long-running liveness probe: does Substrate still work N hours from now?

Unlike the other user classes, this one is not trying to find a throughput
ceiling. Each user brings up exactly one actor and then loops for the whole
test, doing two things:

  * `ActorPing` on every iteration — a round trip through the router to the
    actor and back. This covers the data path (router -> worker -> sandbox ->
    actor).
  * `ActorSuspend` / `ActorResume` every `--soak-pings-per-cycle` iterations —
    a full suspend/resume of the actor. This covers the lifecycle path
    (checkpoint to the snapshot store, worker release, scheduler placement,
    snapshot restore).

The cycle is not optional decoration. Pinging alone leaves the actor in
STATUS_RUNNING forever, so resume gets exercised exactly once, in `on_start`,
at t=0 — and with `boot=True`, which skips snapshots entirely. A restore that
degrades at hour 18, a scheduler that leaks worker assignments, or a snapshot
store that slows as it fills would all be invisible. Cycling turns "the data
path stayed up" into "the lifecycle stayed up too", and makes the suspend and
resume latency percentiles a drift signal in their own right.

Run it from the web UI with a small user count and a long wait time — a handful
of users at `--min-wait-time=10 --max-wait-time=15` is the intended shape. Note
that a cycling user releases its worker mid-cycle and needs a free slot to
resume onto; a worker holds exactly one actor. See the soak section of
benchmarking/README.md.
"""

from locust import User, task, events
from common.grpc_setup import init_grpc_gevent

# Patch gRPC to cooperate with locust's gevent loop before any channel exists.
init_grpc_gevent()

import logging
import time
import uuid

import grpc
import requests
from locust.exception import StopUser
from opentelemetry.propagate import inject

from common import ateapi_pb2
from common import ateapi_pb2_grpc
from common import glutton_pb2
from common.ateapi_channel import DEFAULT_MAX_AGE_SECONDS, RotatingChannel
from common.atespace import ATESPACE, ensure_atespace
from common.grpc_tracing import traced_grpc
from common.metrics import init_metrics, update_user_count
from common.trace import init_tracing, get_tracer
from common.wait_time import init_wait_time, dynamic_wait_time

logger = logging.getLogger(__name__)

init_tracing()
init_metrics()
init_wait_time()

tracer = get_tracer(__name__)

# Atenet router fronts all actor traffic; the Host header selects the actor.
ROUTER_URL = "http://atenet-router.ate-system.svc.cluster.local"
ACTOR_DOMAIN = "actors.resources.substrate.ate.dev"

# The only route glutton exposes in --mode=http. Body in, body out, both
# proto.Marshal'd (cmd/benchmarking/glutton/main.go).
PING_PATH = "/ping"

# A cold boot pulls the image and starts a gVisor sandbox, so the first
# transition to RUNNING is far slower than any later operation. Generous,
# because giving up here costs the user for the rest of the run.
STARTUP_TIMEOUT_SECONDS = 300.0
STARTUP_POLL_SECONDS = 2.0

# Well above any healthy ping. Long enough that a slow-but-alive system reads
# as slow rather than broken, short enough that a black hole is not mistaken
# for a hang.
PING_TIMEOUT_SECONDS = 30.0

# Budget for one suspend or one resume to reach its terminal status. Shorter
# than STARTUP_TIMEOUT_SECONDS on purpose: a snapshot restore taking as long as
# a cold boot is itself the degradation this test is looking for, so the limit
# should not be so loose that it hides one. Still generous enough that a merely
# busy cluster does not trip it.
CYCLE_TIMEOUT_SECONDS = 180.0

# Pings between suspend/resume cycles, per user. At the intended 10-15s wait
# that is a cycle roughly every five minutes, so a 24h run collects a few
# hundred samples per user — enough to see drift — while keeping the ping loop
# the dominant source of liveness signal.
DEFAULT_PINGS_PER_CYCLE = 24

TEMPLATE_NAMESPACE = "benchmark-workloads"
TEMPLATE_NAME = "glutton"

_initialized = False


def _init_soak_options() -> None:
    """Register soak-specific CLI flags (idempotent, as the common inits are)."""
    global _initialized
    if _initialized:
        return

    @events.init_command_line_parser.add_listener
    def on_init_parser(parser) -> None:
        parser.add_argument(
            "--soak-channel-max-age",
            type=float,
            default=DEFAULT_MAX_AGE_SECONDS,
            env_var="LOCUST_SOAK_CHANNEL_MAX_AGE",
            help=(
                "Seconds before a SoakUser rebuilds its ateapi channel to pick "
                "up a rotated pod certificate. Lower it to exercise the rebuild "
                "path without waiting out a real rotation."
            ),
            include_in_web_ui=True,
        )
        parser.add_argument(
            "--soak-pings-per-cycle",
            type=int,
            default=DEFAULT_PINGS_PER_CYCLE,
            env_var="LOCUST_SOAK_PINGS_PER_CYCLE",
            help=(
                "Pings between suspend/resume cycles, per user. The cycle is "
                "what exercises checkpoint, scheduling and snapshot restore; "
                "with 0 the run only tests the data path and the lifecycle is "
                "covered once, at startup."
            ),
            include_in_web_ui=True,
        )

    _initialized = True


_init_soak_options()


class PingFailed(Exception):
    """A ping that did not come back, or came back wrong.

    Carries the classified message from _classify_failure so the Failures tab
    in the web UI is self-explanatory hours after the fact.
    """


class CycleFailed(Exception):
    """A suspend or resume that errored or never reached its target status."""


class SoakUser(User):
    wait_time = dynamic_wait_time

    # `host` is what locust shows in the web UI / --host flag; it can be
    # overridden by the user at test start. Keep the api target in a separate
    # attribute so it's not clobbered when host points elsewhere (e.g. when
    # running with other user classes via --class-picker).
    host = "api.ate-system.svc.cluster.local:443"
    api_host = "api.ate-system.svc.cluster.local:443"

    def on_start(self) -> None:
        # Set every attribute on_stop touches before anything can raise, so a
        # failure partway through startup still tears down cleanly.
        self.channel = None
        self.http_session = None
        self.actor_created = False
        self._counted = False
        self._pings_since_cycle = 0
        self.actor_name = f"sb-{uuid.uuid4()}"
        self.actor_ref = ateapi_pb2.ObjectRef(atespace=ATESPACE, name=self.actor_name)
        self.ping_url = f"{ROUTER_URL}{PING_PATH}"
        self.host_header = f"{self.actor_name}.{ATESPACE}.{ACTOR_DOMAIN}"

        update_user_count(1, self.__class__.__name__)
        self._counted = True

        # Unlike the short tests, a soak user outlives the pod certificate it
        # starts with: cmd/podcertcontroller caps the lifetime at 24h and
        # kubelet rewrites the bundle 30 minutes before that. RotatingChannel
        # re-reads it from disk periodically so the run does not fail at hour
        # 23.5 for reasons that have nothing to do with Substrate's health.
        self.channel = RotatingChannel(
            self.api_host,
            ateapi_pb2_grpc.ControlStub,
            max_age_seconds=self._channel_max_age(),
        )

        try:
            ensure_atespace(self.channel.stub, self.__class__.__name__)
        except Exception as e:
            logger.error(f"Failed to ensure atespace {ATESPACE}: {e}")
            self._abort(delete_actor=False)

        try:
            self._call(
                "CreateActor",
                lambda stub, md: stub.CreateActor.with_call(
                    ateapi_pb2.CreateActorRequest(
                        actor=ateapi_pb2.Actor(
                            metadata=ateapi_pb2.ResourceMetadata(
                                atespace=ATESPACE, name=self.actor_name
                            ),
                            actor_template_namespace=TEMPLATE_NAMESPACE,
                            actor_template_name=TEMPLATE_NAME,
                        )
                    ),
                    metadata=md,
                ),
            )
        except Exception as e:
            logger.error(f"Failed to create actor {self.actor_name}: {e}")
            self._abort(delete_actor=False)
        self.actor_created = True

        # CreateActor registers the actor as STATUS_SUSPENDED; it does not run
        # it (cmd/ateapi/internal/controlapi/create_actor.go). boot=True skips
        # the golden snapshot, which does not exist yet for a fresh actor.
        try:
            self._call(
                "ResumeActorColdStart",
                lambda stub, md: stub.ResumeActor.with_call(
                    ateapi_pb2.ResumeActorRequest(actor=self.actor_ref, boot=True),
                    metadata=md,
                ),
            )
        except Exception as e:
            logger.error(f"Failed to resume actor {self.actor_name}: {e}")
            self._abort(delete_actor=True)

        self._await_running()

        # One HTTP session per user, held for the whole run: keeping the
        # connection to the router warm is part of what we are testing.
        self.http_session = requests.Session()
        logger.info(f"Soak actor {self.actor_name} is running; starting pings")

    def on_stop(self) -> None:
        # Reached both on a normal stop and after _abort raises StopUser, so
        # everything here has to tolerate having already run.
        self._release_user_count()
        # _abort already closed the channel on the startup-failure path; the
        # actor it left behind (if any) is gone with it.
        if self.actor_created and self.channel is not None:
            try:
                self._call(
                    "SuspendActor",
                    lambda stub, md: stub.SuspendActor.with_call(
                        ateapi_pb2.SuspendActorRequest(actor=self.actor_ref),
                        metadata=md,
                    ),
                )
            except Exception as e:
                logger.error(f"Failed to suspend actor {self.actor_name}: {e}")
            self._delete_actor()
        self._close()

    @task
    def soak_step(self) -> None:
        """One iteration: always ping, and every Nth iteration also cycle.

        Cycling is driven by a counter rather than a second @task because
        locust's task weights are fixed at class definition time, and the
        ping-to-cycle ratio needs to be settable from the web UI at run start.
        """
        self.ping_actor()

        interval = self._pings_per_cycle()
        if not interval:
            return
        self._pings_since_cycle += 1
        if self._pings_since_cycle < interval:
            return

        self._pings_since_cycle = 0
        self.cycle_actor()
        # Prove the restored actor actually serves traffic. A resume that
        # reports STATUS_RUNNING but comes back deaf would otherwise not
        # surface until the next iteration, where it would be recorded as a
        # plain ping failure with nothing tying it to the restore.
        self.ping_actor()

    def cycle_actor(self) -> None:
        """Suspend the actor and bring it back from its own snapshot.

        `on_start` resumes with boot=True, which skips snapshots. Once the
        actor has suspended even once its LatestSnapshotInfo is set, and every
        resume after that restores from the snapshot store instead
        (cmd/ateapi/internal/controlapi/workflow_resume.go). So it is the
        second and later cycles that cover the path production actually uses.
        """
        if not self._timed_transition(
            "ActorSuspend",
            "SuspendActorCycle",
            lambda stub, md: stub.SuspendActor.with_call(
                ateapi_pb2.SuspendActorRequest(actor=self.actor_ref),
                metadata=md,
            ),
            ateapi_pb2.Actor.STATUS_SUSPENDED,
        ):
            # A failed suspend usually means the actor never left RUNNING, in
            # which case pings still work and the next cycle can retry — do
            # not throw away a healthy user over it. Resuming here would be
            # worse than useless: ResumeActor on a RUNNING actor fails, and
            # the user would be aborted for a fault it does not have.
            if self._get_status() == ateapi_pb2.Actor.STATUS_RUNNING:
                return
            logger.error(
                f"Actor {self.actor_name} is stranded after a failed suspend; "
                f"stopping this user"
            )
            self._abort(delete_actor=True)

        if not self._timed_transition(
            "ActorResume",
            "ResumeActorCycle",
            lambda stub, md: stub.ResumeActor.with_call(
                ateapi_pb2.ResumeActorRequest(actor=self.actor_ref, boot=False),
                metadata=md,
            ),
            ateapi_pb2.Actor.STATUS_RUNNING,
        ):
            # Every subsequent ping would fail against a suspended actor,
            # burying the one failure that explains why under thousands that
            # do not. Stop instead; the dropped user count is the symptom.
            logger.error(
                f"Actor {self.actor_name} did not come back from its cycle; "
                f"stopping this user"
            )
            self._abort(delete_actor=True)

    def _timed_transition(self, name: str, rpc_name: str, invoke, target: int) -> bool:
        """Drive one actor state transition, timing the whole thing.

        Two rows come out of this. `_call` reports the RPC's own server-side
        handler time under `rpc_name` — that is just the control plane
        accepting the request, which for suspend and resume returns long
        before the work is done. The row fired here under `name` is the
        wall-clock to actually reach `target`, checkpoint upload or snapshot
        restore included. The second one is what drifts.
        """
        start_time = time.time()
        exception = None
        try:
            self._call(rpc_name, invoke)
            if not self._await_status(target, CYCLE_TIMEOUT_SECONDS):
                raise CycleFailed(
                    f"did not reach {ateapi_pb2.Actor.Status.Name(target)} "
                    f"within {CYCLE_TIMEOUT_SECONDS:.0f}s"
                )
        except Exception as e:
            exception = CycleFailed(self._classify_failure(e))
            logger.error(f"{name} failed for {self.actor_name}: {exception}")

        events.request.fire(
            request_type="actor",
            name=name,
            response_time=(time.time() - start_time) * 1000,
            response_length=0,
            exception=exception,
            user_class=self.__class__.__name__,
        )
        return exception is None

    def ping_actor(self) -> None:
        """Round-trip a ping through the router to the actor and back.

        Echoes a fresh uuid so a stale or misrouted response is caught rather
        than counted as a success.
        """
        message = str(uuid.uuid4())
        body = glutton_pb2.PingRequest(message=message).SerializeToString()
        headers = {
            "Host": self.host_header,
            "Content-Type": "application/x-protobuf",
        }

        start_time = time.time()
        with tracer.start_as_current_span("ActorPing") as span:
            inject(headers)
            exception = None
            length = 0
            try:
                response = self.http_session.post(
                    self.ping_url,
                    data=body,
                    headers=headers,
                    timeout=PING_TIMEOUT_SECONDS,
                )
                length = len(response.content)
                if response.status_code >= 400:
                    raise PingFailed(
                        f"HTTP {response.status_code}: {response.text.strip()[:200]}"
                    )
                pong = glutton_pb2.PingResponse()
                pong.ParseFromString(response.content)
                if pong.message != message:
                    raise PingFailed(
                        f"ping echo mismatch: sent={message} recv={pong.message}"
                    )
            except Exception as e:
                exception = PingFailed(self._classify_failure(e))
                logger.error(f"Ping failed for {self.actor_name}: {exception}")

            duration = (time.time() - start_time) * 1000
            events.request.fire(
                request_type="http",
                name="ActorPing",
                response_time=duration,
                response_length=length,
                exception=exception,
                user_class=self.__class__.__name__,
            )
            ctx = span.get_span_context()
            if ctx.trace_flags.sampled:
                suffix = " (failed)" if exception else ""
                logger.info(
                    f"Traced ActorPing{suffix}: trace_id={ctx.trace_id:032x}, "
                    f"duration_ms={duration:.2f} (client)"
                )

    def _channel_max_age(self) -> float:
        opts = self.environment.parsed_options
        value = getattr(opts, "soak_channel_max_age", None) if opts else None
        return value if value else DEFAULT_MAX_AGE_SECONDS

    def _pings_per_cycle(self) -> int:
        # `is None` rather than a truthiness test: 0 is a meaningful value
        # here (cycling off) and must not fall back to the default.
        opts = self.environment.parsed_options
        value = getattr(opts, "soak_pings_per_cycle", None) if opts else None
        if value is None:
            return DEFAULT_PINGS_PER_CYCLE
        return max(0, int(value))

    def _call(self, name: str, invoke):
        """Run a unary ateapi call under traced_grpc, reporting auth failures.

        `invoke` receives (stub, metadata) and must return the pair that
        `.with_call()` yields. Telling the channel about the failure is what
        lets it drop a certificate that has stopped being accepted.
        """
        # Resolve the stub outside the span: this is where a rebuild happens,
        # and the reconnect should not be charged to the call's latency.
        stub = self.channel.stub
        try:
            with traced_grpc(name, self.__class__.__name__) as metadata:
                _, metadata.call = invoke(stub, metadata)
        except Exception as e:
            self.channel.mark_failed(e)
            raise

    def _get_status(self) -> int | None:
        """Current actor status, or None if the control plane didn't answer.

        Deliberately not traced: this runs on a poll loop during startup and
        again on every ping failure, and mixing diagnostic reads into the
        stats would blur the numbers the soak is judged on.
        """
        try:
            stub = self.channel.stub
            # GetActor returns an Actor, not a wrapper response.
            actor = stub.GetActor(
                ateapi_pb2.GetActorRequest(actor=self.actor_ref),
                timeout=PING_TIMEOUT_SECONDS,
            )
            return actor.status
        except Exception as e:
            self.channel.mark_failed(e)
            logger.warning(f"GetActor failed for {self.actor_name}: {e}")
            return None

    def _await_status(self, target: int, timeout: float) -> bool:
        """Poll until the actor reports `target`. False on crash or timeout.

        STATUS_CRASHED short-circuits: nothing downstream of a crash is going
        to reach a healthy status, and waiting out the full timeout only
        delays the report.
        """
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            status = self._get_status()
            if status == target:
                return True
            if status == ateapi_pb2.Actor.STATUS_CRASHED:
                return False
            time.sleep(STARTUP_POLL_SECONDS)
        return False

    def _await_running(self) -> None:
        """Block until the actor reports STATUS_RUNNING, or give up on it."""
        if self._await_status(
            ateapi_pb2.Actor.STATUS_RUNNING, STARTUP_TIMEOUT_SECONDS
        ):
            return

        logger.error(
            f"Actor {self.actor_name} did not reach STATUS_RUNNING within "
            f"{STARTUP_TIMEOUT_SECONDS:.0f}s (crashed during startup, or too "
            f"slow); giving up on this user"
        )
        self._abort(delete_actor=True)

    def _classify_failure(self, err: BaseException) -> str:
        """Turn a ping failure into something diagnosable at 3am.

        A failed ping alone cannot distinguish "the actor died" from "the whole
        control plane is down", and by the time anyone reads the Failures tab
        the evidence is gone. So ask the control plane immediately: if it
        answers, the fault is downstream of it; if it doesn't, suspect ateapi.
        """
        status = self._get_status()
        if status is None:
            return f"{err} | control plane also unreachable (ate-api-server suspect)"
        name = ateapi_pb2.Actor.Status.Name(status)
        return f"{err} | control plane reports {name} — actor-side failure"

    def _abort(self, delete_actor: bool) -> None:
        """Stop this user without stopping the test. Does not return."""
        self._release_user_count()
        if delete_actor:
            self._delete_actor()
        self._close()
        raise StopUser()

    def _delete_actor(self) -> None:
        try:
            self._call(
                "DeleteActor",
                lambda stub, md: stub.DeleteActor.with_call(
                    ateapi_pb2.DeleteActorRequest(actor=self.actor_ref),
                    metadata=md,
                ),
            )
            self.actor_created = False
        except Exception as e:
            logger.error(f"Failed to delete actor {self.actor_name}: {e}")

    def _release_user_count(self) -> None:
        if self._counted:
            update_user_count(-1, self.__class__.__name__)
            self._counted = False

    def _close(self) -> None:
        if self.http_session is not None:
            try:
                self.http_session.close()
            except Exception as e:
                logger.warning(f"Failed to close http session: {e}")
            self.http_session = None
        if self.channel is not None:
            self.channel.close()
            self.channel = None
