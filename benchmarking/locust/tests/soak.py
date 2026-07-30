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

"""Long-running liveness soak: does Substrate still work N hours from now?

Unlike the other user classes, this one is not looking for a throughput
ceiling. Each user brings up exactly one counter actor and then loops for the
whole run, in the shape a real actor's life has: suspended most of the time,
resumed to serve a short burst, suspended again.

One iteration is

    resolve -> ResumeActor -> ping x N -> SuspendActor -> (wait)

so locust's wait time elapses with the actor *suspended*, and every ping is
served by an actor that was just restored from its own snapshot. The lifecycle
path — checkpoint upload, worker release, scheduler placement, snapshot
restore — is therefore on the critical path of every iteration rather than
occasionally, and the file-counter assertion in `_check_counters` crosses a
real snapshot round trip every single time.

Actors are addressed the way a real client addresses them — by their own
`<actor>.<atespace>.actors.resources.substrate.ate.dev` name — so the resolver
sits on the path alongside the router. `_resolve_actor` explains why the
lookup is also timed on its own.

`--soak-pings-per-cycle` sets N, i.e. how long the actor stays awake. Keeping
it small is what makes the model faithful. Setting it to 0 turns cycling off
and leaves the actor running, which reduces the run to a data-path test; that
is a diagnostic mode, not the intended one.

What this shape deliberately gives up is the long-lived RUNNING actor: nothing
here stays up long enough to catch a slow leak in a *running* sandbox. Raise
`--soak-pings-per-cycle` if that is the failure mode you are hunting.

Why the counter demo rather than glutton: its response carries two counters
with *different* durability, so a ping asserts more than "something answered".
See `_check_counters` for what each one proves.

Run it from the web UI with a small user count and a long wait time — a handful
of users at `--min-wait-time=10 --max-wait-time=15` is the intended shape. Note
that a user releases its worker on every suspend and needs a free slot to
resume onto; a worker holds exactly one actor. See the soak section of
benchmarking/README.md.
"""

from locust import User, task, events
from common.grpc_setup import init_grpc_gevent

# Patch gRPC to cooperate with locust's gevent loop before any channel exists.
init_grpc_gevent()

import logging
import re
import socket
import time
import uuid

import requests
from locust.exception import StopUser
from opentelemetry.propagate import inject

from common import ateapi_pb2
from common import ateapi_pb2_grpc
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

# Actors are addressed by name, exactly as a real client addresses them:
# http://<actor>.<atespace>.actors.resources.substrate.ate.dev/. Resolving that
# is itself part of the system under test — atenet reconciles a stub-domain
# entry into kube-system:kube-dns pointing this suffix at the ate-system CoreDNS
# it runs (cmd/atenet/internal/dns/dns.go), which answers with the router's IP
# at TTL 60 (corefile.go). The alternative, POSTing to the router's service DNS
# name with a spoofed Host header, skips both hops and would keep reporting
# success with the stub domain gone.
ACTOR_DOMAIN = "actors.resources.substrate.ate.dev"

TEMPLATE_NAMESPACE = "ate-demo-counter"
TEMPLATE_NAME = "counter"

# The counter's only route answers any method on "/" by bumping both counters
# and rendering them into the body (demos/counter/counter.go).
COUNTER_RE = re.compile(
    r"preserved memory count:\s*(?P<memory>\d+)\s*\|\s*"
    r"preserved file counter:\s*(?P<file>\d+)"
)

# A cold boot pulls the image and starts a gVisor sandbox, so the first
# transition to RUNNING is far slower than any later operation. Generous,
# because giving up here costs the user for the rest of the run.
STARTUP_TIMEOUT_SECONDS = 300.0
STARTUP_POLL_SECONDS = 2.0

# Well above any healthy ping. Long enough that a slow-but-alive system reads
# as slow rather than broken, short enough that a black hole is not mistaken
# for a hang — an unbounded request would park this greenlet forever and the
# user would stop contributing without ever recording a failure.
PING_TIMEOUT_SECONDS = 30.0

# Budget for one suspend or one resume to reach its terminal status. Shorter
# than STARTUP_TIMEOUT_SECONDS on purpose: a snapshot restore taking as long as
# a cold boot is itself the degradation this test is looking for, so the limit
# should not be so loose that it hides one. Still generous enough that a merely
# busy cluster does not trip it.
CYCLE_TIMEOUT_SECONDS = 180.0

# Pings served per awake window, per user. Small on purpose: a production
# actor wakes, handles a little traffic and goes back down, and matching that
# is the whole point of the loop shape. It also keeps the cycle — the thing
# most likely to drift over 24h — on every iteration.
DEFAULT_PINGS_PER_CYCLE = 3

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
                "Pings served per awake window, per user: each iteration "
                "resumes the actor, pings it this many times, then suspends it "
                "again. Small values model production, where an actor is "
                "suspended most of the time. With 0 the actor is left running "
                "and the run only covers the data path."
            ),
            include_in_web_ui=True,
        )

    _initialized = True


_init_soak_options()


class PingFailed(Exception):
    """A ping that did not come back, or came back wrong.

    Raised internally with a bare reason, then re-raised by ping_actor wrapped
    in _classify_failure's verdict — that is the form that reaches the Failures
    tab, and it is what makes the tab readable hours after the fact.
    """


class CycleFailed(Exception):
    """A suspend or resume that errored or never reached its target status."""


class DNSFailed(Exception):
    """The actor's name did not resolve."""


class SoakUser(User):
    wait_time = dynamic_wait_time

    # `host` is what locust shows in the web UI / --host flag; it can be
    # overridden by the user at test start. Keep the api target in a separate
    # attribute so it's not clobbered when host points elsewhere (e.g. when
    # running with other user classes via --class-picker).
    host = "api.ate-system.svc.cluster.local:443"
    api_host = "api.ate-system.svc.cluster.local:443"

    def on_start(self) -> None:
        # Set every attribute on_stop touches before anything can raise. on_stop
        # runs even when on_start raises StopUser — locust wraps both in one try
        # (locust/user/users.py) — so a failure partway through startup would
        # otherwise tear down against attributes that do not exist yet.
        self.channel = None
        self.http_session = None
        self.actor_created = False
        self._counted = False
        self._running = False
        self._last_file_counter = None
        self._expected_memory_counter = None
        self.actor_name = f"sb-{uuid.uuid4()}"
        self.actor_ref = ateapi_pb2.ObjectRef(atespace=ATESPACE, name=self.actor_name)
        self.actor_fqdn = f"{self.actor_name}.{ATESPACE}.{ACTOR_DOMAIN}"
        # No port: the A record resolves to the router service, which listens on
        # 80 (manifests/ate-install/atenet-router.yaml). No Host header either —
        # requests derives it from the URL, which is the whole point.
        self.ping_url = f"http://{self.actor_fqdn}/"
        self._last_resolved_ip = None

        update_user_count(1, self.__class__.__name__)
        self._counted = True

        # Unlike the short tests, a soak user outlives the pod certificate it
        # starts with: cmd/podcertcontroller caps the lifetime at 24h and
        # kubelet rewrites the bundle before that. RotatingChannel re-reads it
        # from disk periodically so the run does not fail at hour 23 for
        # reasons that have nothing to do with Substrate's health.
        self.channel = RotatingChannel(
            self.api_host,
            ateapi_pb2_grpc.ControlStub,
            max_age_seconds=self._channel_max_age(),
        )

        try:
            ensure_atespace(self.channel.stub, self.__class__.__name__)
        except Exception as e:
            self._abort(f"could not ensure atespace {ATESPACE}: {e}")

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
            self._abort(f"CreateActor failed: {e}")
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
            self._abort(f"cold-start ResumeActor failed: {e}")

        self._await_running()
        self._running = True

        # One HTTP session per user, held for the whole run: keeping the
        # connection to the router warm is part of what we are testing.
        self.http_session = requests.Session()

        # Ping once before handing over to the task loop. It fails fast if the
        # data path is broken at spawn time, and it seeds the file-counter
        # baseline from a known-good read.
        self.ping_actor()

        # Leave the actor suspended: the task loop's invariant is that an
        # iteration starts with a down actor. This first suspend also writes
        # the snapshot that the loop's first resume restores from, so even
        # iteration one exercises the real restore path rather than boot=True.
        self._suspend_actor()
        logger.info(f"Soak actor {self.actor_name} is ready; starting cycles")

    def on_stop(self) -> None:
        # Reached both on a normal stop and after _abort raises StopUser —
        # including from on_start, where most of the attributes below were only
        # just assigned. Suspend before delete: DeleteActor only accepts
        # suspended actors.
        self._release_user_count()
        if self.actor_created and self.channel is not None:
            # Usually already suspended: the task loop leaves it that way, and
            # SuspendActor on a suspended actor errors. Confirm with the
            # control plane rather than trusting _running, which locust can
            # leave stale by killing the greenlet mid-iteration — skipping a
            # needed suspend would leak the actor, since DeleteActor only
            # accepts suspended ones.
            if self._running or self._get_status() == ateapi_pb2.Actor.STATUS_RUNNING:
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
        """One wake/serve/sleep cycle, the unit this soak is built around.

        Resume, serve a short burst, suspend. locust applies the wait time
        between task invocations, so the idle stretch lands with the actor
        suspended — which is where a production actor spends most of its life,
        and which keeps a user off a worker while it waits.
        """
        pings = self._pings_per_cycle()

        self._resolve_actor()
        self._resume_actor()

        if pings == 0:
            # Cycling disabled. _resume_actor was a no-op after the first
            # iteration, so the actor just stays up and this degenerates to a
            # data-path-only loop, one ping per wait.
            self.ping_actor()
            return

        for _ in range(pings):
            self.ping_actor()

        self._suspend_actor()

    def _resume_actor(self) -> None:
        """Bring the actor back from its own snapshot, if it is not up already.

        Every resume here is boot=False. The only boot=True resume in this
        class is the cold start in `on_start`, which is immediately followed by
        a suspend, so from the first iteration onward LatestSnapshotInfo is set
        and this restores from the snapshot store — the path production uses.
        """
        # The memory counter does not survive a cycle (see _check_counters), so
        # stop predicting it now rather than after the fact.
        self._expected_memory_counter = None

        # A previous suspend can fail leaving the actor up. ResumeActor rejects
        # a RUNNING actor, so firing one anyway would charge this user a
        # failure for a fault it does not have.
        if self._running:
            return

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
            self._abort("did not come back from its snapshot")
        self._running = True

    def _suspend_actor(self) -> None:
        """Check the actor back in, releasing its worker."""
        if self._timed_transition(
            "ActorSuspend",
            "SuspendActorCycle",
            lambda stub, md: stub.SuspendActor.with_call(
                ateapi_pb2.SuspendActorRequest(actor=self.actor_ref),
                metadata=md,
            ),
            ateapi_pb2.Actor.STATUS_SUSPENDED,
        ):
            self._running = False
            return

        # A failed suspend usually means the actor never left RUNNING, in which
        # case pings still work and the next iteration can retry — do not throw
        # away a healthy user over it. Leaving _running set is what makes the
        # next _resume_actor skip its RPC.
        if self._get_status() == ateapi_pb2.Actor.STATUS_RUNNING:
            self._running = True
            return
        self._abort("stranded mid-suspend, neither running nor suspended")

    def _timed_transition(self, name: str, rpc_name: str, invoke, target: int) -> bool:
        """Drive one actor state transition, timing it two ways.

        SuspendActor and ResumeActor are synchronous today: ActorWorkflow runs
        every step inline, including the blocking atelet Checkpoint/Restore, and
        only finalizes the status afterwards (cmd/ateapi/internal/controlapi/
        workflow.go). So the RPC already covers the snapshot work, and the
        actor has reached `target` by the time it returns — _await_status
        normally succeeds on its first poll without sleeping.

        Two rows still come out of this, and the gap between them is the point:

          * `rpc_name`, from _call, is ateapi's own handler time, taken from
            the x-server-elapsed-us trailer. It deliberately excludes the
            client's gevent scheduling delay.
          * `name`, fired here, is client wall-clock around the same RPC plus
            one confirming GetActor.

        Subtract them and you get how long this greenlet spent queued behind
        others, which is what tells you whether a latency rise is Substrate or
        the load generator. The GetActor also corroborates the status
        independently: a successful RPC is only ateapi's claim about it.

        Keep both even though they nearly coincide. workflow_pause.go notes
        suspend and pause are currently identical; if either ever goes async,
        the RPC row silently stops covering the snapshot work while this one
        keeps measuring the transition end to end.
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

    def _resolve_actor(self) -> None:
        """Resolve the actor's name, timed as its own row.

        The pings exercise DNS too, but only just: requests holds one pooled
        connection to the router for the whole run, so getaddrinfo fires once
        when that connection is opened and effectively never again. DNS could
        then break for twenty hours — a clobbered kube-dns stubDomain, the
        ate-system CoreDNS gone — while every ping kept succeeding over the
        connection already established. An explicit lookup per iteration is
        what makes DNS a component this soak actually covers.

        A lookup failure is recorded, not fatal. The pings that follow will
        fail too if it matters, and between the two rows the Failures tab says
        which layer went.
        """
        start_time = time.time()
        exception = None
        address = None
        try:
            # The same call requests makes when opening a connection, and a
            # real query every time: containers run without nscd, so nothing
            # caches in front of CoreDNS's 60s TTL.
            infos = socket.getaddrinfo(
                self.actor_fqdn, 80, family=socket.AF_INET, type=socket.SOCK_STREAM
            )
            address = infos[0][4][0]
        except Exception as e:
            exception = DNSFailed(str(e))
            logger.error(f"DNS lookup failed for {self.actor_fqdn}: {e}")

        events.request.fire(
            request_type="dns",
            name="ActorDNS",
            response_time=(time.time() - start_time) * 1000,
            response_length=0,
            exception=exception,
            user_class=self.__class__.__name__,
        )

        if address is not None and address != self._last_resolved_ip:
            if self._last_resolved_ip is not None:
                # Not a failure — the router's ClusterIP can legitimately be
                # reassigned. Logged because it explains a burst of ping
                # failures that would otherwise look like the actor's fault.
                logger.warning(
                    f"Actor DNS answer changed: {self._last_resolved_ip} -> {address}"
                )
            self._last_resolved_ip = address

    def ping_actor(self) -> None:
        """Round-trip a request through the router to the actor and back.

        Addressed by the actor's own FQDN, so the resolver and the router are
        both on the path — see ACTOR_DOMAIN.
        """
        headers = {}

        start_time = time.time()
        with tracer.start_as_current_span("ActorPing") as span:
            inject(headers)
            exception = None
            length = 0
            try:
                response = self.http_session.post(
                    self.ping_url,
                    headers=headers,
                    timeout=PING_TIMEOUT_SECONDS,
                )
                length = len(response.content)
                if response.status_code >= 400:
                    # Status only, body to the log — see _check_counters for why
                    # the message has to stay constant across occurrences.
                    logger.error(
                        f"Ping to {self.actor_name} returned "
                        f"{response.status_code}: {response.text.strip()[:200]}"
                    )
                    raise PingFailed(f"HTTP {response.status_code}")
                self._check_counters(response.text)
            except Exception as e:
                exception = PingFailed(self._classify_failure(e))
                logger.error(f"Ping failed for {self.actor_name}: {exception}")
                # The request may well have been served before whatever went
                # wrong, in which case the actor's counters moved without us
                # seeing the reply. Predicting the next value from a number we
                # never read would manufacture a second failure.
                self._expected_memory_counter = None

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

    def _check_counters(self, body: str) -> None:
        """Assert the counter demo's two counters moved the way they should.

        The counter answers with a memory counter and a file counter, and the
        two have deliberately different durability under the template's
        `onCommit: Data` (demos/counter/counter.yaml.tmpl). SuspendActor
        checkpoints with the OnCommit scope
        (cmd/ateapi/internal/controlapi/workflow_suspend.go), and
        SNAPSHOT_SCOPE_DATA captures only DurableDir volumes — "Memory and the
        rest of rootfs are excluded" (internal/proto/ateletpb/atelet.proto).
        So:

          * The file counter lives in the DurableDir at /home/counter and must
            survive every cycle. Requiring it to increase for 24h is the
            strongest assertion available here: it proves the DurableDir ->
            snapshot store -> restore round trip preserved state, every time.
            A reset means a cycle silently lost the volume.
          * The memory counter is excluded, so a cycle resets it. Between
            cycles, though, nothing should be restarting the actor, so it must
            advance by exactly one per ping. That catches an actor being
            silently replaced mid-window — which a liveness check that only
            looked at HTTP status would read as perfect health.

        Failure messages here are deliberately fixed strings, with the observed
        values logged instead. locust keys its Failures table on the message
        (StatsError.create_key), so a message carrying counter values would
        create a fresh row per occurrence — over 24h a systematic fault would
        grow thousands of one-hit rows in the master instead of one row with a
        count. The log has the numbers when you need them.
        """
        match = COUNTER_RE.search(body)
        if match is None:
            logger.error(f"Unparseable counter response: {body.strip()[:200]}")
            raise PingFailed("response did not contain the counter pair")
        memory = int(match.group("memory"))
        file_counter = int(match.group("file"))

        if self._last_file_counter is not None and file_counter <= self._last_file_counter:
            logger.error(
                f"File counter went backwards for {self.actor_name}: "
                f"{self._last_file_counter} -> {file_counter}"
            )
            raise PingFailed("file counter did not increase (durable state lost)")
        self._last_file_counter = file_counter

        if (
            self._expected_memory_counter is not None
            and memory != self._expected_memory_counter
        ):
            logger.error(
                f"Memory counter jumped for {self.actor_name}: expected "
                f"{self._expected_memory_counter}, got {memory}"
            )
            # Do not re-arm off a value we did not predict, or every ping for
            # the rest of the window inherits this one discrepancy.
            self._expected_memory_counter = None
            raise PingFailed("memory counter skipped (actor restarted mid-window?)")
        self._expected_memory_counter = memory + 1

    def _channel_max_age(self) -> float:
        opts = self.environment.parsed_options
        value = getattr(opts, "soak_channel_max_age", None) if opts else None
        return value if value else DEFAULT_MAX_AGE_SECONDS

    def _pings_per_cycle(self) -> int:
        # `is None` rather than a truthiness test: 0 is a meaningful value here
        # (cycling off) and must not fall back to the default.
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
        again on every ping failure, and mixing diagnostic reads into the stats
        would blur the numbers the soak is judged on.
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
        to reach a healthy status, and waiting out the full timeout only delays
        the report.
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

        self._abort(
            f"did not reach STATUS_RUNNING within {STARTUP_TIMEOUT_SECONDS:.0f}s "
            f"(crashed during startup, or no free worker to place it on)"
        )

    def _classify_failure(self, err: BaseException) -> str:
        """Turn a failure into something diagnosable at 3am.

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

    def _abort(self, reason: str) -> None:
        """Stop this user without stopping the test. Does not return.

        Teardown is left to `on_stop`, which locust runs after StopUser
        propagates out of the task (or out of on_start — both are wrapped in
        the same try in locust/user/users.py). Cleaning up here as well would
        duplicate it, and worse, would delete the actor without suspending it
        first: DeleteActor only accepts suspended actors.
        """
        logger.error(f"Stopping soak user for {self.actor_name}: {reason}")
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
