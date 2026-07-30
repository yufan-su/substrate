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

"""Authenticated gRPC channel to ateapi, shared by every locust user class.

ateapi rejects calls that carry no credential: its interceptor takes identity
from an mTLS client certificate, or failing that from an `authorization:
Bearer <jwt>` header. We present the certificate, which is what the base
install gives ate-controller and atenet-router.

Both files are projected in by the Deployment
(benchmarking/locust/manifests/locust.yaml).

Two entry points, differing only in how long the caller lives:

  * ateapi_channel(): one channel, built from whatever the credential bundle
    said at that moment. Right for the short benchmark runs.
  * RotatingChannel: rebuilds periodically so a user that outlives its
    certificate keeps working. Right for the soak (see tests/soak.py).
"""

import logging
import re
import time

import grpc

logger = logging.getLogger(__name__)

CA_FILE = "/run/servicedns-ca/ca.crt"
CRED_BUNDLE = "/run/podidentity.podcert.ate.dev/credential-bundle.pem"

# How long a RotatingChannel keeps a generation before rebuilding it.
# cmd/podcertcontroller caps certificate lifetime at 24h and kubelet rewrites
# the bundle before expiry, so any period comfortably under that works; an
# hour keeps the rebuild rare enough to be invisible in the stats.
DEFAULT_MAX_AGE_SECONDS = 3600.0

# The DNS SAN on the apiserver's serving cert, which is shorter than the
# endpoint the user classes dial.
SERVER_NAME = "api.ate-system.svc"

_PEM_BLOCK = re.compile(
    rb"-----BEGIN (?P<kind>[A-Z ]+)-----.*?-----END (?P=kind)-----\n?",
    re.DOTALL,
)


def _split_cred_bundle(bundle: bytes):
    """Split a Kubernetes pod-certificate bundle into (key, chain).

    The bundle is one file holding a PRIVATE KEY block followed by
    CERTIFICATE blocks in leaf-to-root order. grpc wants the two halves
    separately, so pull the PEM blocks apart by armor rather than parsing
    the DER — no crypto library needed, and nothing here has to understand
    the key type.
    """
    key, chain = None, []
    for m in _PEM_BLOCK.finditer(bundle):
        if m.group("kind") == b"PRIVATE KEY":
            key = m.group(0)
        else:
            chain.append(m.group(0))
    if key is None:
        raise ValueError(f"{CRED_BUNDLE}: no PRIVATE KEY block")
    if not chain:
        raise ValueError(f"{CRED_BUNDLE}: no CERTIFICATE block")
    return key, b"".join(chain)


def ateapi_channel(host: str, options=None) -> grpc.Channel:
    """Open an mTLS channel to ateapi that authenticates as this pod.

    The certificate is read once, when the channel is built. Python's gRPC
    has no per-handshake reload hook, unlike the Go client's
    credbundle.ClientLoader. That is fine for a short run: each locust User
    builds its own channel in on_start, so a respawn picks up a rotated
    certificate, and an already-established connection is unaffected by the
    old one expiring.

    It stops being fine once a User outlives its certificate, because nothing
    rebuilds the channel and the next reconnect re-handshakes with the expired
    one. Use RotatingChannel for those.
    """
    target = host.replace("http://", "").replace("https://", "")
    with open(CA_FILE, "rb") as f:
        ca_cert = f.read()
    with open(CRED_BUNDLE, "rb") as f:
        private_key, cert_chain = _split_cred_bundle(f.read())

    creds = grpc.ssl_channel_credentials(
        root_certificates=ca_cert,
        private_key=private_key,
        certificate_chain=cert_chain,
    )
    channel_options = [("grpc.ssl_target_name_override", SERVER_NAME)]
    if options:
        channel_options.extend(options)
    return grpc.secure_channel(target, creds, options=channel_options)


# Codes worth throwing the channel away for. UNAUTHENTICATED is the one this
# exists for — an expired client certificate — but a channel that has gone
# UNAVAILABLE has nothing worth keeping either, and rebuilding re-reads the
# bundle as a side effect.
_REBUILD_CODES = frozenset(
    {grpc.StatusCode.UNAUTHENTICATED, grpc.StatusCode.UNAVAILABLE}
)


class RotatingChannel:
    """An ateapi channel that rebuilds itself before its certificate expires.

    A locust User normally builds one channel in on_start and holds it for the
    few minutes it lives. A soak user holds it for a day, which is exactly the
    window in which the pod certificate the channel captured stops being valid
    (cmd/podcertcontroller caps the lifetime at 24h). kubelet rewrites the
    bundle on disk ahead of expiry, but grpc.ssl_channel_credentials copied the
    old bytes at construction and never looks again — so the next reconnect
    fails to authenticate and the run dies for a reason that has nothing to do
    with the system under test.

    Rebuilding on a timer sidesteps that: each new generation re-reads the
    bundle. mark_failed() additionally forces a rebuild when the server starts
    rejecting us, so a certificate that expired early is dropped on the first
    failure rather than at the next scheduled rotation.

    Not thread-safe, which is fine — locust gives each User its own greenlet
    and its own channel, and the rebuild itself does not yield.
    """

    def __init__(
        self,
        host: str,
        stub_factory,
        max_age_seconds: float = DEFAULT_MAX_AGE_SECONDS,
        options=None,
    ) -> None:
        self._host = host
        self._stub_factory = stub_factory
        self._max_age = max_age_seconds
        self._options = options
        self._channel = None
        self._stub = None
        self._built_at = 0.0
        self._rebuild()

    @property
    def stub(self):
        """The current stub, rebuilding the channel first if it is stale.

        Resolve this outside any timing span: a rebuild opens a new connection,
        and charging that to the call that happened to trigger it would show up
        as a latency spike in whatever operation drew the short straw.
        """
        if self._stub is None or time.monotonic() - self._built_at >= self._max_age:
            self._rebuild()
        return self._stub

    def mark_failed(self, err: BaseException) -> None:
        """Drop the channel if `err` suggests the credential is no longer good.

        Anything else — NOT_FOUND, INVALID_ARGUMENT, a timeout — says something
        about the request, not the connection, so the channel is left alone.
        """
        code = err.code() if isinstance(err, grpc.RpcError) else None
        if code in _REBUILD_CODES:
            logger.warning(f"Dropping ateapi channel after {code}: {err}")
            self._close_channel()

    def close(self) -> None:
        self._close_channel()

    def _rebuild(self) -> None:
        self._close_channel()
        self._channel = ateapi_channel(self._host, options=self._options)
        self._stub = self._stub_factory(self._channel)
        self._built_at = time.monotonic()

    def _close_channel(self) -> None:
        # Cleared together: a None stub is what tells the `stub` property to
        # rebuild, so leaving one behind would keep handing out a dead channel.
        if self._channel is not None:
            try:
                self._channel.close()
            except Exception as e:
                logger.warning(f"Failed to close ateapi channel: {e}")
        self._channel = None
        self._stub = None
