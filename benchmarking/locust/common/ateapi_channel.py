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

Certificate rotation: cmd/podcertcontroller caps pod certificate lifetime at
24h and asks kubelet to start refreshing 30 minutes before expiry, so the
bundle on disk is rewritten under any run long enough to reach that point.
Python's gRPC has no per-handshake reload hook (unlike the Go client's
credbundle.ClientLoader), so a channel keeps presenting whatever certificate
it was built with. Short tests never notice — users respawn often enough to
pick up the new bundle. Anything that keeps one user alive for hours should
use RotatingChannel below instead of holding an ateapi_channel() forever.
"""

import logging
import re
import time
from typing import Callable, TypeVar

import grpc

logger = logging.getLogger(__name__)

CA_FILE = "/run/servicedns-ca/ca.crt"
CRED_BUNDLE = "/run/podidentity.podcert.ate.dev/credential-bundle.pem"

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

    The certificate is read once, when the channel is built — see the module
    docstring for what that means for long-running users.
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


# Rebuild the channel this often even when nothing has failed. Well under the
# ~23.5h point at which kubelet starts rewriting the credential bundle, so a
# long-lived user never reaches a rotation with a stale certificate in hand.
DEFAULT_MAX_AGE_SECONDS = 3600.0

# Failures that plausibly mean "the certificate we are holding is no longer
# accepted". UNAVAILABLE is included because a rejected handshake surfaces as a
# transport failure rather than an auth error.
_REBUILD_CODES = frozenset({
    grpc.StatusCode.UNAUTHENTICATED,
    grpc.StatusCode.UNAVAILABLE,
})

_S = TypeVar("_S")


class RotatingChannel:
    """An ateapi channel + stub that rebuilds itself from disk periodically.

    Wraps ateapi_channel() rather than reimplementing it, so the mTLS setup
    and PEM splitting live in exactly one place. Rebuilds happen lazily, on
    the next `.stub` access, when either the channel has outlived
    `max_age_seconds` or a caller reported an auth-shaped RPC failure via
    `mark_failed()`.

    Intended for users that outlive a certificate rotation. Tests whose users
    churn every few minutes should keep using ateapi_channel() directly.

    Usage:
        chan = RotatingChannel(host, ateapi_pb2_grpc.ControlStub)
        try:
            chan.stub.GetActor(req)
        except grpc.RpcError as e:
            chan.mark_failed(e)
            raise
    """

    def __init__(
        self,
        host: str,
        stub_factory: Callable[[grpc.Channel], _S],
        max_age_seconds: float = DEFAULT_MAX_AGE_SECONDS,
    ) -> None:
        self._host = host
        self._stub_factory = stub_factory
        self._max_age = max_age_seconds
        self._channel: grpc.Channel | None = None
        self._stub: _S | None = None
        self._built_at = 0.0
        self._rebuild_requested = False
        self._build()

    @property
    def stub(self) -> _S:
        """The current stub, rebuilding the channel first if it is due."""
        if self._should_rebuild():
            self._build()
        return self._stub

    def mark_failed(self, error: BaseException) -> None:
        """Note an RPC failure. Auth-shaped ones schedule a rebuild.

        Takes the exception rather than a bare flag so callers can hand over
        whatever they caught without classifying it themselves; anything that
        is not an auth-shaped grpc.RpcError is ignored.
        """
        if not isinstance(error, grpc.RpcError):
            return
        code = error.code() if isinstance(error, grpc.Call) else None
        if code in _REBUILD_CODES:
            self._rebuild_requested = True

    def close(self) -> None:
        self._close_channel()
        self._stub = None

    def _should_rebuild(self) -> bool:
        if self._channel is None or self._rebuild_requested:
            return True
        return (time.monotonic() - self._built_at) >= self._max_age

    def _build(self) -> None:
        previous = self._channel
        self._channel = ateapi_channel(self._host)
        self._stub = self._stub_factory(self._channel)
        self._built_at = time.monotonic()
        self._rebuild_requested = False
        if previous is not None:
            # Close only after the replacement is live so a failure to build
            # leaves the old (possibly still working) channel in place.
            _close_quietly(previous)
            logger.info("Rebuilt ateapi channel to %s", self._host)

    def _close_channel(self) -> None:
        if self._channel is not None:
            _close_quietly(self._channel)
            self._channel = None


def _close_quietly(channel: grpc.Channel) -> None:
    try:
        channel.close()
    except Exception as e:
        logger.warning(f"Failed to close ateapi channel: {e}")
