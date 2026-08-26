# SPDX-License-Identifier: Apache-2.0
"""Adapter-owned atomic primitives with stable script digests."""

from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Any, cast

from valkey.exceptions import NoScriptError, WatchError

from .._canonical import sha1_hex
from ..client import ClientProtocol
from ..codec import DecodedEnvelope, EnvelopeCodec
from ..configuration import SERIALIZER_CANONICAL_JSON
from ..key import require_shared_hash_slot

CAS_SCRIPT = b"""-- meridian-storage-valkey cas v1
local current = valkey.call('GET', KEYS[1])
if not current then return -1 end
if string.sub(current, 1, 6) ~= 'MRVK1|' then return -2 end
if string.sub(current, 7, 77) ~= ARGV[1] then return 0 end
valkey.call('SET', KEYS[1], ARGV[2], 'PX', ARGV[3])
return 1
"""

RELEASE_LEASE_SCRIPT = b"""-- meridian-storage-valkey release lease v1
local current = valkey.call('GET', KEYS[1])
if current and current == ARGV[1] then
  return valkey.call('DEL', KEYS[1])
end
return 0
"""

CAS_SCRIPT_DIGEST = sha1_hex(CAS_SCRIPT)
RELEASE_LEASE_SCRIPT_DIGEST = sha1_hex(RELEASE_LEASE_SCRIPT)


@dataclass(frozen=True, slots=True)
class CompareAndSetResult:
    swapped: bool
    missing: bool = False
    corrupt: bool = False


class AtomicExecutor:
    """Execute only package-owned scripts or bounded transaction primitives."""

    def __init__(self, client: ClientProtocol, codec: EnvelopeCodec) -> None:
        self._client = client
        self._codec = codec

    def put_if_absent(self, key: bytes, envelope: bytes, ttl_ms: int) -> bool:
        return bool(self._client.set(key, envelope, nx=True, px=ttl_ms))

    def compare_and_set(
        self,
        key: bytes,
        expected_envelope_version: str,
        envelope: bytes,
        ttl_ms: int,
    ) -> CompareAndSetResult:
        raw = self._eval_script(
            CAS_SCRIPT,
            CAS_SCRIPT_DIGEST,
            1,
            key,
            expected_envelope_version,
            envelope,
            str(ttl_ms),
        )
        if isinstance(raw, bytes):
            raw = int(raw)
        if raw == 1:
            return CompareAndSetResult(True)
        if raw == -1:
            return CompareAndSetResult(False, missing=True)
        if raw == -2:
            return CompareAndSetResult(False, corrupt=True)
        return CompareAndSetResult(False)

    def acquire_lease(self, lease_key: bytes, token: bytes, lease_ms: int) -> bool:
        return bool(self._client.set(lease_key, token, nx=True, px=lease_ms))

    def release_lease(self, lease_key: bytes, token: bytes) -> bool:
        return bool(
            self._eval_script(
                RELEASE_LEASE_SCRIPT,
                RELEASE_LEASE_SCRIPT_DIGEST,
                1,
                lease_key,
                token,
            )
        )

    def batch_get(
        self, keys: tuple[bytes, ...], *, require_atomic_slot: bool = False
    ) -> tuple[object, ...]:
        if require_atomic_slot:
            require_shared_hash_slot(keys)
        return tuple(self._client.mget(keys))

    def increment(
        self,
        key: bytes,
        *,
        amount: float,
        schema_fingerprint: str,
        ttl_ms: int,
        maximum_staleness_ms: int | None = None,
        retries: int = 8,
    ) -> DecodedEnvelope:
        """Atomically increment a Schema-declared numeric cached value.

        This is an internal coordination primitive. It is deliberately not an
        invented Cache Catalog Operation in Meridian V1.
        """

        if isinstance(amount, bool) or not isinstance(amount, int | float):
            raise TypeError("increment amount must be numeric")
        if isinstance(amount, float) and not math.isfinite(amount):
            raise ValueError("increment amount must be finite")
        for _ in range(retries):
            pipeline: Any = self._client.pipeline(transaction=True)
            try:
                pipeline.watch(key)
                current = pipeline.get(key)
                if current is None or not isinstance(current, bytes | bytearray | memoryview):
                    raise ValueError("numeric increment requires an existing envelope")
                decoded = self._codec.decode(
                    current,
                    expected_schema_fingerprint=schema_fingerprint,
                    maximum_staleness_ms=maximum_staleness_ms,
                )
                if (
                    decoded.serializer_id != SERIALIZER_CANONICAL_JSON
                    or isinstance(decoded.value, bool)
                    or not isinstance(decoded.value, int | float)
                ):
                    raise ValueError("numeric increment requires a canonical JSON number")
                new_value: int | float = decoded.value + amount
                if isinstance(new_value, float) and not math.isfinite(new_value):
                    raise ValueError("numeric increment result must be finite")
                envelope = self._codec.encode(
                    cast(Any, new_value),
                    serializer_id=SERIALIZER_CANONICAL_JSON,
                    schema_fingerprint=schema_fingerprint,
                    ttl_ms=ttl_ms,
                    source_version=decoded.source_version,
                )
                pipeline.multi()
                pipeline.set(key, envelope, px=ttl_ms)
                pipeline.execute()
                return self._codec.decode(
                    envelope,
                    expected_schema_fingerprint=schema_fingerprint,
                    maximum_staleness_ms=maximum_staleness_ms,
                )
            except WatchError:
                continue
            finally:
                pipeline.reset()
        raise WatchError("numeric increment contention exceeded its retry bound")

    def _eval_script(
        self,
        script: bytes,
        declared_digest: str,
        numkeys: int,
        *arguments: object,
    ) -> object:
        sha = declared_digest.removeprefix("sha1:")
        try:
            return self._client.evalsha(sha, numkeys, *arguments)
        except NoScriptError:
            loaded = self._client.script_load(script)
            if isinstance(loaded, bytes):
                loaded = loaded.decode("ascii")
            if loaded != sha:
                raise RuntimeError("Valkey loaded a script under an unexpected digest") from None
            return self._client.evalsha(sha, numkeys, *arguments)


__all__ = [
    "CAS_SCRIPT",
    "CAS_SCRIPT_DIGEST",
    "RELEASE_LEASE_SCRIPT",
    "RELEASE_LEASE_SCRIPT_DIGEST",
    "AtomicExecutor",
    "CompareAndSetResult",
]
