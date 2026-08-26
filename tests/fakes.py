# SPDX-License-Identifier: Apache-2.0
"""Small deterministic Valkey protocol fake; real behavior uses integration suites."""

from __future__ import annotations

import hashlib
from collections.abc import Mapping, Sequence
from dataclasses import dataclass

from valkey.exceptions import ConnectionError as ValkeyConnectionError
from valkey.exceptions import NoScriptError, WatchError

from meridian_storage.adapters.valkey.atomic import (
    CAS_SCRIPT_DIGEST,
    RELEASE_LEASE_SCRIPT_DIGEST,
)


@dataclass
class _Stored:
    value: object
    expires_at_ms: int | None


class FakePipeline:
    def __init__(self, client: FakeValkey) -> None:
        self.client = client
        self.watched: bytes | None = None
        self.expected_revision = 0
        self.pending: tuple[bytes, object, int] | None = None

    def watch(self, key: bytes) -> None:
        self.watched = key
        self.expected_revision = self.client.revisions.get(key, 0)

    def get(self, key: bytes) -> object:
        return self.client.get(key)

    def multi(self) -> None:
        return None

    def set(self, key: bytes, value: object, *, px: int) -> FakePipeline:
        self.pending = (key, value, px)
        return self

    def execute(self) -> list[object]:
        assert self.watched is not None
        if self.client.force_watch_conflict:
            self.client.force_watch_conflict = False
            raise WatchError("injected contention")
        if self.client.revisions.get(self.watched, 0) != self.expected_revision:
            raise WatchError("watched key changed")
        assert self.pending is not None
        key, value, px = self.pending
        return [self.client.set(key, value, px=px)]

    def reset(self) -> None:
        self.pending = None


class FakeValkey:
    """In-memory implementation of the adapter's bounded client surface."""

    def __init__(self) -> None:
        self.now_ms = 1_700_000_000_000
        self.values: dict[bytes, _Stored] = {}
        self.revisions: dict[bytes, int] = {}
        self.scripts: set[str] = set()
        self.closed = False
        self.raise_connection = False
        self.force_watch_conflict = False
        self.engine_version = "8.1.9"
        self.maxmemory = 64 * 1024 * 1024
        self.eviction = "allkeys-lru"
        self.appendonly = "no"
        self.save = ""
        self.role = "master"
        self.replicas = 0

    def _check(self) -> None:
        if self.raise_connection:
            raise ValkeyConnectionError("secret-host.internal:6379 password=do-not-leak")

    def _bytes(self, name: bytes | str) -> bytes:
        return name if isinstance(name, bytes) else name.encode()

    def _live(self, name: bytes | str) -> _Stored | None:
        key = self._bytes(name)
        item = self.values.get(key)
        if (
            item is not None
            and item.expires_at_ms is not None
            and item.expires_at_ms <= self.now_ms
        ):
            self.values.pop(key, None)
            self.revisions[key] = self.revisions.get(key, 0) + 1
            return None
        return item

    def advance(self, milliseconds: int) -> None:
        self.now_ms += milliseconds

    def ping(self) -> bool:
        self._check()
        return True

    def info(self, section: str | None = None) -> Mapping[object, object]:
        self._check()
        if section == "server":
            return {"valkey_version": self.engine_version}
        if section == "memory":
            return {"maxmemory": self.maxmemory}
        if section == "replication":
            return {"role": self.role, "connected_slaves": self.replicas}
        return {}

    def config_get(self, pattern: str = "*") -> Mapping[object, object]:
        self._check()
        return {
            "maxmemory-policy": self.eviction,
            "appendonly": self.appendonly,
            "save": self.save,
        }

    def execute_command(self, *args: object, **kwargs: object) -> object:
        self._check()
        del args, kwargs
        return b"OK"

    def script_load(self, script: bytes | str) -> str:
        self._check()
        value = script.encode() if isinstance(script, str) else script
        digest = hashlib.sha1(value, usedforsecurity=False).hexdigest()
        self.scripts.add(digest)
        return digest

    def script_exists(self, *shas: str) -> Sequence[object]:
        self._check()
        return [sha in self.scripts for sha in shas]

    def evalsha(self, sha: str, numkeys: int, *keys_and_args: object) -> object:
        self._check()
        if sha not in self.scripts:
            raise NoScriptError("NOSCRIPT")
        assert numkeys == 1
        key = bytes(keys_and_args[0])
        if sha == RELEASE_LEASE_SCRIPT_DIGEST.removeprefix("sha1:"):
            token = keys_and_args[1]
            current = self.get(key)
            return self.delete(key) if current == token else 0
        if sha == CAS_SCRIPT_DIGEST.removeprefix("sha1:"):
            current = self.get(key)
            if current is None:
                return -1
            if not isinstance(current, bytes) or not current.startswith(b"MRVK1|"):
                return -2
            expected = str(keys_and_args[1]).encode()
            if current[6:77] != expected:
                return 0
            envelope = keys_and_args[2]
            ttl = int(keys_and_args[3])
            self.set(key, envelope, px=ttl)
            return 1
        raise AssertionError("unknown adapter script")

    def get(self, name: bytes | str) -> object:
        self._check()
        item = self._live(name)
        return None if item is None else item.value

    def mget(self, keys: Sequence[bytes]) -> Sequence[object]:
        return [self.get(key) for key in keys]

    def set(self, name: bytes, value: object, **kwargs: object) -> object:
        self._check()
        key = self._bytes(name)
        if kwargs.get("nx") and self._live(key) is not None:
            return None
        px = kwargs.get("px")
        assert px is None or isinstance(px, int)
        expires = None if px is None else self.now_ms + px
        self.values[key] = _Stored(value, expires)
        self.revisions[key] = self.revisions.get(key, 0) + 1
        return True

    def delete(self, *names: bytes) -> int:
        self._check()
        deleted = 0
        for name in names:
            key = self._bytes(name)
            if key in self.values:
                self.values.pop(key)
                deleted += 1
            self.revisions[key] = self.revisions.get(key, 0) + 1
        return deleted

    def pttl(self, name: bytes) -> int:
        item = self._live(name)
        if item is None:
            return -2
        if item.expires_at_ms is None:
            return -1
        return item.expires_at_ms - self.now_ms

    def pipeline(self, transaction: bool = True) -> FakePipeline:
        assert transaction
        return FakePipeline(self)

    def close(self) -> None:
        self.closed = True


class RecordingSink:
    def __init__(self) -> None:
        self.events: list[tuple[str, Mapping[str, str]]] = []

    def emit(self, name: str, attributes: Mapping[str, str]) -> None:
        self.events.append((name, dict(attributes)))
