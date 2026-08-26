# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

import os
import shutil
import subprocess
import time
from pathlib import Path

import pytest
from meridian_storage.semantics.catalogs import CacheCatalogProvider

from meridian_storage.adapters.valkey import ValkeyAdapterFactory
from tests.conftest import RESOURCE, execution_request, make_context
from tests.fakes import FakeValkey

pytestmark = pytest.mark.integration


def _operation(method: str, **arguments: object) -> object:
    provider = CacheCatalogProvider()
    expression = getattr(provider.create_surface(), method)(resource=RESOURCE, **arguments)
    return provider.normalize(expression)


def test_real_standalone_contract_ttl_eviction_restart_profile() -> None:
    endpoint = os.environ.get("MERIDIAN_VALKEY_STANDALONE_ENDPOINT")
    compose = os.environ.get("MERIDIAN_VALKEY_STANDALONE_COMPOSE")
    if endpoint is None or compose is None:
        pytest.skip("standalone integration endpoint is not configured")
    docker = shutil.which("docker")
    if docker is None:
        pytest.fail("docker is required for standalone restart conformance")
    context = make_context(FakeValkey(), endpoint=endpoint)
    runtime = ValkeyAdapterFactory().create(context)
    runtime.open()
    try:
        probe = runtime.probe()
        assert probe.manifest.engine_version == "8.1.9"
        assert probe.evidence["persistence"] == "disabled"
        assert probe.evidence["evictionPolicy"] == "allkeys-lru"
        session = runtime.open_session(transactional=False)
        put = session.execute(
            execution_request(
                _operation("put", key=["real", 1], value={"server": "valkey"}, ttl_ms=500)
            )
        )
        assert put.data["stored"] is True  # type: ignore[index]
        hit = session.execute(execution_request(_operation("get", key=["real", 1])))
        assert hit.data["entry"]["value"] == {"server": "valkey"}  # type: ignore[index]
        key, _ = runtime.cache.physical_key(
            execution_request(_operation("get", key=["real", 1])).context,
            _operation("get", key=["real", 1]).resources[0],  # type: ignore[attr-defined]
            ["real", 1],
        )
        ttl = runtime.cache.client.pttl(key)
        assert 0 < ttl <= 500
        assert runtime.cache.delete(
            execution_request(_operation("get", key=["real", 1])).context,
            _operation("get", key=["real", 1]).resources[0],  # type: ignore[attr-defined]
            ["real", 1],
        )

        cache_context = execution_request(_operation("get", key="eviction-0")).context
        resource = _operation("get", key="eviction-0").resources[0]  # type: ignore[attr-defined]
        payload = "x" * 500_000
        for index in range(40):
            runtime.cache.put(
                cache_context,
                resource,
                f"eviction-{index}",
                payload,
                requested_ttl_ms=10_000,
            )
        misses = sum(
            not runtime.cache.lookup(cache_context, resource, f"eviction-{index}").hit
            for index in range(20)
        )
        assert misses > 0

        runtime.cache.put(cache_context, resource, "before-restart", "disposable")
        subprocess.run(  # noqa: S603 - fixed argv targets the disposable test server
            [
                docker,
                "compose",
                "-p",
                "meridian-valkey-standalone",
                "-f",
                str(Path(compose).resolve()),
                "restart",
                "valkey",
            ],
            check=True,
            timeout=30,
        )
        deadline = time.monotonic() + 15
        while time.monotonic() < deadline:
            try:
                restarted = runtime.cache.lookup(cache_context, resource, "before-restart")
                break
            except Exception:
                time.sleep(0.1)
        else:
            raise AssertionError("adapter did not reconnect after standalone restart")
        assert not restarted.hit
    finally:
        runtime.close()
