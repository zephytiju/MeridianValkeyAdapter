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
from tests.conftest import RESOURCE, execution_request, make_context, settings_mapping
from tests.fakes import FakeValkey

pytestmark = pytest.mark.cluster


def _operation(method: str, **arguments: object) -> object:
    provider = CacheCatalogProvider()
    expression = getattr(provider.create_surface(), method)(resource=RESOURCE, **arguments)
    return provider.normalize(expression)


def test_existing_runtime_recovers_after_sentinel_promotes_replica() -> None:
    seeds = os.environ.get("MERIDIAN_VALKEY_SENTINEL_SEEDS")
    compose = os.environ.get("MERIDIAN_VALKEY_SENTINEL_COMPOSE")
    if seeds is None or compose is None:
        pytest.skip("Sentinel conformance environment is not configured")
    docker = shutil.which("docker")
    if docker is None:
        pytest.fail("docker is required for Sentinel conformance")
    endpoints = seeds.split(",")
    settings = settings_mapping(sentinel=True)
    settings["seedEndpoints"] = endpoints[1:]
    context = make_context(
        FakeValkey(), sentinel=True, settings_override=settings, endpoint=endpoints[0]
    )
    ports = {"172.30.99.10": 6392, "172.30.99.11": 6393, "172.30.99.12": 6394}

    def map_address(address: tuple[str, int]) -> tuple[str, int]:
        host, port = address
        return ("127.0.0.1", ports.get(host, port))

    runtime = ValkeyAdapterFactory(address_mapper=map_address).create(context)
    runtime.open()
    try:
        assert runtime.probe().evidence["replicas"] == "2"
        session = runtime.open_session(transactional=False)
        initial = session.execute(
            execution_request(_operation("put", key="before-failover", value=1))
        )
        assert initial.data["stored"] is True  # type: ignore[index]
        time.sleep(0.5)
        subprocess.run(  # noqa: S603 - fixed argv targets the disposable test topology
            [
                docker,
                "compose",
                "-p",
                "meridian-valkey-failover",
                "-f",
                str(Path(compose).resolve()),
                "stop",
                "primary",
            ],
            check=True,
            timeout=30,
        )
        deadline = time.monotonic() + 30
        last_error: BaseException | None = None
        while time.monotonic() < deadline:
            try:
                result = session.execute(
                    execution_request(_operation("put", key="after-failover", value=2))
                )
                if result.data["stored"] is True:  # type: ignore[index]
                    break
            except BaseException as exc:
                last_error = exc
            time.sleep(0.25)
        else:
            raise AssertionError("Sentinel did not restore a writable primary") from last_error
        hit = session.execute(execution_request(_operation("get", key="after-failover")))
        assert hit.data["entry"]["value"] == 2  # type: ignore[index]
    finally:
        runtime.close()
