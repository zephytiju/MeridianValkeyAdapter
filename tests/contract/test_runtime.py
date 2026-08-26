# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

from collections.abc import Mapping
from typing import cast

import pytest
from meridian_storage.errors import (
    CompatibilityError,
    ConfigurationError,
    LifecycleError,
    ValidationError,
)
from meridian_storage.registry import ResourceRef
from meridian_storage.semantics import JsonValue
from meridian_storage.semantics.catalogs import CacheCatalogProvider
from meridian_storage.spi import PhysicalResource
from valkey.exceptions import AuthenticationError
from valkey.exceptions import ConnectionError as ValkeyConnectionError

from meridian_storage.adapters.valkey import ValkeyAdapterFactory
from meridian_storage.adapters.valkey.client import create_client_handle
from meridian_storage.adapters.valkey.configuration import ValkeySettings
from meridian_storage.adapters.valkey.descriptor import (
    DATA_PLANE_CONTRACTS,
    SUPPORTED_ENGINE_VERSION,
    adapter_descriptor,
    capability_manifest,
)
from meridian_storage.adapters.valkey.errors import translate_engine_error
from tests.conftest import (
    RESOURCE,
    SCHEMA,
    execution_request,
    fake_builder,
    make_context,
    settings_mapping,
)
from tests.fakes import FakeValkey, RecordingSink


def _operation(method: str, **arguments: object) -> object:
    provider = CacheCatalogProvider()
    surface = provider.create_surface()
    expression = getattr(surface, method)(resource=RESOURCE, **arguments)
    return provider.normalize(expression)


def _runtime(client: FakeValkey, sink: RecordingSink | None = None) -> object:
    factory = ValkeyAdapterFactory(client_builder=fake_builder(client), event_sink=sink)
    return factory.create(make_context(client))


def test_descriptor_is_exact_disposable_and_deterministic() -> None:
    settings = ValkeySettings.from_mapping(cast(Mapping[str, object], settings_mapping()))
    descriptor = adapter_descriptor(settings)
    first = capability_manifest(SUPPORTED_ENGINE_VERSION, settings)
    second = capability_manifest(SUPPORTED_ENGINE_VERSION, settings)
    assert first.fingerprint == second.fingerprint
    assert first.available_operation_contracts == DATA_PLANE_CONTRACTS
    advertised = {item.operation_contract for item in descriptor.capabilities}
    assert advertised == set(DATA_PLANE_CONTRACTS)
    serialized = repr(descriptor.to_dict()).lower()
    assert "structured" not in serialized
    assert "evidence" not in serialized
    assert "durable" not in serialized
    assert "transaction" not in serialized


def test_runtime_open_probe_physical_and_lifecycle(fake_client: FakeValkey) -> None:
    runtime = _runtime(fake_client)
    with pytest.raises(LifecycleError):
        runtime.open_session(transactional=False)  # type: ignore[attr-defined]
    runtime.open()  # type: ignore[attr-defined]
    probe = runtime.probe()  # type: ignore[attr-defined]
    assert probe.evidence["persistence"] == "disabled"
    assert probe.evidence["topology"] == "standalone"
    physical = PhysicalResource(
        ResourceRef.parse(RESOURCE),
        "sha256:" + "3" * 64,
        SCHEMA,
        "disposable-cache",
    )
    verified = runtime.verify_physical((physical,))  # type: ignore[attr-defined]
    assert verified.evidence["authority"] == "disposable-only"
    with pytest.raises(ValidationError, match="non-Cache"):
        runtime.verify_physical(  # type: ignore[attr-defined]
            (
                PhysicalResource(
                    ResourceRef.parse("structured:demo.table"),
                    "sha256:" + "4" * 64,
                    SCHEMA,
                    "structured",
                ),
            )
        )
    with pytest.raises(ValidationError, match="transactions"):
        runtime.open_session(transactional=True)  # type: ignore[attr-defined]
    runtime.close()  # type: ignore[attr-defined]
    assert fake_client.closed
    with pytest.raises(LifecycleError):
        runtime.probe()  # type: ignore[attr-defined]


def test_released_cache_operations_execute_through_core_spi(fake_client: FakeValkey) -> None:
    runtime = _runtime(fake_client)
    runtime.open()  # type: ignore[attr-defined]
    session = runtime.open_session(transactional=False)  # type: ignore[attr-defined]

    miss = session.execute(execution_request(_operation("get", key="a")))
    assert miss.data == {"hit": False}

    put = session.execute(
        execution_request(
            _operation("put", key="a", value={"answer": 42}, ttl_ms=500, source_version="v1")
        )
    )
    assert put.data["stored"] is True  # type: ignore[index]
    version = put.data["entry"]["version"]  # type: ignore[index]

    hit = session.execute(execution_request(_operation("get", key="a")))
    assert hit.data["hit"] is True  # type: ignore[index]
    assert hit.data["entry"]["value"] == {"answer": 42}  # type: ignore[index]

    conflict = session.execute(
        execution_request(_operation("put_if_absent", key="a", value="other"))
    )
    assert conflict.data["stored"] is False  # type: ignore[index]

    swapped = session.execute(
        execution_request(
            _operation("compare_and_set", key="a", expected_version=version, value="new")
        )
    )
    assert swapped.data["swapped"] is True  # type: ignore[index]

    session.execute(execution_request(_operation("put", key="b", value=2)))
    invalidated = session.execute(
        execution_request(_operation("invalidate", selector={"keys": ["a", "b"]}))
    )
    assert invalidated.data == {"invalidated": 2, "requested": 2}

    deleted = session.execute(execution_request(_operation("delete", key="absent")))
    assert deleted.data == {"deleted": False}
    assert session.execute(execution_request(_operation("get", key="a"))).data == {"hit": False}


def test_raw_byte_resource_uses_canonical_base64url_operations(fake_client: FakeValkey) -> None:
    raw = settings_mapping()
    resources = cast(dict[str, dict[str, JsonValue]], raw["resources"])
    resources[RESOURCE]["serializerId"] = "raw-bytes"
    runtime = ValkeyAdapterFactory(client_builder=fake_builder(fake_client)).create(
        make_context(fake_client, settings_override=raw)
    )
    runtime.open()
    session = runtime.open_session(transactional=False)
    put = session.execute(execution_request(_operation("put", key="binary", value="AP8")))
    assert put.data["entry"]["value"] == "AP8"  # type: ignore[index]
    assert put.data["entry"]["valueEncoding"] == "base64url"  # type: ignore[index]
    hit = session.execute(execution_request(_operation("get", key="binary")))
    assert hit.data["entry"]["value"] == "AP8"  # type: ignore[index]
    for value in ("not+url", "AA==", 1):
        with pytest.raises(ValidationError, match="serializer"):
            session.execute(execution_request(_operation("put", key="bad", value=value)))


def test_get_degrades_to_miss_but_mutation_reports_unavailable(fake_client: FakeValkey) -> None:
    runtime = _runtime(fake_client)
    runtime.open()  # type: ignore[attr-defined]
    session = runtime.open_session(transactional=False)  # type: ignore[attr-defined]
    fake_client.raise_connection = True
    degraded = session.execute(execution_request(_operation("get", key="a")))
    assert degraded.data == {"hit": False, "degraded": True}
    assert degraded.provenance["cacheOutcome"] == "unavailable-miss"
    with pytest.raises(Exception, match="temporarily unavailable") as captured:
        session.execute(execution_request(_operation("put", key="a", value=1)))
    assert "secret-host" not in str(captured.value)
    assert "do-not-leak" not in str(captured.value)


def test_probe_rejects_engine_and_deployment_drift(fake_client: FakeValkey) -> None:
    for attribute, value, message in (
        ("engine_version", "8.1.8", "version"),
        ("maxmemory", 0, "maxmemory"),
        ("eviction", "noeviction", "eviction"),
        ("appendonly", "yes", "persistence"),
        ("role", "slave", "primary"),
    ):
        fresh = FakeValkey()
        setattr(fresh, attribute, value)
        with pytest.raises(CompatibilityError, match=message):
            _runtime(fresh).open()  # type: ignore[attr-defined]


def test_factory_and_client_construction_fail_closed(fake_client: FakeValkey) -> None:
    context = make_context(fake_client)
    wrong = context.binding.to_dict()
    wrong["adapterId"] = "org.example.other"
    from meridian_storage.runtime.config import BindingConfig
    from meridian_storage.spi import AdapterCreateContext

    wrong_context = AdapterCreateContext(
        BindingConfig.from_mapping(wrong, "binding"), context.identity, context.credential
    )
    with pytest.raises(ConfigurationError, match="identity"):
        ValkeyAdapterFactory(client_builder=fake_builder(fake_client)).create(wrong_context)

    secure_settings = settings_mapping()
    secure_settings["allowInsecureTestProfile"] = False
    secure_context = make_context(fake_client, settings_override=secure_settings)
    settings = ValkeySettings.from_mapping(cast(Mapping[str, object], secure_settings))
    with pytest.raises(ConfigurationError, match="plaintext"):
        create_client_handle(secure_context, settings)


@pytest.mark.parametrize(
    ("exc", "text"),
    [
        (AuthenticationError("bad secret"), "rejected the configured identity"),
        (ValkeyConnectionError("secret.internal password=x"), "temporarily unavailable"),
    ],
)
def test_engine_error_translation_is_redacted(exc: BaseException, text: str) -> None:
    with pytest.raises(Exception, match=text) as captured:
        translate_engine_error(exc, resource_ref=RESOURCE, request_id="request-1")
    rendered = str(captured.value)
    assert "bad secret" not in rendered
    assert "secret.internal" not in rendered
    assert "password=x" not in rendered


def test_session_rejects_unadvertised_and_invalid_invalidation(fake_client: FakeValkey) -> None:
    runtime = _runtime(fake_client)
    runtime.open()  # type: ignore[attr-defined]
    session = runtime.open_session(transactional=False)  # type: ignore[attr-defined]
    provider = CacheCatalogProvider()
    create = provider.normalize(
        provider.create_surface().create_resource(namespace="demo", name="items")
    )
    with pytest.raises(ValidationError, match="only one-Resource"):
        session.execute(execution_request(create))
    with pytest.raises(ValidationError, match="scans are forbidden"):
        session.execute(
            execution_request(_operation("invalidate", selector={"prefix": "anything"}))
        )
    session.close()
    with pytest.raises(LifecycleError):
        session.execute(execution_request(_operation("get", key="a")))
