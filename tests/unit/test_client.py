# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

from collections.abc import Mapping
from typing import cast

import pytest
from meridian_storage.errors import ConfigurationError
from meridian_storage.runtime.config import BindingConfig
from meridian_storage.semantics import JsonValue
from meridian_storage.spi import AdapterCreateContext, SecretValue

from meridian_storage.adapters.valkey import client as client_module
from meridian_storage.adapters.valkey.client import ClientHandle, create_client_handle
from meridian_storage.adapters.valkey.configuration import ValkeySettings
from tests.conftest import make_context, settings_mapping
from tests.fakes import FakeValkey


def _settings(raw: Mapping[str, JsonValue]) -> ValkeySettings:
    return ValkeySettings.from_mapping(cast(Mapping[str, object], raw))


def _replace_binding(context: AdapterCreateContext, **changes: JsonValue) -> AdapterCreateContext:
    raw = context.binding.to_dict()
    raw.update(changes)
    return AdapterCreateContext(
        BindingConfig.from_mapping(raw, "binding"),
        context.identity,
        context.credential,
        context.tls_ca,
        context.tls_client_certificate,
    )


def test_standalone_client_receives_bounded_authenticated_options(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fake = FakeValkey()
    captured: dict[str, object] = {}

    def constructor(**kwargs: object) -> FakeValkey:
        captured.update(kwargs)
        return fake

    monkeypatch.setattr(client_module, "Valkey", constructor)
    raw = settings_mapping()
    context = make_context(fake, settings_override=raw)
    handle = create_client_handle(context, _settings(raw))
    assert handle.client is fake
    assert captured["username"] == "default"
    assert captured["password"] == b"meridian-test-password"
    assert captured["protocol"] == 2
    assert captured["max_connections"] == 16
    assert captured["decode_responses"] is False


def test_server_tls_material_is_verified_and_passed(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fake = FakeValkey()
    captured: dict[str, object] = {}
    base = make_context(fake)
    raw = base.binding.to_dict()
    raw["endpoint"] = "valkeys://cache.internal:6380"
    raw["tls"] = {
        "mode": "server",
        "serverName": "cache.internal",
        "caRef": {"provider": "test", "reference": "ca"},
        "clientCertificateRef": None,
    }
    raw_settings = cast(Mapping[str, JsonValue], raw["settings"])
    context = AdapterCreateContext(
        BindingConfig.from_mapping(raw, "binding"),
        base.identity,
        base.credential,
        SecretValue(b"-----BEGIN CERTIFICATE-----\nTEST\n-----END CERTIFICATE-----"),
    )

    def constructor(**kwargs: object) -> FakeValkey:
        captured.update(kwargs)
        return fake

    monkeypatch.setattr(client_module, "Valkey", constructor)
    create_client_handle(context, _settings(raw_settings))
    assert captured["ssl"] is True
    assert captured["ssl_cert_reqs"] == "required"
    assert captured["ssl_check_hostname"] is True


def test_sentinel_constructor_owns_all_handles(monkeypatch: pytest.MonkeyPatch) -> None:
    client = FakeValkey()
    closed: list[str] = []

    class Seed:
        def close(self) -> None:
            closed.append("seed")

    class Sentinel:
        def __init__(self, *_args: object, **kwargs: object) -> None:
            assert kwargs["min_other_sentinels"] == 2
            self.sentinels = [Seed(), Seed(), Seed()]

        def master_for(self, service_name: str, *, db: int) -> FakeValkey:
            assert service_name == "meridian-cache" and db == 0
            return client

    monkeypatch.setattr(client_module, "_RemappingSentinel", Sentinel)
    raw = settings_mapping(sentinel=True)
    raw["seedEndpoints"] = [
        "valkey://127.0.0.1:26392",
        "valkey://127.0.0.1:26393",
    ]
    context = make_context(
        client,
        sentinel=True,
        settings_override=raw,
        endpoint="valkey://127.0.0.1:26391",
    )
    handle = create_client_handle(context, _settings(raw))
    handle.close()
    assert client.closed and closed == ["seed", "seed", "seed"]


def test_service_resolution_and_endpoint_uniqueness() -> None:
    fake = FakeValkey()
    raw = settings_mapping()
    base = make_context(fake, settings_override=raw)
    context = _replace_binding(base, endpoint=None, serviceRef="service:cache")
    captured: list[tuple[object, ...]] = []

    def builder(*args: object) -> FakeValkey:
        captured.append(args)
        return fake

    create_client_handle(
        context,
        _settings(raw),
        service_resolver=lambda ref: ("valkey://resolved.internal:6379",),
        client_builder=builder,
    )
    assert captured[0][2][0].host == "resolved.internal"  # type: ignore[index,union-attr]
    with pytest.raises(ConfigurationError, match="serviceRef requires"):
        create_client_handle(context, _settings(raw), client_builder=builder)

    duplicate = settings_mapping()
    duplicate["seedEndpoints"] = ["valkey://127.0.0.1:6379"]
    with pytest.raises(ConfigurationError, match="unique"):
        create_client_handle(
            make_context(fake, settings_override=duplicate),
            _settings(duplicate),
            client_builder=builder,
        )


def test_client_construction_rejects_mismatched_or_unusable_security() -> None:
    fake = FakeValkey()
    raw = settings_mapping()
    settings = _settings(raw)
    base = make_context(fake)
    mismatch = _replace_binding(base, endpoint="valkeys://cache.internal")
    with pytest.raises(ConfigurationError, match="scheme"):
        create_client_handle(mismatch, settings)

    bad_identity = AdapterCreateContext(base.binding, SecretValue(b"\xff"), base.credential)
    with pytest.raises(ConfigurationError, match="UTF-8"):
        create_client_handle(bad_identity, settings)

    many = settings_mapping()
    many["seedEndpoints"] = ["valkey://cache-two.internal"]
    with pytest.raises(ConfigurationError, match="exactly one"):
        create_client_handle(make_context(fake, settings_override=many), _settings(many))


def test_client_handle_attempts_all_closers_and_raises_first_failure() -> None:
    calls: list[str] = []

    class Broken(FakeValkey):
        def close(self) -> None:
            calls.append("client")
            raise RuntimeError("client failure")

    def broken_closer() -> None:
        calls.append("closer")
        raise RuntimeError("closer failure")

    handle = ClientHandle(Broken(), (broken_closer,))
    with pytest.raises(RuntimeError, match="client failure"):
        handle.close()
    assert calls == ["client", "closer"]
