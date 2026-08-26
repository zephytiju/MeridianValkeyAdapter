# SPDX-License-Identifier: Apache-2.0
"""Valkey client construction with topology and credentials kept behind the Adapter."""

from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from typing import Any, Protocol, cast, runtime_checkable
from urllib.parse import urlsplit

from meridian_storage.errors import ConfigurationError, ErrorCode
from meridian_storage.spi import AdapterCreateContext

from valkey import Valkey
from valkey.sentinel import Sentinel

from .configuration import TOPOLOGY_SENTINEL, ValkeySettings


@runtime_checkable
class ClientProtocol(Protocol):
    def ping(self) -> object: ...

    def info(self, section: str | None = None) -> Mapping[object, object]: ...

    def config_get(self, pattern: str = "*") -> Mapping[object, object]: ...

    def execute_command(self, *args: object, **kwargs: object) -> object: ...

    def script_load(self, script: bytes | str) -> object: ...

    def script_exists(self, *shas: str) -> Sequence[object]: ...

    def evalsha(self, sha: str, numkeys: int, *keys_and_args: object) -> object: ...

    def get(self, name: bytes | str) -> object: ...

    def mget(self, keys: Sequence[bytes]) -> Sequence[object]: ...

    def set(self, name: bytes, value: object, **kwargs: object) -> object: ...

    def delete(self, *names: bytes) -> int: ...

    def pttl(self, name: bytes) -> int: ...

    def pipeline(self, transaction: bool = True) -> Any: ...

    def close(self) -> None: ...


class ServiceResolver(Protocol):
    def __call__(self, service_ref: str) -> tuple[str, ...]: ...


class AddressMapper(Protocol):
    def __call__(self, address: tuple[str, int]) -> tuple[str, int]: ...


ClientBuilder = Callable[
    [AdapterCreateContext, ValkeySettings, tuple["Endpoint", ...]], ClientProtocol
]


@dataclass(frozen=True, slots=True)
class Endpoint:
    host: str
    port: int
    tls: bool


@dataclass(slots=True)
class ClientHandle:
    client: ClientProtocol
    _closers: tuple[Callable[[], None], ...] = ()

    def close(self) -> None:
        failures: list[BaseException] = []
        try:
            self.client.close()
        except BaseException as exc:  # adapter close must attempt every owned handle
            failures.append(exc)
        for closer in self._closers:
            try:
                closer()
            except BaseException as exc:
                failures.append(exc)
        if failures:
            raise failures[0]


class _RemappingSentinel(Sentinel):
    def __init__(
        self,
        *args: object,
        address_mapper: AddressMapper | None,
        **kwargs: object,
    ) -> None:
        self._address_mapper = address_mapper
        super().__init__(*args, **kwargs)  # type: ignore[no-untyped-call]

    def discover_master(self, service_name: str) -> tuple[str, int]:
        address = cast(
            tuple[str, int],
            super().discover_master(service_name),  # type: ignore[no-untyped-call]
        )
        return self._address_mapper(address) if self._address_mapper is not None else address

    def discover_slaves(self, service_name: str) -> list[tuple[str, int]]:
        addresses = cast(
            list[tuple[str, int]],
            super().discover_slaves(service_name),  # type: ignore[no-untyped-call]
        )
        if self._address_mapper is None:
            return addresses
        return [self._address_mapper(address) for address in addresses]


def parse_endpoint(value: str) -> Endpoint:
    try:
        parsed = urlsplit(value)
        if parsed.scheme not in {"valkey", "valkeys"}:
            raise ValueError("endpoint scheme must be valkey or valkeys")
        if (
            parsed.username is not None
            or parsed.password is not None
            or parsed.query
            or parsed.fragment
            or parsed.path not in {"", "/"}
        ):
            raise ValueError("endpoint must not contain credentials, query, fragment, or path")
        if parsed.hostname is None:
            raise ValueError("endpoint requires a host")
        port = parsed.port or 6379
        if not 1 <= port <= 65_535:
            raise ValueError("endpoint port is invalid")
        return Endpoint(parsed.hostname, port, parsed.scheme == "valkeys")
    except (TypeError, ValueError) as exc:
        raise ConfigurationError(
            ErrorCode.CONFIG_INVALID,
            "Valkey Binding endpoint is invalid",
        ) from exc


def _resolved_endpoints(
    context: AdapterCreateContext,
    settings: ValkeySettings,
    service_resolver: ServiceResolver | None,
) -> tuple[Endpoint, ...]:
    binding = context.binding
    values: tuple[str, ...]
    if binding.endpoint is not None:
        values = (binding.endpoint, *settings.seed_endpoints)
    else:
        if binding.service_ref is None or service_resolver is None:
            raise ConfigurationError(
                ErrorCode.CONFIG_INVALID,
                "Valkey serviceRef requires a composition-root service resolver",
            )
        values = (*service_resolver(binding.service_ref), *settings.seed_endpoints)
    endpoints = tuple(parse_endpoint(item) for item in values)
    if not endpoints:
        raise ConfigurationError(ErrorCode.CONFIG_INVALID, "Valkey requires at least one endpoint")
    if len({(item.host, item.port, item.tls) for item in endpoints}) != len(endpoints):
        raise ConfigurationError(ErrorCode.CONFIG_INVALID, "Valkey endpoints must be unique")
    return endpoints


def create_client_handle(
    context: AdapterCreateContext,
    settings: ValkeySettings,
    *,
    service_resolver: ServiceResolver | None = None,
    client_builder: ClientBuilder | None = None,
    address_mapper: AddressMapper | None = None,
) -> ClientHandle:
    endpoints = _resolved_endpoints(context, settings, service_resolver)
    if client_builder is not None:
        return ClientHandle(client_builder(context, settings, endpoints))
    binding = context.binding
    tls_required = binding.tls.mode != "disabled"
    if any(endpoint.tls != tls_required for endpoint in endpoints):
        raise ConfigurationError(
            ErrorCode.CONFIG_INVALID,
            "Valkey endpoint scheme must match the Binding TLS policy",
        )
    if not tls_required and not settings.allow_insecure_test_profile:
        raise ConfigurationError(
            ErrorCode.CONFIG_INVALID,
            "plaintext Valkey is restricted to the explicit isolated test profile",
        )
    if binding.tls.mode == "mutual":
        raise ConfigurationError(
            ErrorCode.CONFIG_INVALID,
            "Valkey V1 supports authenticated server TLS, not mutual TLS materialization",
        )
    try:
        username = context.identity.reveal().decode("utf-8")
    except UnicodeDecodeError as exc:
        raise ConfigurationError(
            ErrorCode.CONFIG_INVALID,
            "Valkey identity must be a UTF-8 ACL username",
        ) from exc
    password = context.credential.reveal()
    common: dict[str, object] = {
        "username": username,
        "password": password,
        "socket_connect_timeout": binding.client.acquire_timeout_ms / 1000,
        "socket_timeout": binding.client.operation_timeout_ms / 1000,
        "health_check_interval": max(1, binding.client.idle_timeout_ms // 1000),
        "max_connections": binding.client.max_size,
        "decode_responses": False,
        # valkey-py 6.1.1's RESP3 HELLO/AUTH path performs its health PING
        # before authentication when a health interval is configured. RESP2
        # sends AUTH with check_health=False and preserves authenticated health
        # checks thereafter.
        "protocol": 2,
        "client_name": "meridian-storage-valkey/1.0.0",
    }
    if tls_required:
        if context.tls_ca is None or binding.tls.server_name is None:
            raise ConfigurationError(
                ErrorCode.CONFIG_INVALID,
                "server TLS requires resolved CA bytes and server name",
            )
        try:
            ca_data = context.tls_ca.reveal().decode("utf-8")
        except UnicodeDecodeError as exc:
            raise ConfigurationError(
                ErrorCode.CONFIG_INVALID,
                "Valkey TLS CA material must be PEM UTF-8",
            ) from exc
        common.update(
            {
                "ssl": True,
                "ssl_ca_data": ca_data,
                "ssl_cert_reqs": "required",
                "ssl_check_hostname": True,
            }
        )
    if settings.topology.mode == TOPOLOGY_SENTINEL:
        master_name = settings.topology.sentinel_master
        if master_name is None:
            raise ConfigurationError(
                ErrorCode.CONFIG_INVALID,
                "Sentinel topology requires its deployment-owned master name",
            )
        sentinel_addresses = [(item.host, item.port) for item in endpoints]
        sentinel = _RemappingSentinel(
            sentinel_addresses,
            min_other_sentinels=max(0, len(sentinel_addresses) - 1),
            sentinel_kwargs=common,
            address_mapper=address_mapper,
            **common,
        )
        client = cast(
            ClientProtocol,
            sentinel.master_for(  # type: ignore[no-untyped-call]
                master_name, db=settings.database
            ),
        )
        closers = tuple(cast(Callable[[], None], item.close) for item in sentinel.sentinels)
        return ClientHandle(client, closers)
    if len(endpoints) != 1:
        raise ConfigurationError(
            ErrorCode.CONFIG_INVALID,
            "standalone Valkey accepts exactly one endpoint",
        )
    endpoint = endpoints[0]
    arguments = cast(
        Any,
        {"host": endpoint.host, "port": endpoint.port, "db": settings.database, **common},
    )
    return ClientHandle(cast(ClientProtocol, Valkey(**arguments)))


__all__ = [
    "AddressMapper",
    "ClientBuilder",
    "ClientHandle",
    "ClientProtocol",
    "Endpoint",
    "ServiceResolver",
    "create_client_handle",
    "parse_endpoint",
]
