# SPDX-License-Identifier: Apache-2.0
"""Stable, redacted translation of Valkey client failures."""

from __future__ import annotations

from enum import StrEnum
from typing import Never

from meridian_storage.errors import (
    AuthenticationError,
    AuthorizationError,
    ConflictError,
    ErrorCode,
    InternalError,
    MeridianTimeoutError,
    RateLimitError,
    SafeCause,
    UnavailableError,
    ValidationError,
)

from valkey import exceptions as valkey_errors


class ValkeyErrorCode(StrEnum):
    AUTHENTICATION = "MERIDIAN_VALKEY_AUTHENTICATION"
    AUTHORIZATION = "MERIDIAN_VALKEY_AUTHORIZATION"
    CONFLICT = "MERIDIAN_VALKEY_CONFLICT"
    CORRUPT = "MERIDIAN_VALKEY_CORRUPT"
    CROSS_SLOT = "MERIDIAN_VALKEY_CROSS_SLOT"
    LIMIT = "MERIDIAN_VALKEY_LIMIT"
    TIMEOUT = "MERIDIAN_VALKEY_TIMEOUT"
    UNAVAILABLE = "MERIDIAN_VALKEY_UNAVAILABLE"


def translate_engine_error(
    exc: BaseException,
    *,
    operation_contract: str | None = None,
    resource_ref: str | None = None,
    request_id: str | None = None,
    execution_id: str | None = None,
) -> Never:
    """Raise a credential-free Meridian failure for one Valkey client error."""

    details = {
        "operation_contract": operation_contract,
        "resource_ref": resource_ref,
        "request_id": request_id,
        "execution_id": execution_id,
        "cause": SafeCause(type=type(exc).__name__),
        "adapter_provenance": {"engineErrorType": type(exc).__name__},
    }
    if isinstance(exc, valkey_errors.AuthenticationError):
        raise AuthenticationError(
            ValkeyErrorCode.AUTHENTICATION,
            "Valkey rejected the configured identity",
            **details,
        ) from exc
    if isinstance(exc, valkey_errors.AuthorizationError | valkey_errors.NoPermissionError):
        raise AuthorizationError(
            ValkeyErrorCode.AUTHORIZATION,
            "Valkey denied an adapter-owned command",
            **details,
        ) from exc
    if isinstance(exc, valkey_errors.TimeoutError):
        raise MeridianTimeoutError(
            ValkeyErrorCode.TIMEOUT,
            "Valkey did not complete before the bounded deadline",
            retryable=True,
            **details,
        ) from exc
    if isinstance(exc, valkey_errors.ClusterCrossSlotError):
        raise ValidationError(
            ValkeyErrorCode.CROSS_SLOT,
            "Valkey rejected keys that do not share an atomic hash slot",
            **details,
        ) from exc
    if isinstance(exc, valkey_errors.WatchError):
        raise ConflictError(
            ValkeyErrorCode.CONFLICT,
            "Valkey compare-and-set contention exceeded its retry bound",
            retryable=True,
            **details,
        ) from exc
    if isinstance(exc, valkey_errors.MaxConnectionsError | valkey_errors.OutOfMemoryError):
        raise RateLimitError(
            ValkeyErrorCode.LIMIT,
            "Valkey rejected work because a configured capacity limit was reached",
            retryable=True,
            **details,
        ) from exc
    if isinstance(
        exc,
        valkey_errors.BusyLoadingError
        | valkey_errors.ClusterDownError
        | valkey_errors.ConnectionError
        | valkey_errors.MasterDownError
        | valkey_errors.ReadOnlyError
        | valkey_errors.TryAgainError,
    ):
        raise UnavailableError(
            ValkeyErrorCode.UNAVAILABLE,
            "Valkey is temporarily unavailable",
            retryable=True,
            **details,
        ) from exc
    if isinstance(exc, valkey_errors.DataError | valkey_errors.ResponseError):
        raise ValidationError(
            ErrorCode.OPERATION_INVALID,
            "Valkey rejected the bounded adapter request",
            **details,
        ) from exc
    raise InternalError(
        ErrorCode.ADAPTER_FAILURE,
        "the Valkey client failed without a recognized safe category",
        **details,
    ) from exc


__all__ = ["ValkeyErrorCode", "translate_engine_error"]
