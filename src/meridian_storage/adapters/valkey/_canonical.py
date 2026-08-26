# SPDX-License-Identifier: Apache-2.0
"""Small deterministic helpers shared by the Valkey adapter."""

from __future__ import annotations

import base64
import hashlib
import math
import re
from collections.abc import Mapping, Sequence
from typing import cast

from meridian_storage.semantics import JsonValue, canonical_json_bytes

_FINGERPRINT_RE = re.compile(r"^sha256:[0-9a-f]{64}$")
_SAFE_TOKEN_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.:/-]{0,255}$")


def sha256_hex(value: bytes) -> str:
    """Return a lowercase SHA-256 fingerprint."""

    return "sha256:" + hashlib.sha256(value).hexdigest()


def sha1_hex(value: bytes) -> str:
    """Return the digest format used by Valkey's SCRIPT commands."""

    return "sha1:" + hashlib.sha1(value, usedforsecurity=False).hexdigest()


def base64url_digest(value: bytes) -> str:
    """Return an unpadded base64url SHA-256 digest."""

    return base64.urlsafe_b64encode(hashlib.sha256(value).digest()).rstrip(b"=").decode("ascii")


def require_fingerprint(value: object, field_name: str) -> str:
    if not isinstance(value, str) or _FINGERPRINT_RE.fullmatch(value) is None:
        raise ValueError(f"{field_name} must be a lowercase SHA-256 fingerprint")
    return value


def safe_token(value: object, field_name: str, *, maximum: int = 256) -> str:
    if (
        not isinstance(value, str)
        or not value
        or len(value.encode("utf-8")) > maximum
        or _SAFE_TOKEN_RE.fullmatch(value) is None
    ):
        raise ValueError(f"{field_name} must be a bounded safe token")
    return value


def integer(value: object, field_name: str, minimum: int, maximum: int) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or not minimum <= value <= maximum:
        raise ValueError(f"{field_name} must be between {minimum} and {maximum}")
    return value


def boolean(value: object, field_name: str) -> bool:
    if not isinstance(value, bool):
        raise ValueError(f"{field_name} must be boolean")
    return value


def json_scalar_or_array(value: object, field_name: str) -> JsonValue:
    """Validate the Cache Catalog's non-null scalar-or-array key contract."""

    if value is None or isinstance(value, Mapping):
        raise ValueError(f"{field_name} must be a non-null JSON scalar or array")
    if isinstance(value, Sequence) and not isinstance(value, str | bytes | bytearray):
        normalized = cast(JsonValue, list(value))
    elif isinstance(value, str | int | float | bool):
        if isinstance(value, float) and not math.isfinite(value):
            raise ValueError(f"{field_name} must contain finite JSON values")
        normalized = cast(JsonValue, value)
    else:
        raise ValueError(f"{field_name} must be a non-null JSON scalar or array")
    canonical_json_bytes(normalized)
    return normalized


def json_mapping(value: object, field_name: str) -> Mapping[str, object]:
    if not isinstance(value, Mapping) or any(not isinstance(key, str) for key in value):
        raise ValueError(f"{field_name} must be an object")
    return cast(Mapping[str, object], value)


__all__ = [
    "base64url_digest",
    "boolean",
    "integer",
    "json_mapping",
    "json_scalar_or_array",
    "require_fingerprint",
    "safe_token",
    "sha1_hex",
    "sha256_hex",
]
