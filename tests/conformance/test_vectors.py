# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

import base64
import json
from collections.abc import Mapping
from pathlib import Path
from typing import cast

import jsonschema
import pytest
from meridian_storage.context import OperationContext
from meridian_storage.registry import ResourceRef

from meridian_storage.adapters.valkey.atomic import (
    CAS_SCRIPT_DIGEST,
    RELEASE_LEASE_SCRIPT_DIGEST,
)
from meridian_storage.adapters.valkey.codec import EnvelopeCodec
from meridian_storage.adapters.valkey.configuration import ValkeySettings
from meridian_storage.adapters.valkey.descriptor import capability_manifest
from meridian_storage.adapters.valkey.key import KeyEncoder, KeyMaterial, hash_slot
from tests.conftest import SCHEMA, settings_mapping

pytestmark = pytest.mark.conformance
ROOT = Path(__file__).parents[2]


def _vectors() -> dict[str, object]:
    path = ROOT / "evidence/conformance-vectors.json"
    return cast(dict[str, object], json.loads(path.read_text()))


def test_vector_document_matches_its_schema() -> None:
    schema = json.loads((ROOT / "contracts/conformance-vectors.schema.json").read_text())
    jsonschema.Draft202012Validator.check_schema(schema)
    jsonschema.validate(_vectors(), schema)


def test_key_vectors_are_byte_exact_and_private() -> None:
    vectors = _vectors()
    keys = cast(dict[str, object], vectors["keyEncoding"])
    atomic = cast(dict[str, object], keys["atomicGroup"])
    context = OperationContext(
        principal_ref="principal:test",
        request_id="request-vector",
        tenant="tenant-a",
        scope={"region": "us-test-1"},
    )
    encoder = KeyEncoder("vector-namespace", 3, maximum_key_bytes=512)
    resource = ResourceRef.parse("cache:demo.items")
    plain = encoder.encode(KeyMaterial(context, resource, ["customer", 17], SCHEMA))
    left = encoder.encode(KeyMaterial(context, resource, "left", SCHEMA, "group-a"))
    right = encoder.encode(KeyMaterial(context, resource, "right", SCHEMA, "group-a"))
    assert plain.decode() == keys["plain"]
    assert left.decode() == atomic["left"]
    assert right.decode() == atomic["right"]
    assert hash_slot(left) == hash_slot(right) == atomic["slot"]
    assert not any(secret.encode() in plain for secret in ("tenant-a", "customer", "demo.items"))


def test_envelope_vectors_are_byte_exact_and_decodable() -> None:
    vectors = _vectors()
    envelopes = cast(dict[str, dict[str, str]], vectors["envelopes"])
    codec = EnvelopeCodec(maximum_value_bytes=4096, clock_ms=lambda: 1_700_000_000_000)
    canonical = codec.encode(
        {"b": 2, "a": [True, None]},
        serializer_id="canonical-json",
        schema_fingerprint=SCHEMA,
        ttl_ms=60_000,
        source_version="source-7",
    )
    raw = codec.encode(
        b"\x00\xffmeridian",
        serializer_id="raw-bytes",
        schema_fingerprint=SCHEMA,
        ttl_ms=5000,
    )
    assert base64.b64encode(canonical).decode() == envelopes["canonicalJson"]["encodedBase64"]
    assert codec.extract_version(canonical) == envelopes["canonicalJson"]["version"]
    assert base64.b64encode(raw).decode() == envelopes["rawBytes"]["encodedBase64"]
    assert codec.extract_version(raw) == envelopes["rawBytes"]["version"]


def test_script_and_descriptor_vectors_are_exact() -> None:
    vectors = _vectors()
    scripts = cast(dict[str, str], vectors["scripts"])
    descriptor = cast(dict[str, object], vectors["descriptor"])
    assert scripts == {
        "compareAndSet": CAS_SCRIPT_DIGEST,
        "releaseLease": RELEASE_LEASE_SCRIPT_DIGEST,
    }
    settings = ValkeySettings.from_mapping(cast(Mapping[str, object], settings_mapping()))
    manifest = capability_manifest("8.1.9", settings)
    assert manifest.fingerprint == descriptor["capabilityFingerprint"]
    assert list(manifest.available_operation_contracts) == descriptor["operationContracts"]
