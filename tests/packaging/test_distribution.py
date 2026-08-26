# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

import email
import json
import tarfile
import zipfile
from pathlib import Path

import jsonschema
import pytest

pytestmark = pytest.mark.packaging
ROOT = Path(__file__).parents[2]
DIST = ROOT / "dist"


def _wheel() -> Path:
    matches = list(DIST.glob("meridian_storage_valkey-1.0.0-py3-none-any.whl"))
    assert len(matches) == 1, "build the 1.0.0 wheel before packaging tests"
    return matches[0]


def _sdist() -> Path:
    matches = list(DIST.glob("meridian_storage_valkey-1.0.0.tar.gz"))
    assert len(matches) == 1, "build the 1.0.0 sdist before packaging tests"
    return matches[0]


def test_wheel_has_one_namespace_package_and_required_material() -> None:
    with zipfile.ZipFile(_wheel()) as archive:
        names = set(archive.namelist())
        metadata_name = next(name for name in names if name.endswith(".dist-info/METADATA"))
        metadata = email.message_from_bytes(archive.read(metadata_name))
    assert "meridian_storage/__init__.py" not in names
    assert "meridian_storage/adapters/valkey/__init__.py" in names
    assert "meridian_storage/adapters/valkey/py.typed" in names
    assert "meridian_storage/adapters/valkey/compatibility.json" in names
    assert "meridian_storage/adapters/valkey/evidence/conformance-vectors.json" in names
    assert any(name.endswith(".dist-info/licenses/LICENSE") for name in names)
    assert any(name.endswith(".dist-info/licenses/NOTICE") for name in names)
    assert metadata["Name"] == "meridian-storage-valkey"
    assert metadata["Version"] == "1.0.0"
    assert metadata["License-Expression"] == "Apache-2.0"
    requirements = metadata.get_all("Requires-Dist")
    assert requirements[:3] == [
        "meridian-storage-core==1.0.0",
        "meridian-storage-semantics==1.0.0",
        "valkey==6.1.1",
    ]


def test_entry_point_and_packaged_contracts_are_exact() -> None:
    with zipfile.ZipFile(_wheel()) as archive:
        entry_name = next(name for name in archive.namelist() if name.endswith("entry_points.txt"))
        entry_points = archive.read(entry_name).decode()
        compatibility = json.loads(
            archive.read("meridian_storage/adapters/valkey/compatibility.json")
        )
        schema = json.loads(
            archive.read(
                "meridian_storage/adapters/valkey/contracts/conformance-vectors.schema.json"
            )
        )
    assert entry_points.strip().splitlines() == [
        "[meridian_storage.adapters]",
        "valkey = meridian_storage.adapters.valkey:ValkeyAdapterFactory",
    ]
    assert compatibility["distribution"] == "meridian-storage-valkey"
    assert compatibility["version"] == "1.0.0"
    jsonschema.Draft202012Validator.check_schema(schema)


def test_sdist_contains_tests_docs_lock_and_no_release_self_reference() -> None:
    with tarfile.open(_sdist(), "r:gz") as archive:
        names = set(archive.getnames())
    prefix = "meridian_storage_valkey-1.0.0/"
    required = {
        "LICENSE",
        "NOTICE",
        "README.md",
        "requirements.lock",
        "tests/integration/docker/compose.single.yml",
        "tests/cluster/docker/compose.sentinel.yml",
        "evidence/conformance-vectors.json",
    }
    assert {prefix + name for name in required} <= names
    assert prefix + "evidence/release-evidence.json" not in names
