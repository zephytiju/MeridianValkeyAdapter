# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

import pytest
from valkey import exceptions as valkey_errors

from meridian_storage.adapters.valkey.errors import translate_engine_error


@pytest.mark.parametrize(
    ("engine_error", "message"),
    [
        (valkey_errors.AuthorizationError("denied"), "denied an adapter-owned command"),
        (valkey_errors.TimeoutError("slow"), "bounded deadline"),
        (valkey_errors.ClusterCrossSlotError("slot"), "atomic hash slot"),
        (valkey_errors.WatchError("watch"), "contention"),
        (valkey_errors.MaxConnectionsError("full"), "capacity limit"),
        (valkey_errors.DataError("bad"), "bounded adapter request"),
        (RuntimeError("unknown secret"), "without a recognized safe category"),
    ],
)
def test_all_engine_errors_have_stable_safe_categories(
    engine_error: BaseException, message: str
) -> None:
    with pytest.raises(Exception, match=message) as captured:
        translate_engine_error(engine_error)
    assert "unknown secret" not in str(captured.value)
