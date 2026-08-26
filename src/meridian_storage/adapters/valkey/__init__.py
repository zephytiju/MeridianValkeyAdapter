# SPDX-License-Identifier: Apache-2.0
"""Valkey adapter for Meridian V1's disposable Cache Catalog."""

from ._version import __version__
from .atomic import (
    CAS_SCRIPT_DIGEST,
    RELEASE_LEASE_SCRIPT_DIGEST,
    AtomicExecutor,
    CompareAndSetResult,
)
from .cache import CacheEventSink, CacheLookup, CacheMetrics, ValkeyCache
from .codec import (
    DecodedEnvelope,
    EnvelopeCodec,
    EnvelopeCorruptionError,
    EnvelopeError,
    EnvelopeExpiredError,
    EnvelopeSchemaMismatchError,
    EnvelopeTooLargeError,
)
from .configuration import (
    AdapterLimits,
    MemoryExpectation,
    ResourceCachePolicy,
    SingleFlightPolicy,
    TopologyExpectation,
    TTLPolicy,
    ValkeySettings,
)
from .descriptor import (
    ADAPTER_CONTRACT_VERSION,
    ADAPTER_ID,
    SUPPORTED_ENGINE_VERSION,
    adapter_descriptor,
    capability_manifest,
)
from .key import KeyEncoder, KeyMaterial, hash_slot, require_shared_hash_slot
from .runtime import (
    ValkeyAdapterFactory,
    ValkeyAdapterRuntime,
    ValkeyAdapterSession,
    expected_capability_fingerprint,
)

__all__ = [
    "ADAPTER_CONTRACT_VERSION",
    "ADAPTER_ID",
    "CAS_SCRIPT_DIGEST",
    "RELEASE_LEASE_SCRIPT_DIGEST",
    "SUPPORTED_ENGINE_VERSION",
    "AdapterLimits",
    "AtomicExecutor",
    "CacheEventSink",
    "CacheLookup",
    "CacheMetrics",
    "CompareAndSetResult",
    "DecodedEnvelope",
    "EnvelopeCodec",
    "EnvelopeCorruptionError",
    "EnvelopeError",
    "EnvelopeExpiredError",
    "EnvelopeSchemaMismatchError",
    "EnvelopeTooLargeError",
    "KeyEncoder",
    "KeyMaterial",
    "MemoryExpectation",
    "ResourceCachePolicy",
    "SingleFlightPolicy",
    "TTLPolicy",
    "TopologyExpectation",
    "ValkeyAdapterFactory",
    "ValkeyAdapterRuntime",
    "ValkeyAdapterSession",
    "ValkeyCache",
    "ValkeySettings",
    "__version__",
    "adapter_descriptor",
    "capability_manifest",
    "expected_capability_fingerprint",
    "hash_slot",
    "require_shared_hash_slot",
]
