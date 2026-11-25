"""Configuration constants for the EcoPlots library.

Defines the API base URL, cache settings, magic bytes for ``.ecoproj`` binary
project files, and the facet lists that control which filter keys are valid in
each operational mode (``"observations"`` / ``"samples"``).
"""

import hashlib
import os
import tempfile

# Default configuration

try:
    from .version import __version__ as VERSION
except ImportError:  # pragma: no cover - defensive fallback for source checkouts
    VERSION = "1.0.0"

PRODUCTION_API_BASE_URL = "https://ecoplots.tern.org.au"
DEV_API_BASE_URL = "https://ecoplots-test.tern.org.au"
API_BASE_URL_ENV_VAR = "TERNDATA_ECOPLOTS_API_BASE_URL"


def _is_dev_version(version: str) -> bool:
    """Return True for PEP 440 development versions."""
    return ".dev" in version.lower()


def _resolve_api_base_url(version: str = VERSION) -> str:
    """Resolve the EcoPlots API base URL for the installed package."""
    override = os.getenv(API_BASE_URL_ENV_VAR)
    if override:
        return override.rstrip("/")
    if _is_dev_version(version):
        return DEV_API_BASE_URL
    return PRODUCTION_API_BASE_URL


API_BASE_URL = _resolve_api_base_url()

CACHE_EXPIRE_SECONDS = 60 * 60 * 24 * 14  # Default cache expiration time: 14 days
CACHE_DIR = os.path.join(
    tempfile.gettempdir(),
    "ecoplots_labels_cache",
    hashlib.sha256(API_BASE_URL.rstrip("/").encode()).hexdigest()[:16],
)

MAGIC = b"ECPJ"

# Public subfacets resolve independently and are grouped only in API payloads.
EQUIPMENT_FACETS = {
    "platform_type": ("platform", "type", "platform_type"),
    "platform_instance": ("platform", "instance", "platform_id"),
    "platform_deployment": ("platform", "deployment", "deployment_id"),
    "instrument_type": ("instrument", "type", "system_type"),
    "instrument_deployment": ("instrument", "deployment", "deployment_id"),
}
EQUIPMENT_GROUPS = {
    group: {child: name for name, (parent, child, _) in EQUIPMENT_FACETS.items() if parent == group}
    for group in ("platform", "instrument")
}

FACET_API_NAMES = {
    "platform": "platform_id",
    "deployment": "deployment_id",
    "instrument": "system_id",
}
FACET_ALIASES = {
    **{api_name: facet for facet, api_name in FACET_API_NAMES.items()},
    "platforms": "platform",
    "deployments": "deployment",
    "instruments": "instrument",
    "system_type": "instrument_type",
    **{name + "s": name for name in EQUIPMENT_FACETS},
}
FACET_VOCAB_NAMES = {
    **FACET_API_NAMES,
    **{name: vocabulary for name, (_, _, vocabulary) in EQUIPMENT_FACETS.items()},
}

VOCAB_FACETS = [
    "region_type",
    "region",
    "dataset",
    "site_id",
    "site_visit_id",
    "platform_id",
    "deployment_id",
    "system_id",
    "feature_type",
    "observed_property",
    "attributes",
    # "units",
    "used_procedure",
    "system_type",
    "platform_type",
    "core_attributes",
]

QUERY_FACETS = [
    "speciesname",
    *EQUIPMENT_FACETS,
    "region_type",
    "region",
    "dataset",
    "site_id",
    "site_visit_id",
    "platform",
    "deployment",
    "instrument",
    "feature_type",
    "observed_property",
    "spatial",
    "project",
    "date_from",
    "date_to",
    # "description", #TODO: AI based description to facet mapping
]

SAMPLE_QUERY_FACETS = [
    "region_type",
    "region",
    "dataset",
    "site_id",
    "soil_subsite_id",
    "soil_depth_range",
    "speciesname",
    "material_sample_type",
    "used_procedure",
    "has_image",
    "spatial",
    "date_from",
    "date_to",
]

DISCOVERY_FACETS = [
    *EQUIPMENT_FACETS,
    "region",
    "dataset",
    "feature_type",
    "observed_property",
    "site_id",
    "used_procedure",
    "platform",
    "deployment",
    "instrument",
]

SAMPLE_DISCOVERY_FACETS = [
    "region",
    "dataset",
    "material_sample_type",
    "site_id",
    "used_procedure",
    "sample_name",
]

DISCOVERY_ATTRIBUTES = [
    "dataset",
    "feature_type",
    "observation",
    "site",
    "site_visit",
]

MATERIAL_SAMPLE_TYPE_MAP = {
    "http://linked.data.gov.au/def/tern-cv/eee45444-7940-4ff5-801f-8eef5452943f": "Plant Tissue Sample",
    "http://linked.data.gov.au/def/tern-cv/18317af1-7c83-468d-883e-ba791500c6e3": "Plant Voucher Specimen",
    "http://linked.data.gov.au/def/tern-cv/f69d8f1e-d83f-49b1-be06-6553b04914fc": "Soil Metagenomic Sample",
    "http://linked.data.gov.au/def/tern-cv/a49f0fcd-19f5-4098-ac2e-85a972286a43": "Soil Pit Sample",
    "http://linked.data.gov.au/def/tern-cv/f51d6ffa-6108-4aab-b63b-b41fc7748da6": "Soil Subsite Sample",
}
