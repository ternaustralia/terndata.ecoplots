"""Configuration constants for the EcoPlots library.

Defines the API base URL, cache settings, magic bytes for ``.ecoproj`` binary
project files, and the facet lists that control which filter keys are valid in
each operational mode (``"observations"`` / ``"samples"``).
"""
import os
import tempfile

# Default configuration

VERSION = "1.0.0"

API_BASE_URL = "https://ecoplots.tern.org.au"

CACHE_EXPIRE_SECONDS = 60 * 60 * 24 * 14  # Default cache expiration time: 14 days
CACHE_DIR = os.path.join(tempfile.gettempdir(), "ecoplots_labels_cache")

MAGIC = b"ECPJ"

VOCAB_FACETS = [
    "region_type",
    "region",
    "dataset",
    "site_id",
    "site_visit_id",
    "feature_type",
    "observed_property",
    "attributes",
    # "units",
    "used_procedure",
    # "system_type",
    "core_attributes",
]

QUERY_FACETS = [
    "region_type",
    "region",
    "dataset",
    "site_id",
    "site_visit_id",
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
    "region",
    "dataset",
    "feature_type",
    "observed_property",
    "site_id",
    "used_procedure",
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
