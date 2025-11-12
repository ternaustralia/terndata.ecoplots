import os
import tempfile

# Default configuration

VERSION = "0.0.3-beta"

API_BASE_URL = "http://ecoplots-test.tern.org.au"

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
    # "description", #TODO: AI based description to facet mapping
]

DISCOVERY_FACETS = [
    "region",
    "dataset",
    "feature_type",
    "observed_property",
    "site_id",
]

DISCOVERY_ATTRIBUTES = [
    "dataset",
    "feature_type",
    "observation",
    "site",
    "site_visit",
]
