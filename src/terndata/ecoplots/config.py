import os
import tempfile
# Default configuration

# API_BASE_URL = "https://ecoplots-test.tern.org.au"
API_BASE_URL = "http://localhost:8000"

ELASTICSEARCH_URL="https://es-test.tern.org.au"
ELASTICSEARCH_INDEX_SITEVISIT = "plotdata_ecoplots-sites"
ELASTICSEARCH_INDEX_DATA = "plotdata_ecoplots-data"
ELASTICSEARCH_INDEX_LABELS = "plotdata_ecoplots-labels"
CACHE_EXPIRE_SECONDS = 60 * 60 * 24 * 14  # Default cache expiration time: 14 days
CACHE_DIR = os.path.join(tempfile.gettempdir(), "ecoplots_labels_cache")

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
