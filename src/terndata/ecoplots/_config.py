import os
import tempfile

# Default configuration

VERSION = "0.0.4-beta"

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
    "project",
    # "description", #TODO: AI based description to facet mapping
]

SAMPLE_QUERY_FACETS = [
    "region_type",
    "region",
    "dataset",
    "site_id",
    "material_sample_type",
    "used_procedure",
    "spatial",
]

DISCOVERY_FACETS = [
    "region",
    "dataset",
    "feature_type",
    "observed_property",
    "site_id",
]

SAMPLE_DISCOVERY_FACETS = [
    "region",
    "dataset",
    "material_sample_type",
    "site_id",
    "used_procedure",
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
