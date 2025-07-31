import re
from rapidfuzz import process, fuzz

ALL_FACETS = [
    "region_type",
    "region",
    "dataset",
    "feature_type",
    "observed_property"
]

DISCOVERY_FACETS = [
    "region",
    "dataset",
    "feature_type",
    "observed_property"
]

REGION_TYPES = [
    "subregions",
    "bioregions",
    "nrm-regions",
    "states-and-territories",
    "local-government-areas",
    "wwf-ecoregions",
    "terrestrial-capad-regions"
]

def resolve_discovery_facet(user_input, allowed_facets=DISCOVERY_FACETS, threshold=70):
    """
    Resolves user input to the closest allowed facet using fuzzy matching.
    Returns the facet name if found, else None.
    """
    # Normalize user input
    cleaned_input = user_input.strip().replace(" ", "_").lower()
    match, score, _ = process.extractOne(cleaned_input, allowed_facets, scorer=fuzz.QRatio)
    if score >= threshold:
        return match
    return None


def resolve_region_type(user_input, allowed_region_types=REGION_TYPES, threshold=60):
    """
    Resolves user input to the closest allowed region type using fuzzy matching.
    Returns the region type if found, else None.
    """
    # Normalize user input
    user_input = re.sub(r"\bibra7[-_ ]*", "", user_input, flags=re.IGNORECASE) # Remove 'ibra7' prefix if present
    cleaned_input = user_input.strip().replace(" ", "-").lower()
    if cleaned_input.startswith("l"):
        threshold = 20
    match, score, _ = process.extractOne(cleaned_input, allowed_region_types, scorer=fuzz.QRatio)
    if score >= threshold:
        return match
    return None