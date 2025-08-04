import re
import warnings
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

REGION_TYPES_MAP = {
    "subregions": "https://linked.data.gov.au/dataset/ibra7/subregions",
    "bioregions": "https://linked.data.gov.au/dataset/ibra7",
    "nrm-regions": "https://linked.data.gov.au/dataset/ausnrm2023",
    "states-and-territories": "https://linked.data.gov.au/dataset/asgsed3/STE",
    "local-government-areas": "https://linked.data.gov.au/dataset/asgsed3/LGA2023",
    "wwf-ecoregions": "https://linked.data.gov.au/dataset/wwf2011",
    "terrestrial-capad-regions": "https://linked.data.gov.au/dataset/auscapad2022",
}

REGION_TYPES = list(REGION_TYPES_MAP.keys())
REGION_URLS = list(REGION_TYPES_MAP.values())

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
    # --- Case 1: Input is a URL ---
    if user_input.startswith("http://") or user_input.startswith("https://"):
        if user_input in REGION_URLS:
            # Perfect match
            return next(k for k, v in REGION_TYPES_MAP.items() if v == user_input)
        
        # Fuzzy match against known URLs
        best_url, score, _ = process.extractOne(user_input, REGION_URLS, scorer=fuzz.QRatio)
        print(score)
        if score >= 75:
            # Auto-correct but warn user
            canonical_type = next(k for k, v in REGION_TYPES_MAP.items() if v == best_url)
            warnings.warn(
                f"Input URL '{user_input}' corrected to '{best_url}'.",
                UserWarning
            )
            return canonical_type
        else:
            # Suggest but don't auto-correct
            best_url, score, _ = process.extractOne(user_input, REGION_URLS, scorer=fuzz.QRatio)
            if score >= 20:
                raise ValueError(
                    f"Unrecognized URL '{user_input}'. "
                    f"Did you mean '{best_url}'?"
                )
            raise ValueError(
                f"Unrecognized URL '{user_input}'. "
                f"No close matches found. Allowed URLs: {', '.join(REGION_URLS)}."
            )

    # --- Case 2: Input is label (not URL) ---
    # Normalize user input
    user_input = re.sub(r"\bibra7[-_ ]*", "", user_input, flags=re.IGNORECASE) # Remove 'ibra7' prefix if present
    cleaned_input = user_input.strip().replace(" ", "-").lower()
    if cleaned_input.startswith("l"):
        threshold = 20
    match, score, _ = process.extractOne(cleaned_input, allowed_region_types, scorer=fuzz.QRatio)
    if match:    
        if score >= threshold:
            return match
            
        raise ValueError(
            f"Invalid region type: '{user_input}'. "
            f"Did you mean '{match}'?\n"
            f"Allowed types: {', '.join(allowed_region_types)}."
        )
    
    # No matches at all (very rare)
    raise ValueError(
        f"Invalid region type: '{user_input}'. "
        f"No close matches found. Allowed types: {', '.join(allowed_region_types)}."
    )

