import asyncio
import re
import warnings

from rapidfuzz import process, fuzz
from typing import Dict, List, Union

from .utils import _normalise_to_list, _get_cached_labels

ALL_FACETS = [
    "region_type",
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

def resolve_facet(user_input: str, allowed_facets: List, threshold: int =70):
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


def resolve_single_input(
    candidate: str, 
    labels_dict: Dict[str, str], 
    threshold: int = 80
) -> str:
    """
    Resolve a single input (name or URL) to the canonical URL using RapidFuzz.
    """
    uris = list(labels_dict.keys())
    names = list(labels_dict.values())

    # --- Case 1: Input is already a URL ---
    if candidate.startswith(("http://", "https://")):
        if candidate in labels_dict:
            return candidate
        
        # Fuzzy URL correction
        best_uri, score, _ = process.extractOne(candidate, uris, scorer=fuzz.QRatio)
        if score >= threshold:
            warnings.warn(
                f"Input URL '{candidate}' corrected to '{best_uri}' (score={score}).",
                UserWarning
            )
            return best_uri
        else:
            raise ValueError(
                f"Unrecognized URL '{candidate}'. Closest match: '{best_uri}' (score={score})"
            )

    # --- Case 2: Input is a name ---
    if candidate in names:
        for uri, label in labels_dict.items():
            if label == candidate:
                return uri

    # Fuzzy match against names
    best_name, score, _ = process.extractOne(candidate, names, scorer=fuzz.QRatio)
    if score >= threshold:
        best_uri = next(uri for uri, lbl in labels_dict.items() if lbl == best_name)
        if score < 100:
            warnings.warn(
                f"Input '{candidate}' resolved to '{best_name}' (score={score})",
                UserWarning
            )
        return best_uri

    raise ValueError(
        f"Unrecognized label '{candidate}'. Did you mean '{best_name}'? (score={score})"
    )


async def resolve_facet_inputs(facet: str, user_values: List[str], region_type: str = None) -> List[str]:
    """
    Resolve all user inputs for a facet to canonical URLs.
    Runs sequentially but can be scheduled concurrently per facet.
    """
    # Special handling for region facet
    if facet == "region" and not region_type:
        raise ValueError("Filtering by 'region' requires 'region_type' to be provided.")

    labels_dict = _get_cached_labels(facet)
    loop = asyncio.get_event_loop()
    
    tasks = [
        loop.run_in_executor(None, resolve_single_input, val, labels_dict)
        for val in user_values
    ]
    return await asyncio.gather(*tasks)


async def build_structured_query_async(user_filters: Dict[str, Union[str, List[str]]]) -> Dict[str, Dict[str, List[str]]]:
    """
    Convert user-friendly filters to a structured query with canonical URLs asynchronously.
    Parallelizes resolution per facet for speed.
    """
    structured = {"query": {}}
    region_type = user_filters.get("region_type")

    # Normalize filters and build async tasks
    facet_tasks = []
    for facet, value in user_filters.items():
        if not value:
            continue
        values_list = _normalise_to_list(value)
        facet_tasks.append(
            asyncio.create_task(resolve_facet_inputs(facet, values_list, region_type=region_type))
        )

    # Gather all results in order
    results = await asyncio.gather(*facet_tasks)

    # Assign back to facets
    for (facet, _), resolved in zip(user_filters.items(), results):
        if user_filters[facet]:  # skip empty
            structured["query"][facet] = resolved

    return structured


def resolve_filter_values_to_urls(
    facet: str,
    user_values: list,
    labels_dict: dict,
    threshold: int = 75
):
    """
    Validates and resolves a list of filter values (labels or URLs) for a facet.
    Returns: list of canonical URLs; raises warning for fuzzy, error for not found.
    """
    uris = list(labels_dict.keys())
    names = list(labels_dict.values())
    resolved = set()
    unmatched = set()
    matched = set()
    corrected = set()

    for candidate in user_values:
        candidate = candidate.strip()
        # URL match
        if candidate in uris:
            resolved.add(candidate)
            matched.add(labels_dict[candidate])
            continue
        # Exact label match
        if candidate in names:
            resolved.add(next(u for u, n in labels_dict.items() if n == candidate))
            matched.add(candidate)
            continue
        # Fuzzy label match
        best_name, score, _ = process.extractOne(candidate, names, scorer=fuzz.QRatio)
        print(f"Fuzzy match score for '{candidate}': {score}")
        if score >= threshold:
            best_uri = next(u for u, n in labels_dict.items() if n == best_name)
            warnings.warn(
                f"Value '{candidate}' for facet '{facet}' auto-corrected to '{best_name}' (score={score})",
                UserWarning
            )
            resolved.add(best_uri)
            matched.add(best_name)
            corrected.add(candidate)
        # No match found
        else:
            unmatched.add(candidate)

    return list(resolved), list(matched), list(unmatched), list(corrected)


def validate_facet(facet, value):
    labels_dict = _get_cached_labels(facet)
    user_values = value if isinstance(value, (list, tuple)) else [value]
    urls, matched, unmatched, corrected = resolve_filter_values_to_urls(
        facet, user_values, labels_dict
    )
    return facet, urls, matched, unmatched, corrected