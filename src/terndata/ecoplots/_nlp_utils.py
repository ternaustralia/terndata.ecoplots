import asyncio
import re
import warnings
from typing import Optional

from rapidfuzz import fuzz, process

from ._utils import _get_cached_labels

ALL_FACETS = ["region_type", "region", "dataset", "feature_type", "observed_property"]

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


def resolve_facet(user_input: str, allowed_facets: list, threshold: int = 70) -> Optional[str]:
    """Resolve user input to the closest allowed facet using fuzzy matching.

    Args:
        user_input: The input string from the user to be matched against allowed facets.
        allowed_facets: A list of valid facet names that the user input can be matched to.
        threshold: The minimum similarity score (0-100) required for a match.
            Defaults to 70.

    Returns:
        The matched facet name if found, else None.

    Notes:
        - Intended for internal use only.
    """
    # Normalize user input
    cleaned_input = user_input.strip().replace(" ", "_").lower()
    result = process.extractOne(cleaned_input, allowed_facets, scorer=fuzz.QRatio)  # type: ignore
    if result is None:
        return None
    match, score, _ = result
    if score >= threshold:
        return match
    return None


def resolve_region_type(
    user_input: str, allowed_region_types: list = REGION_TYPES, threshold: int = 90
) -> Optional[str]:
    """Resolve a user-provided region type or URL to the closest allowed region type.

    Args:
        user_input: The user input, which can be a region type label or a URL.
        allowed_region_types: List of allowed region type
            labels to match against. Defaults to REGION_TYPES.
        threshold: Minimum fuzzy match score required to
            consider a match valid. Defaults to 90.

    Returns:
        The resolved region type label if a match is found; otherwise, None.

    Raises:
        ValueError: If the input cannot be resolved to a known region type or URL,
            or if no close matches are found.

    Notes:
        - Intended for internal use only.
    """
    # --- Case 1: Input is a URL ---
    if user_input.startswith("http://") or user_input.startswith("https://"):
        if user_input in REGION_URLS:
            # Perfect match
            return next(k for k, v in REGION_TYPES_MAP.items() if v == user_input)

        # Fuzzy match against known URLs
        result = process.extractOne(user_input, REGION_URLS, scorer=fuzz.QRatio)  # type: ignore
        if result is None:
            pass  # fall through to next
        else:
            best_url, score, _ = result
            if score >= threshold:
                # Auto-correct but warn user
                warnings.warn(
                    f"Input URL '{user_input}' corrected to '{best_url}'.",
                    UserWarning,
                    stacklevel=3,
                )
                return next(k for k, v in REGION_TYPES_MAP.items() if v == best_url)
        # Suggest but don't auto-correct
        result = process.extractOne(user_input, REGION_URLS, scorer=fuzz.QRatio)  # type: ignore
        if result is None:
            raise ValueError(
                f"Unrecognized URL '{user_input}'. "
                f"No close matches found. Allowed URLs: {', '.join(REGION_URLS)}."
            )
        best_url, score, _ = result
        if score >= 20:
            raise ValueError(f"Unrecognized URL '{user_input}'. " f"Did you mean '{best_url}'?")
        raise ValueError(
            f"Unrecognized URL '{user_input}'. "
            f"No close matches found. Allowed URLs: {', '.join(REGION_URLS)}."
        )

    # --- Case 2: Input is label (not URL) ---
    # Normalize user input
    user_input = re.sub(
        r"\bibra7[-_ ]*", "", user_input, flags=re.IGNORECASE
    )  # Remove 'ibra7' prefix if present
    cleaned_input = user_input.strip().replace(" ", "-").lower()
    if cleaned_input.startswith("l"):
        threshold = 20
    result = process.extractOne(cleaned_input, allowed_region_types, scorer=fuzz.QRatio)  # type: ignore
    if result is not None:
        match, score, _ = result
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


def resolve_single_input(candidate: str, labels_dict: dict[str, str], threshold: int = 80) -> str:
    """Resolve a single input (name or URL) to the canonical URL using RapidFuzz.

    Args:
        candidate: The user input to resolve.
        labels_dict: A dictionary mapping canonical URLs to user-friendly names.
        threshold: The minimum similarity score for a match to be considered valid.

    Raises:
        ValueError: If the input cannot be resolved to a known label or URL.

    Returns:
        The resolved canonical URL if a match is found; otherwise, raises a ValueError.

    Notes:
        - Intended for internal use only.
    """
    uris = list(labels_dict.keys())
    names = list(labels_dict.values())

    # --- Case 1: Input is already a URL ---
    if candidate.startswith(("http://", "https://")):
        if candidate in labels_dict:
            return candidate

        # Fuzzy URL correction
        result = process.extractOne(candidate, uris, scorer=fuzz.QRatio)  # type: ignore
        if result is not None:
            best_uri, score, _ = result
            if score >= threshold:
                warnings.warn(
                    f"Input URL '{candidate}' corrected to '{best_uri}').",
                    UserWarning,
                    stacklevel=3,
                )
                return best_uri

        raise ValueError(f"Unrecognized URL '{candidate}'.")

    # --- Case 2: Input is a name ---
    if candidate in names:
        for uri, label in labels_dict.items():
            if label == candidate:
                return uri

    # Fuzzy match against names
    result = process.extractOne(candidate, names, scorer=fuzz.QRatio)
    if result is not None:
        best_name, score, _ = result
        if score >= threshold:
            if score < 100:
                warnings.warn(
                    f"Input '{candidate}' resolved to '{best_name}'.",
                    UserWarning,
                    stacklevel=3,
                )
            # Best URI for the resolved name
            return next(uri for uri, lbl in labels_dict.items() if lbl == best_name)

    raise ValueError(f"Unrecognized value '{candidate}'.")


async def resolve_facet_inputs(
    facet: str, user_values: list[str], region_type: Optional[str] = None
) -> list[str]:
    """Resolve all user inputs for a facet to canonical URLs.

    Runs sequentially but can be scheduled concurrently per facet.

    Args:
        facet: The facet to resolve.
        user_values: The user input values to resolve.
        region_type: The region type to use for resolution (if applicable).

    Raises:
        ValueError: If region facet is used without region_type.

    Returns:
        A list of resolved canonical URLs.

    Notes:
        - If a user value cannot be resolved, a warning will be issued.
    """
    # Special handling for region facet
    if facet == "region" and not region_type:
        raise ValueError("Filtering by 'region' requires 'region_type' to be provided.")

    labels_dict = _get_cached_labels(facet)
    loop = asyncio.get_event_loop()

    tasks = [
        loop.run_in_executor(None, resolve_single_input, val, labels_dict) for val in user_values
    ]
    return await asyncio.gather(*tasks)


def resolve_filter_values_to_urls(
    facet: str, user_values: list, labels_dict: dict, threshold: int = 75
):
    """Validate and resolve a list of filter values (labels or URLs) for a facet.

    Args:
        facet: The facet name (e.g., "region", "dataset").
        user_values: The user input values to resolve.
        labels_dict: A dictionary mapping URLs to user-friendly names.
        threshold: The minimum similarity score for a match to be considered valid.

    Returns:
        A list of canonical URLs; raises warning for fuzzy, error for not found.

    Notes:
        - If a user value cannot be resolved, a warning will be issued.
        - Intended for internal use only.
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
        result = process.extractOne(candidate, names, scorer=fuzz.QRatio)  # type: ignore
        if result is not None:
            best_name, score, _ = result
            if score >= threshold:
                best_uri = next(u for u, n in labels_dict.items() if n == best_name)
                warnings.warn(
                    f"Value '{candidate}' for facet '{facet}' corrected to '{best_name}'.",
                    UserWarning,
                    stacklevel=3,
                )
                resolved.add(best_uri)
                matched.add(best_name)
                corrected.add(candidate)
            else:
                unmatched.add(candidate)
        else:
            unmatched.add(candidate)

    return list(resolved), list(matched), list(unmatched), list(corrected)


def validate_facet(facet, value) -> tuple:
    """Validate and resolve user-provided values for a given facet to their corresponding URLs.

    Args:
        facet: The name of the facet to validate (e.g., a field or category).
        value (Any): The value(s) to validate. Can be a single value or a list/tuple of values.

    Returns:
        tuple: A tuple containing:
            - facet (str): The facet name.
            - urls (list): List of resolved URLs corresponding to the provided values.
            - matched (list): List of values that were successfully matched.
            - unmatched (list): List of values that could not be matched.
            - corrected (list): List of corrected values, if any corrections were applied.

    Raises:
        None

    """
    labels_dict = _get_cached_labels(facet)
    user_values = list(value) if isinstance(value, (list, tuple)) else [value]
    urls, matched, unmatched, corrected = resolve_filter_values_to_urls(
        facet, user_values, labels_dict
    )
    return facet, urls, matched, unmatched, corrected
