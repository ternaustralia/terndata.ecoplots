"""Translate API search suggestions into explicit library selections."""

import pandas as pd
import requests

from ._config import QUERY_FACETS, SAMPLE_QUERY_FACETS
from ._exceptions import EcoPlotsError

_COLUMNS = ["label", "match_type", "facet", "value", "filters", "selectable", "note"]
_ALIASES = {
    "parameter": "observed_property",
    "platform_id": "platform_instance",
    "system_type": "instrument_type",
    "deployment_id": "deployment",
    "species_name": "speciesname",
    "accepted_name": "speciesname",
}


def search_suggestions(base_url, mode, text, limit):
    """Request typed suggestions and expose mode-compatible selection dictionaries."""
    if not isinstance(text, str) or not text.strip() or len(text) > 256:
        raise EcoPlotsError("Search text must be a non-empty string of at most 256 characters.")
    if isinstance(limit, bool) or not isinstance(limit, int) or not 1 <= limit <= 99:
        raise EcoPlotsError("Search limit must be an integer between 1 and 99.")
    payload = {
        "text": text.strip(),
        "mode": "suggest",
        "context": mode,
        "limit": limit,
    }
    response = requests.post(f"{base_url}/api/v1.0/ui/search", json=payload, timeout=30)
    response.raise_for_status()
    data = response.json()
    allowed = SAMPLE_QUERY_FACETS if mode == "samples" else QUERY_FACETS
    rows = []
    for suggestion in data.get("suggestions", []):
        kind = suggestion.get("type", "")
        facet = _ALIASES.get(kind, kind)
        value = suggestion.get("value")
        note = ""
        if kind == "result_value":
            facet = "observed_property"
            value = suggestion.get("observed_property")
            note = (
                "Selects the observed property, including other result values."
                if value
                else "The matched result does not identify its observed property."
            )
        filters = {facet: value} if facet in allowed and isinstance(value, str) and value else {}
        if facet == "region" and filters:
            region_type = suggestion.get("region_type")
            if region_type:
                filters["region_type"] = region_type
            else:
                filters = {}
                note = "The suggestion does not identify its required region type."
        if not filters and not note:
            note = "This suggestion cannot be selected in the current mode."
        rows.append(
            {
                "label": suggestion.get("label", ""),
                "match_type": kind,
                "facet": facet,
                "value": value,
                "filters": filters,
                "selectable": bool(filters),
                "note": note,
            }
        )
    result = pd.DataFrame(rows, columns=_COLUMNS)
    result.attrs["intent"] = data.get("intent", {})
    return result
