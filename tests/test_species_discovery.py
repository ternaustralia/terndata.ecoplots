"""Species facets and selectable search suggestions in both modes."""

import copy
from types import SimpleNamespace

import pytest
import requests

from terndata.ecoplots import AsyncEcoPlots, EcoPlots


@pytest.mark.parametrize("client_type", [EcoPlots, AsyncEcoPlots])
@pytest.mark.parametrize("mode", ["observations", "samples"])
@pytest.mark.parametrize("kind", ["species_name", "accepted_name"])
def test_species_discovery_and_search_selection(client_type, mode, kind, monkeypatch):
    calls = []
    species = "Acacia aneura"

    def post(url, **kwargs):
        calls.append((url, copy.deepcopy(kwargs)))
        if url.endswith("/data/summary"):
            return SimpleNamespace(raise_for_status=lambda: None, content=b'{"total_doc": 5}')
        if url.endswith("/ui/search"):
            data = {"suggestions": [{"type": kind, "label": species, "value": species}]}
        else:
            data = {
                "aggregations": {
                    "speciesname": {
                        "value": {
                            "buckets": [
                                {"key": species, "doc_count": 5},
                            ]
                        }
                    }
                }
            }
        return SimpleNamespace(raise_for_status=lambda: None, json=lambda: data)

    monkeypatch.setattr("terndata.ecoplots._base.requests.post", post)
    ec = client_type(mode)
    if mode == "samples":
        ec._query_filters["has_image"] = True
    before = copy.deepcopy(ec._query_filters)
    rows = ec.get_speciesname()
    assert rows.to_dict("records") == [{"speciesname": species, "count": 5}]
    endpoint = "/ui/facet/samples" if mode == "samples" else "/ui/facet"
    assert calls[0][0].endswith(endpoint)
    assert calls[0][1]["params"] == {"facet": "speciesname"}
    if mode == "samples":
        assert calls[0][1]["json"]["has_image"] is True
        assert "has_image" not in calls[0][1]["json"]["query"]
    assert ec._query_filters == before
    assert ec.discover("speciesname").equals(rows)
    suggestions = ec.search("Acacia")
    assert suggestions.iloc[0]["selectable"]
    assert suggestions.iloc[0]["filters"] == {"speciesname": species}
    # Retain real species validation; mock only the samples' persistent dataset vocabulary.
    monkeypatch.setattr(
        "terndata.ecoplots._base.validate_facet",
        lambda facet, values: (facet, values, values, [], []),
    )
    ec.select(suggestions.iloc[0]["filters"])
    assert ec.get_api_query_filters("speciesname") == [species]
    assert "get_speciesname(" in ec.help()["method"].str.cat(sep="\n")


@pytest.mark.parametrize("mode", ["observations", "samples"])
def test_species_discovery_empty_results_and_http_errors(mode, monkeypatch):
    monkeypatch.setattr(
        "terndata.ecoplots._base.requests.post",
        lambda *a, **k: SimpleNamespace(
            raise_for_status=lambda: None, json=lambda: {"aggregations": {}}
        ),
    )
    result = EcoPlots(mode).get_speciesname()
    assert result.empty
    assert list(result.columns) == ["speciesname", "count"]

    def fail():
        raise requests.HTTPError("unavailable")

    monkeypatch.setattr(
        "terndata.ecoplots._base.requests.post",
        lambda *a, **k: SimpleNamespace(raise_for_status=fail),
    )
    with pytest.raises(requests.HTTPError):
        EcoPlots(mode).get_speciesname()
