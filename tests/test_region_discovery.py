"""Regression checks for region discovery and selected region types."""

import copy
from types import SimpleNamespace

import orjson
import pytest

from terndata.ecoplots import EcoPlots, EcoPlotsError
from terndata.ecoplots._nlp_utils import REGION_TYPES_MAP


@pytest.fixture
def region_requests(monkeypatch):
    sent = []

    def post(url, **kwargs):
        sent.append((url, kwargs))
        data = (
            {"aggregations": {"region": {"buckets": []}}}
            if url.endswith("/ui/facet/samples")
            else [{"key": "Test region", "uri": "https://example.test/region"}]
        )
        return SimpleNamespace(
            raise_for_status=lambda: None,
            content=orjson.dumps(data),
            text=orjson.dumps(data).decode(),
        )

    monkeypatch.setattr("terndata.ecoplots._base.requests.post", post)
    monkeypatch.setattr(
        "terndata.ecoplots._base.validate_facet",
        lambda facet, values: (
            facet,
            [uri for uri, label in REGION_TYPES_MAP.items() if label in values],
            values,
            [],
            [],
        ),
    )
    return sent


@pytest.mark.parametrize(
    "value, slug",
    [
        ("IBRA7 Bioregions", "ibra7-bioregions"),
        ("ibra7-bioregions", "ibra7-bioregions"),
        ("bioregions", "ibra7-bioregions"),
        ("https://linked.data.gov.au/dataset/ibra7", "ibra7-bioregions"),
        ("IBRA7 Subregions", "ibra7-subregions"),
        ("ibra7-subregions", "ibra7-subregions"),
        ("subregions", "ibra7-subregions"),
        ("https://linked.data.gov.au/dataset/ibra7/subregions", "ibra7-subregions"),
        ("States and Territories", "states-and-territories"),
    ],
)
def test_region_discovery_preserves_api_slug(value, slug, region_requests):
    frame = EcoPlots().get_regions(value)

    url, request = region_requests[0]
    assert url.endswith("/api/v1.0/discovery/region")
    assert request["params"] == [("region_type", slug)]
    assert frame.iloc[0]["key"] == "Test region"


@pytest.mark.parametrize("mode", ["observations", "samples"])
@pytest.mark.parametrize("dispatch", [False, True])
@pytest.mark.parametrize("label", ["IBRA7 Bioregions", "IBRA7 Subregions"])
def test_region_discovery_uses_selected_type(mode, dispatch, label, region_requests):
    uri = next(uri for uri, name in REGION_TYPES_MAP.items() if name == label)
    ec = EcoPlots(
        mode=mode,
        filterset={"region_type": [label]},
        query_filters={"region_type": [uri]},
    )
    before = copy.deepcopy((ec._filters, ec._query_filters))

    ec.discover("regions") if dispatch else ec.get_regions()

    request = region_requests[0][1]
    if mode == "observations":
        assert request["params"] == [("region_type", label.lower().replace(" ", "-"))]
    assert request["json"]["query"]["region_type"] == [uri]
    assert (ec._filters, ec._query_filters) == before


def test_region_discovery_can_use_selected_api_uri(region_requests):
    ec = EcoPlots(query_filters={"region_type": ["https://linked.data.gov.au/dataset/ibra7"]})
    ec.get_regions()
    assert region_requests[0][1]["params"] == [("region_type", "ibra7-bioregions")]


@pytest.mark.parametrize("mode", ["observations", "samples"])
def test_missing_or_ambiguous_region_type_requires_explicit_argument(mode, region_requests):
    with pytest.raises(EcoPlotsError, match="region_type is required"):
        EcoPlots(mode=mode).get_regions()
    ec = EcoPlots(mode=mode, filterset={"region_type": ["IBRA7 Bioregions", "IBRA7 Subregions"]})
    with pytest.raises(EcoPlotsError, match="Multiple region types"):
        ec.discover("region")
    assert region_requests == []

    ec.get_regions("IBRA7 Subregions")
    assert len(region_requests) == 1
