"""Search suggestions, mode help and sample CSV transport contracts."""

import asyncio
import copy
from types import SimpleNamespace

import pytest
import requests
from diskcache import Cache

from terndata.ecoplots import AsyncEcoPlots, EcoPlots, EcoPlotsError
from terndata.ecoplots import _nlp_utils as nlp
from terndata.ecoplots import _utils as utils


@pytest.mark.parametrize("client_type", [EcoPlots, AsyncEcoPlots])
def test_search_context_and_suggestion_selection(client_type, monkeypatch, tmp_path):
    calls = []
    uri = "https://example.test/observed-property/biomass"

    def post(url, **kwargs):
        calls.append((url, kwargs))
        if url.endswith("/data/summary"):
            return SimpleNamespace(raise_for_status=lambda: None, content=b'{"total_doc": 1}')
        return SimpleNamespace(
            raise_for_status=lambda: None,
            json=lambda: {
                "suggestions": [
                    {"label": "Biomass", "type": "parameter", "value": uri},
                    {
                        "label": "Woody vine",
                        "type": "result_value",
                        "value": "Woody vine",
                        "observed_property": uri,
                    },
                    {"label": "Unknown", "type": "other", "value": "x"},
                ]
            },
        )

    monkeypatch.setattr("terndata.ecoplots._search.requests.post", post)
    monkeypatch.setattr(utils, "CACHE_DIR", str(tmp_path / "cache"))
    with Cache(utils.CACHE_DIR):
        pass
    ec = client_type(query_filters={"site_id": ["https://example.test/site"]})
    before = copy.deepcopy(ec._query_filters)
    rows = ec.search("biomass")
    assert ec._query_filters == before
    assert calls[0][0].endswith("/api/v1.0/ui/search")
    assert "query_context" not in calls[0][1]["json"]
    assert "query" not in calls[0][1]["json"]
    assert calls[0][1]["json"]["context"] == "observations"
    assert rows.iloc[0]["filters"] == {"observed_property": uri}
    assert rows.iloc[1]["filters"] == {"observed_property": uri}
    assert "other result values" in rows.iloc[1]["note"]
    assert not rows.iloc[2]["selectable"]
    # Exercise real vocabulary resolution with an empty local cache.
    assert nlp.validate_facet("observed_property", uri)[1] == [uri]
    ec.select(rows.iloc[0]["filters"])
    assert ec._query_filters["observed_property"] == [uri]
    ec.search("biomass")
    assert "query" not in calls[-1][1]["json"]


@pytest.mark.parametrize("mode", ["observations", "samples"])
def test_search_automatic_mode_and_empty_results(mode, monkeypatch):
    bodies = []

    def post(url, **kwargs):
        bodies.append(kwargs["json"])
        return SimpleNamespace(raise_for_status=lambda: None, json=lambda: {"suggestions": []})

    monkeypatch.setattr("terndata.ecoplots._search.requests.post", post)
    result = EcoPlots(mode).search("test")
    assert result.empty
    assert "filters" in result.columns
    assert bodies[0]["context"] == mode
    assert bodies[0]["mode"] == "suggest"
    assert "query_context" not in bodies[0]
    assert "query" not in bodies[0]


@pytest.mark.parametrize(
    "kwargs",
    [
        {"text": ""},
        {"text": "x" * 257},
        {"text": "x", "limit": 100},
        {"text": "x", "limit": True},
    ],
)
def test_invalid_search_input_does_not_call_api(kwargs, monkeypatch):
    def post(*args, **kwargs):
        pytest.fail("Invalid input must not reach the API")

    monkeypatch.setattr("terndata.ecoplots._search.requests.post", post)
    with pytest.raises(EcoPlotsError):
        EcoPlots().search(**kwargs)


def test_search_http_error_is_propagated(monkeypatch):
    def fail():
        raise requests.HTTPError("unavailable")

    monkeypatch.setattr(
        "terndata.ecoplots._search.requests.post",
        lambda *a, **k: SimpleNamespace(raise_for_status=fail),
    )
    with pytest.raises(requests.HTTPError):
        EcoPlots().search("test")


@pytest.mark.parametrize("client_type", [EcoPlots, AsyncEcoPlots])
@pytest.mark.parametrize("mode", ["observations", "samples"])
def test_help_lists_mode_specific_methods_without_network(client_type, mode):
    frame = client_type(mode).help()
    methods = frame["method"].str.cat(sep="\n")
    assert "search(" in methods
    assert "select(" in methods
    assert "help(" in methods
    assert ("get_platform_types(" in methods) == (mode == "observations")
    assert ("view_sample_images(" in methods) == (mode == "samples")
    assert ("submit_data_job(" in methods) == (mode == "observations")
    assert ("await get_data(" in methods) == (client_type is AsyncEcoPlots)
    assert frame["description"].str.len().min() > 0


CSV = b'sample_id,latitude,longitude,sample_image_urls\nS1,-25,133,"[""https://example.test/image.jpg""]"\n'


@pytest.mark.parametrize("asynchronous", [False, True])
def test_samples_use_csv_endpoint_and_preserve_arrays_coordinates(asynchronous, monkeypatch):
    calls = []

    class Response:
        content = CSV

        def raise_for_status(self):
            pass

        async def read(self):
            return CSV

        async def __aenter__(self):
            return self

        async def __aexit__(self, *args):
            pass

    def post(url, **kwargs):
        calls.append((url, kwargs))
        return Response()

    class Session:
        def post(self, url, **kwargs):
            return post(url, **kwargs)

        async def __aenter__(self):
            return self

        async def __aexit__(self, *args):
            pass

    monkeypatch.setattr("terndata.ecoplots._base.requests.post", post)
    monkeypatch.setattr(
        "terndata.ecoplots._base._aiohttp",
        lambda: SimpleNamespace(ClientSession=Session, ClientTimeout=lambda **kwargs: kwargs),
    )
    client_type = AsyncEcoPlots if asynchronous else EcoPlots
    ec = client_type(
        "samples", query_filters={"material_sample_type": ["sample-type"], "has_image": True}
    )
    before = copy.deepcopy(ec._query_filters)
    frame = asyncio.run(ec.get_data()) if asynchronous else ec.get_data()
    assert calls[0][0].endswith("/api/v1.0/data/samples")
    assert calls[0][1]["params"] == {"dformat": "csv"}
    payload = calls[0][1]["json"]
    assert payload["has_image"] is True
    assert "has_image" not in payload["query"]
    assert "page_size" not in payload
    assert ec._query_filters == before
    assert frame.iloc[0]["sample_image_urls"] == ["https://example.test/image.jpg"]
    assert frame.geometry.iloc[0].x == 133
    assert frame.geometry.iloc[0].y == -25


def test_empty_sample_csv_preserves_headers_and_missing_sample_type_is_rejected():
    frame = EcoPlots._sample_export_frame(b"sample_id,latitude,longitude\n")
    assert frame.empty
    assert "sample_id" in frame.columns
    with pytest.raises(EcoPlotsError, match="Exactly one"):
        EcoPlots("samples").fetch_samples_data_sync()


@pytest.mark.parametrize(
    ("kind", "extra", "expected"),
    [
        ("project", {}, {"project": "uri"}),
        ("platform_id", {}, {"platform_instance": "uri"}),
        ("system_type", {}, {"instrument_type": "uri"}),
        ("region", {"region_type": "region-type"}, {"region": "uri", "region_type": "region-type"}),
        ("region", {}, {}),
        ("result_value", {}, {}),
    ],
)
def test_suggestion_mapping(kind, extra, expected, monkeypatch):
    suggestion = {"type": kind, "value": "uri", "label": "Label", **extra}
    monkeypatch.setattr(
        "terndata.ecoplots._search.requests.post",
        lambda *a, **k: SimpleNamespace(
            raise_for_status=lambda: None, json=lambda: {"suggestions": [suggestion]}
        ),
    )
    row = EcoPlots().search("test").iloc[0]
    assert row["filters"] == expected
    assert bool(row["selectable"]) == bool(expected)


@pytest.mark.parametrize(
    "query",
    [
        {"platform": {"type": ["platform-type"], "instance": ["platform-uri"]}},
        {"speciesname": ["Acacia aneura"]},
        {"spatial": [1, 2, 3, 4]},
    ],
)
def test_search_ignores_existing_selections(query, monkeypatch):
    sent = []

    def post(url, **kwargs):
        sent.append(kwargs["json"])
        return SimpleNamespace(raise_for_status=lambda: None, json=lambda: {})

    monkeypatch.setattr("terndata.ecoplots._search.requests.post", post)
    ec = EcoPlots(query_filters=copy.deepcopy(query))
    assert ec.search("Acacia").empty
    assert sent == [{"text": "Acacia", "mode": "suggest", "context": "observations", "limit": 10}]
    assert ec._query_filters == query
