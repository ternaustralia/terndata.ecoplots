from types import SimpleNamespace
from unittest.mock import Mock

import orjson
import pytest
from diskcache import Cache

from terndata.ecoplots import AsyncEcoPlots, EcoPlots, EcoPlotsError
from terndata.ecoplots import _utils as utils
from terndata.ecoplots import _vocab_cache as vocab
from terndata.ecoplots._config import VOCAB_FACETS

FACETS = [
    ("platform", "platform_id", "Platform A"),
    ("deployment", "deployment_id", "Deployment A"),
    ("instrument", "system_id", "Instrument A"),
]


@pytest.fixture
def labels_cache(tmp_path, monkeypatch):
    vocab.wait_for_vocabularies()
    directory = str(tmp_path / "labels")
    monkeypatch.setattr(utils, "CACHE_DIR", directory)
    with Cache(directory) as cache:
        for _, api_facet, label in FACETS:
            cache[api_facet] = {f"https://example.test/{api_facet}/a": label}
    summary = Mock(return_value={"total_doc": 1})
    monkeypatch.setattr(EcoPlots, "summarise_data", summary)
    return summary


@pytest.fixture(params=[EcoPlots, AsyncEcoPlots], ids=["sync", "async"])
def client(request, labels_cache):
    return request.param()


def test_select_maps_public_facets_to_api_fields(client, labels_cache, data_api):
    selected = {facet: label for facet, _, label in FACETS}
    expected_query = {
        api_facet: [f"https://example.test/{api_facet}/a"] for _, api_facet, _ in FACETS
    }

    assert client.select(**selected) is client
    assert client.get_filter() == {facet: [label] for facet, label in selected.items()}
    assert client.get_api_query_filters() == expected_query
    assert labels_cache.call_args.kwargs["query_filters"] == expected_query
    for facet, api_facet, label in FACETS:
        assert client.get_filter(facet) == [label]
        assert client.get_api_query_filters(facet) == expected_query[api_facet]
        assert facet in client
        assert client[facet] == [label]
        assert api_facet not in str(client)

    client.submit_data_job(dformat="geojson")
    assert data_api.requests[-1][2]["json"] == {"query": expected_query}


@pytest.mark.parametrize(("facet", "api_facet", "label"), FACETS)
def test_select_accepts_uris_and_aliases(client, facet, api_facet, label):
    uri = f"https://example.test/{api_facet}/a"
    plural = facet + "s"

    client.select(filters={api_facet: uri, plural: label})

    assert client.get_filter() == {facet: [label]}
    assert client.get_api_query_filters(api_facet) == [uri]
    assert client.get_api_query_filters(plural) == [uri]
    assert plural in client
    assert client[api_facet] == [label]

    client.remove(filters={plural: label})
    assert client.get_filter() == {}
    assert client.get_api_query_filters() == {}
    assert facet not in client

    client[plural] = uri
    del client[api_facet]
    assert client.get_filter() == {}
    assert client.get_api_query_filters() == {}


def test_removing_one_facet_preserves_other_api_fields(client):
    client.select(**{facet: label for facet, _, label in FACETS})

    client.remove(instrument=None)

    assert set(client.get_filter()) == {"platform", "deployment"}
    assert set(client.get_api_query_filters()) == {"platform_id", "deployment_id"}


@pytest.mark.parametrize(("facet", "api_facet", "label"), FACETS)
def test_discovery_uses_api_facet_and_current_query(client, monkeypatch, facet, api_facet, label):
    client.select(instrument="Instrument A")
    requests = []
    records = [{"key": label, "uri": f"https://example.test/{api_facet}/a", "doc_count": 3}]

    def post(url, **kwargs):
        requests.append((url, kwargs))
        return SimpleNamespace(content=orjson.dumps(records), raise_for_status=lambda: None)

    monkeypatch.setattr("terndata.ecoplots._base.requests.post", post)

    result = getattr(client, f"get_{facet}s")()
    assert result.to_dict(orient="records") == records
    for alias in (facet, facet + "s", api_facet, facet.title()):
        assert client.discover(alias).equals(result)
    discovery_facet = "platform_instance" if facet == "platform" else api_facet
    assert all(url.endswith(f"/discovery/{discovery_facet}") for url, _ in requests)
    assert all(
        kwargs["json"] == {"query": {"system_id": ["https://example.test/system_id/a"]}}
        for _, kwargs in requests
    )


def test_new_facets_survive_project_round_trip(client, tmp_path):
    client.select(**{facet: label for facet, _, label in FACETS})

    restored = type(client).load(client.save(tmp_path / "instruments"))

    assert restored.get_filter() == client.get_filter()
    assert restored.get_api_query_filters() == client.get_api_query_filters()


@pytest.mark.parametrize(("facet", "api_facet", "label"), FACETS)
def test_samples_reject_observation_facets(labels_cache, facet, api_facet, label):
    client = EcoPlots("samples")

    with pytest.raises(EcoPlotsError, match="Invalid filter keys"):
        client.select(**{facet: label})
    with pytest.raises(EcoPlotsError, match="observations mode only"):
        client.discover(facet)


def test_cache_warming_fetches_new_vocabularies_and_reuses_them(tmp_path, monkeypatch):
    vocab.wait_for_vocabularies()
    directory = str(tmp_path / "labels")
    monkeypatch.setattr(utils, "CACHE_DIR", directory)
    monkeypatch.setattr(vocab, "CACHE_DIR", directory)
    monkeypatch.setattr(vocab, "_version", lambda: "version")
    new_facets = {api_facet for _, api_facet, _ in FACETS}
    with Cache(directory) as cache:
        for facet in set(VOCAB_FACETS) - new_facets:
            cache[facet] = {"existing": "Existing label"}
    with Cache(directory) as cache:
        cache[vocab._METADATA] = {
            "facets": {facet: {"version": "version"} for facet in set(VOCAB_FACETS) - new_facets}
        }
    fetched = []

    def fetch(facet, version, previous, labels):
        fetched.append(facet)
        return {f"https://example.test/{facet}/a": f"{facet} label"}, {"version": version}

    monkeypatch.setattr(vocab, "_fetch_facet", fetch)

    labels = vocab.refresh_vocabularies()
    assert set(fetched) == new_facets
    for facet, api_facet, _ in FACETS:
        assert utils._get_cached_labels(facet) == labels[api_facet]
    assert vocab.refresh_vocabularies() == labels
    assert len(fetched) == 3
