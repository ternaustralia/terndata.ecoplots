from copy import deepcopy
from types import SimpleNamespace
from unittest.mock import Mock

import orjson
import pytest
from diskcache import Cache

from terndata.ecoplots import AsyncEcoPlots, EcoPlots, EcoPlotsError
from terndata.ecoplots import _utils as utils
from terndata.ecoplots._config import EQUIPMENT_FACETS

LABELS = {
    "platform_type": {"urn:platform-type:tower": "Fixed platform"},
    "platform_id": {"urn:platform:a": "Tower A", "urn:platform:b": "Tower B"},
    "system_type": {"urn:system-type:camera": "Camera"},
    "deployment_id": {"urn:deployment:a": "Deployment A", "urn:deployment:b": "Deployment B"},
    "system_id": {"urn:system:a": "Instrument A"},
}


@pytest.fixture(params=[EcoPlots, AsyncEcoPlots], ids=["sync", "async"])
def equipment_client(request, tmp_path, monkeypatch):
    directory = str(tmp_path / "labels")
    monkeypatch.setattr(utils, "CACHE_DIR", directory)
    with Cache(directory) as cache:
        for facet, labels in LABELS.items():
            cache[facet] = labels
    monkeypatch.setattr(EcoPlots, "summarise_data", Mock(return_value={"total_doc": 1}))
    return request.param()


def test_flat_and_grouped_selections_produce_identical_nested_api_payloads(equipment_client, data_api):
    client = equipment_client
    client.select(platform_type="Fixed platform", platform_instance="Tower A",
                  platform_deployment="Deployment A", instrument_type="Camera",
                  instrument_deployment="Deployment B")
    expected = {
        "platform": {"type": ["urn:platform-type:tower"], "instance": ["urn:platform:a"],
                     "deployment": ["urn:deployment:a"]},
        "instrument": {"type": ["urn:system-type:camera"], "deployment": ["urn:deployment:b"]},
    }
    assert client.get_api_query_filters() == expected
    grouped = type(client)().select(
        platform={"type": "Fixed platform", "instance": "Tower A", "deployment": "Deployment A"},
        instrument={"type": "Camera", "deployment": "Deployment B"},
    )
    assert grouped.get_filter() == client.get_filter()
    assert grouped.get_api_query_filters() == expected
    client.submit_data_job(dformat="geojson")
    assert data_api.requests[-1][2]["json"] == {"query": expected}


def test_mixed_forms_merge_and_child_removal_preserves_siblings(equipment_client):
    client = equipment_client
    filters = {"platform": {"type": "Fixed platform", "instance": "Tower A"}}
    original = deepcopy(filters)
    client.select(filters=filters, platform_type="Fixed platform", platform_instance="Tower B")
    client.select(instrument={"type": "Camera"})
    assert filters == original
    assert client.get_filter("platform_type") == ["Fixed platform"]
    assert set(client.get_filter("platform_instance")) == {"Tower A", "Tower B"}
    assert client.get_api_query_filters("platform_type") == ["urn:platform-type:tower"]
    assert client.get_filter("platform")["type"] == ["Fixed platform"]
    assert "platform_type" in client
    assert "platform" in client
    assert client["instrument_type"] == ["Camera"]
    client.remove(platform={"type": None})
    assert "type" not in client.get_api_query_filters("platform")
    assert client.get_api_query_filters("platform_instance") == ["urn:platform:a", "urn:platform:b"]
    client.remove(platform_instance="Tower A")
    assert client.get_api_query_filters("platform_instance") == ["urn:platform:b"]
    client.remove(platform=None)
    assert client.get_api_query_filters() == {"instrument": {"type": ["urn:system-type:camera"]}}


def test_equipment_supports_uri_inputs_project_round_trip_and_accessors(equipment_client, tmp_path):
    client = equipment_client.select(platform_type="urn:platform-type:tower",
                                     instrument_type="urn:system-type:camera")
    restored = type(client).load(client.save(tmp_path / "equipment"))
    assert restored.get_filter() == client.get_filter()
    assert restored.get_api_query_filters() == client.get_api_query_filters()
    del restored["platform_type"]
    assert restored.get_api_query_filters() == {"instrument": {"type": ["urn:system-type:camera"]}}
    restored["platform_instance"] = "Tower A"
    assert restored.get_api_query_filters("platform_instance") == ["urn:platform:a"]


@pytest.mark.parametrize("facet", EQUIPMENT_FACETS)
def test_subfacet_discovery_methods_and_dispatch_use_public_api_names(equipment_client, monkeypatch, facet):
    client = equipment_client.select(platform_type="Fixed platform", instrument_type="Camera")
    query = deepcopy(client.get_api_query_filters())
    rows = [{"key": "Available value", "uri": "urn:available", "doc_count": 4}]
    calls = []

    def post(url, **kwargs):
        calls.append((url, kwargs))
        return SimpleNamespace(content=orjson.dumps(rows), raise_for_status=lambda: None)

    monkeypatch.setattr("terndata.ecoplots._base.requests.post", post)
    result = getattr(client, f"get_{facet}s")()
    assert result.to_dict(orient="records") == rows
    assert client.discover(facet).equals(result)
    assert client.discover(facet + "s").equals(result)
    assert all(url.endswith(f"/discovery/{facet}") for url, _ in calls)
    assert all(kwargs["json"] == {"query": query} for _, kwargs in calls)
    assert client.get_api_query_filters() == query


@pytest.mark.parametrize("facet", EQUIPMENT_FACETS)
def test_samples_reject_equipment_discovery_and_selection(equipment_client, facet):
    client = EcoPlots("samples")
    with pytest.raises(EcoPlotsError, match="Invalid filter keys"):
        client.select(**{facet: "value"})
    with pytest.raises(EcoPlotsError, match="observations mode only"):
        client.discover(facet)


def test_unsupported_instrument_children_are_ignored_and_legacy_ids_are_retained(equipment_client):
    client = equipment_client.select(instrument={"instance": "ignored", "type": "Camera"})
    client.select(system_id="Instrument A")
    assert client.get_api_query_filters() == {
        "instrument": {"type": ["urn:system-type:camera"]}, "system_id": ["urn:system:a"],
    }


def test_unmatched_equipment_value_keeps_existing_state(equipment_client):
    client = equipment_client.select(platform_type="Fixed platform")
    filters = deepcopy(client.get_filter())
    query = deepcopy(client.get_api_query_filters())
    with pytest.raises(EcoPlotsError):
        client.select(instrument_type="unmatched-xyz-123")
    assert client.get_filter() == filters
    assert client.get_api_query_filters() == query


def test_removing_equipment_uri_preserves_sibling_fields(equipment_client):
    client = equipment_client.select(platform={"type": "Fixed platform", "instance": "Tower A"})
    client.remove(platform={"instance": "urn:platform:a"})
    assert client.get_api_query_filters() == {"platform": {"type": ["urn:platform-type:tower"]}}


@pytest.mark.parametrize("selection", [42, {"invalid": "value"}, [42]])
def test_malformed_subfacet_values_fail_without_modifying_state(equipment_client, selection):
    client = equipment_client.select(instrument_type="Camera")
    original = deepcopy(client.get_filter())
    with pytest.raises(EcoPlotsError, match="must be a string or list of strings"):
        client.select(platform={"type": selection})
    assert client.get_filter() == original


@pytest.mark.parametrize("uri", ["https://example.test/platform/type/new", "urn:type:new"])
def test_discovery_uris_work_when_the_label_vocabulary_is_missing(equipment_client, uri):
    with Cache(utils.CACHE_DIR) as cache:
        del cache["platform_type"]
    client = equipment_client.select(platform_type=uri)
    assert client.get_filter("platform_type") == [uri]
    assert client.get_api_query_filters() == {"platform": {"type": [uri]}}
