"""Regression checks for optional notebook map interactions."""

import copy
import importlib
from types import SimpleNamespace

import pytest

pytestmark = pytest.mark.filterwarnings(
    "ignore:.* is deprecated, use .* instead:DeprecationWarning:ipyleaflet.leaflet"
)


@pytest.fixture(autouse=True)
def map_bounds(monkeypatch):
    """Record view updates while exercising the standard map implementation."""
    pytest.importorskip("ipyleaflet")
    pytest.importorskip("ipywidgets")
    from terndata.ecoplots._gui._map_view import _set_map_bounds

    bounds = []

    def record_view(map_widget, value):
        bounds.append(value)
        _set_map_bounds(map_widget, value)

    for name in ("spatial_selector", "_regions"):
        module = importlib.import_module(f"terndata.ecoplots._gui.{name}")
        monkeypatch.setattr(module, "_set_map_bounds", record_view)
    return bounds  # noqa: R504 - the callback records subsequent view updates


@pytest.mark.filterwarnings(
    "ignore:.* is deprecated, use .* instead:DeprecationWarning:ipyleaflet.leaflet"
)
def test_site_hover_updates_only_the_selected_marker(monkeypatch):
    """Each site's callbacks must retain that site's marker across preview loops."""
    leaflet = pytest.importorskip("ipyleaflet")
    pytest.importorskip("ipywidgets")
    from terndata.ecoplots._gui import spatial_selector

    callbacks = {}

    def capture_hover(marker, callback):
        callbacks.setdefault(id(marker), {})["over"] = callback

    def capture_leave(marker, callback):
        callbacks.setdefault(id(marker), {})["out"] = callback

    monkeypatch.setattr(leaflet.CircleMarker, "on_mouseover", capture_hover, raising=False)
    monkeypatch.setattr(leaflet.CircleMarker, "on_mouseout", capture_leave, raising=False)
    client = SimpleNamespace(
        _fetch_clusters=lambda: {
            "sites": [
                {"site_id": "site/first", "latitude": -25, "longitude": 133},
                {"site_id": "site/second", "latitude": -26, "longitude": 134},
            ]
        }
    )
    widget = spatial_selector(client)
    map_widget = next(child for child in widget.children if isinstance(child, leaflet.Map))
    site_layer = next(layer for layer in map_widget.layers if isinstance(layer, leaflet.LayerGroup))
    first, second = site_layer.layers[-2:]

    callbacks[id(first)]["over"]()
    assert first.radius == 7
    assert first.fill_opacity == 1.0
    assert second.radius == 5
    assert second.fill_opacity == 0.85

    callbacks[id(first)]["out"]()
    assert first.radius == 5
    assert first.fill_opacity == 0.95
    assert second.fill_opacity == 0.85

    callbacks[id(second)]["over"]()
    assert first.radius == 5
    assert second.radius == 7


@pytest.fixture(params=["observations", "samples"])
def region_map(request, monkeypatch, tmp_path):
    import pandas as pd
    from diskcache import Cache

    from terndata.ecoplots import EcoPlots
    from terndata.ecoplots._gui import spatial_selector
    from terndata.ecoplots._nlp_utils import REGION_TYPES_MAP

    module = importlib.import_module("terndata.ecoplots._gui._regions")
    region_uri = "https://linked.data.gov.au/dataset/ibra7/ARC"
    subregion_uri = "https://linked.data.gov.au/dataset/ibra7/ARC01"
    labels = {
        "region_type": REGION_TYPES_MAP,
        "region": {region_uri: "Arnhem Coast", subregion_uri: "Maningrida"},
        "site_id": {"https://example.test/site": "Site"},
        "dataset": {"http://linked.data.gov.au/dataset/ausplots": "TERN Ecosystem Surveillance"},
    }
    cache_dir = str(tmp_path / "labels")
    monkeypatch.setattr("terndata.ecoplots._utils.CACHE_DIR", cache_dir)
    with Cache(cache_dir) as cache:
        for facet, values in labels.items():
            cache[facet] = values
    monkeypatch.setattr(EcoPlots, "summarise_data", lambda *args, **kwargs: {"total_doc": 1})
    previews = []
    discoveries = []
    wfs_requests = []

    def clusters(self, geometry=None):
        previews.append((self._mode, self._base_url, copy.deepcopy(self._query_filters), geometry))
        return {}

    def regions(self, region_type=None):
        discoveries.append(copy.deepcopy(self._query_filters))
        uri = subregion_uri if region_type.endswith("subregions") else region_uri
        return pd.DataFrame([{"key": labels["region"][uri], "uri": uri}])

    geometry = {
        "type": "MultiPolygon",
        "coordinates": [[[[130, -25], [132, -25], [132, -20], [130, -20], [130, -25]]]],
    }

    def wfs_get(url, **kwargs):
        wfs_requests.append((url, kwargs))
        return SimpleNamespace(
            raise_for_status=lambda: None,
            json=lambda: {
                "type": "FeatureCollection",
                "features": [{"type": "Feature", "properties": {}, "geometry": geometry}],
            },
        )

    monkeypatch.setattr(EcoPlots, "_fetch_clusters", clusters)
    monkeypatch.setattr(EcoPlots, "get_regions", regions)
    monkeypatch.setattr(module.requests, "get", wfs_get)
    client = EcoPlots(
        mode=request.param,
        filterset={"site_id": ["Site"]},
        query_filters={"site_id": ["https://example.test/site"]},
    )
    client._base_url = "https://example.test/custom-api"

    def build():
        widget = spatial_selector(client)
        controls = next(
            child
            for child in widget.children
            if hasattr(child, "children")
            and child.children
            and hasattr(child.children[0], "children")
        )
        region_type, region = controls.children[0].children
        leaflet = pytest.importorskip("ipyleaflet")
        map_widget = next(child for child in widget.children if isinstance(child, leaflet.Map))
        actions = next(
            child
            for child in widget.children
            if hasattr(child, "children")
            and child.children
            and getattr(child.children[0], "description", "") == "Confirm"
        )
        return widget, map_widget, region_type, region, actions

    return SimpleNamespace(
        client=client,
        build=build,
        previews=previews,
        discoveries=discoveries,
        wfs=wfs_requests,
        region_uri=region_uri,
        subregion_uri=subregion_uri,
    )


def test_region_controls_show_boundaries_shade_zoom_and_confirm(region_map, map_bounds):
    from ipyleaflet import GeoJSON, WMSLayer

    client = region_map.client
    before = copy.deepcopy((client._filters, client._query_filters))
    widget, map_widget, region_type, region, actions = region_map.build()
    region_type.value = "https://linked.data.gov.au/dataset/ibra7"
    assert not region.disabled
    assert any(
        isinstance(layer, WMSLayer) and layer.layers == "administrative_area:ibra7_region"
        for layer in map_widget.layers
    )
    region.value = region_map.region_uri
    assert any(
        isinstance(layer, GeoJSON)
        and layer.style["fillOpacity"] > 0
        and layer.style["pane"] == "ecop-regions"
        for layer in map_widget.layers
    )
    assert map_bounds[-1] == [[-25, 130], [-20, 132]]
    params = region_map.wfs[-1][1]["params"]
    assert params["typename"] == "administrative_area:ibra7_region"
    assert params["cql_filter"] == "loci_uri7='https://linked.data.gov.au/dataset/ibra7/ARC'"
    assert params["srsName"] == "EPSG:4326"
    assert (client._filters, client._query_filters) == before
    mode, origin, query, _ = region_map.previews[-1]
    assert mode == client._mode
    assert origin == client._base_url
    assert query["region"] == [region_map.region_uri]
    actions.children[0].click()
    assert client._query_filters["region"] == [region_map.region_uri]
    assert client._query_filters["site_id"] == ["https://example.test/site"]
    assert "Filters applied" in widget.children[-2].value


def test_changing_type_clears_old_region_and_clear_removes_layers(region_map, map_bounds):
    from ipyleaflet import GeoJSON, WMSLayer

    _, map_widget, region_type, region, actions = region_map.build()
    region_type.value = "https://linked.data.gov.au/dataset/ibra7"
    region.value = region_map.region_uri
    region_type.value = "https://linked.data.gov.au/dataset/ibra7/subregions"
    assert region.value is None
    assert "region" not in region_map.discoveries[-1]
    assert not any(isinstance(layer, GeoJSON) for layer in map_widget.layers)
    region.value = region_map.subregion_uri
    assert region_map.wfs[-1][1]["params"]["typename"] == "administrative_area:ibra7_subregion"
    actions.children[-1].click()
    assert region_type.value is None
    assert region.value is None
    assert region.disabled
    assert not any(isinstance(layer, (GeoJSON, WMSLayer)) for layer in map_widget.layers)
    assert map_widget.center == [-25, 133]
    assert map_widget.zoom == 4
    assert "region" not in region_map.client._filters


def test_existing_selection_is_restored_and_replaced_on_confirm(region_map):
    client = region_map.client
    client.select(region_type="IBRA7 Bioregions", region=region_map.region_uri)
    _, _, region_type, region, actions = region_map.build()
    assert region_type.value == "https://linked.data.gov.au/dataset/ibra7"
    assert region.value == region_map.region_uri
    region_type.value = "https://linked.data.gov.au/dataset/ibra7/subregions"
    region.value = region_map.subregion_uri
    actions.children[0].click()
    assert client._query_filters["region_type"] == [
        "https://linked.data.gov.au/dataset/ibra7/subregions"
    ]
    assert client._query_filters["region"] == [region_map.subregion_uri]


def test_region_confirm_rollback_preserves_previous_filters(region_map, monkeypatch):
    from terndata.ecoplots import EcoPlots

    client = region_map.client
    before = copy.deepcopy((client._filters, client._query_filters))
    widget, _, region_type, region, actions = region_map.build()
    region_type.value = "https://linked.data.gov.au/dataset/ibra7"
    region.value = region_map.region_uri
    monkeypatch.setattr(EcoPlots, "_validate_filters", lambda self: False)
    actions.children[0].click()
    assert (client._filters, client._query_filters) == before
    assert "Could not apply region filters" in widget.children[-2].value


def test_region_and_drawn_shape_are_applied_together(region_map):
    from ipyleaflet import DrawControl

    _, map_widget, region_type, region, actions = region_map.build()
    region_type.value = "https://linked.data.gov.au/dataset/ibra7"
    region.value = region_map.region_uri
    draw = next(control for control in map_widget.controls if isinstance(control, DrawControl))
    geometry = {
        "type": "Polygon",
        "coordinates": [[[130, -25], [132, -25], [132, -20], [130, -20], [130, -25]]],
    }
    draw._draw_callbacks(draw, "created", {"type": "Feature", "geometry": geometry})
    actions.children[0].click()
    assert region_map.client._query_filters["region"] == [region_map.region_uri]
    assert region_map.client._query_filters["spatial"].startswith("POLYGON")


def test_confirming_type_only_removes_previous_region(region_map):
    client = region_map.client
    client.select(region_type="IBRA7 Bioregions", region=region_map.region_uri)
    _, _, _, region, actions = region_map.build()
    region.value = None
    actions.children[0].click()
    assert client._query_filters["region_type"] == ["https://linked.data.gov.au/dataset/ibra7"]
    assert "region" not in client._query_filters
    assert "region" not in client._filters


@pytest.mark.parametrize("overlay", ["clusters", "sites"])
def test_data_overlays_stay_above_regions_after_changes_and_toggling(
    region_map, monkeypatch, overlay
):
    from ipyleaflet import CircleMarker, GeoJSON, Heatmap, LayerGroup, WMSLayer

    from terndata.ecoplots import EcoPlots

    data = (
        {"clusters": [{"location": {"lat": -23, "lon": 131}, "num_sites": 10}]}
        if overlay == "clusters"
        else {"sites": [{"site_id": "test-site", "latitude": -23, "longitude": 131}]}
    )
    monkeypatch.setattr(EcoPlots, "_fetch_clusters", lambda self, geometry=None: data)
    _, map_widget, region_type, region, actions = region_map.build()

    def check_layer_order():
        layers = list(map_widget.layers)
        for layer in map_widget.layers:
            if isinstance(layer, LayerGroup):
                layers.extend(layer.layers)
        data_layers = [layer for layer in layers if isinstance(layer, (Heatmap, CircleMarker))]
        region_layers = [layer for layer in layers if isinstance(layer, (WMSLayer, GeoJSON))]
        assert data_layers
        assert region_layers
        for layer in data_layers:
            data_z = map_widget.panes[layer.pane]["zIndex"]
            assert all(
                data_z > map_widget.panes[boundary.pane]["zIndex"] for boundary in region_layers
            )

    region_type.value = "https://linked.data.gov.au/dataset/ibra7"
    check_layer_order()
    region.value = region_map.region_uri
    check_layer_order()
    toggle = actions.children[1]
    toggle.value = False
    assert not any(
        isinstance(layer, (Heatmap, LayerGroup)) and not isinstance(layer, GeoJSON)
        for layer in map_widget.layers
    )
    toggle.value = True
    check_layer_order()
    region_type.value = "https://linked.data.gov.au/dataset/ibra7/subregions"
    region.value = region_map.subregion_uri
    check_layer_order()


def test_standard_map_view_fits_selection_and_resets(region_map, map_bounds):
    from ipyleaflet import DrawControl

    _, map_widget, region_type, region, actions = region_map.build()
    assert not hasattr(map_widget, "navigation")
    assert "max_bounds" not in map_widget.options
    assert map_widget.min_zoom == 1
    assert not map_widget.layers[0].bounds
    assert not map_widget.layers[0].no_wrap
    region_type.value = "https://linked.data.gov.au/dataset/ibra7"
    region.value = region_map.region_uri
    assert map_bounds[-1] == [[-25, 130], [-20, 132]]
    assert -25 <= map_widget.center[0] <= -20
    assert map_widget.center[1] == 131
    assert map_widget.zoom > 4

    draw = next(control for control in map_widget.controls if isinstance(control, DrawControl))
    geometry = {
        "type": "Polygon",
        "coordinates": [[[130, -24], [131, -24], [131, -23], [130, -23], [130, -24]]],
    }
    draw._draw_callbacks(draw, "created", {"type": "Feature", "geometry": geometry})
    assert map_bounds[-1] == [[-24, 130], [-23, 131]]
    actions.children[-1].click()
    assert map_widget.center == [-25, 133]
    assert map_widget.zoom == 4


def test_restore_fetches_only_final_selection_and_drawing_fetches_once(region_map):
    from ipyleaflet import DrawControl

    client = region_map.client
    client.select(region_type="IBRA7 Bioregions", region=region_map.region_uri)
    _, map_widget, _, _, _ = region_map.build()
    assert len(region_map.previews) == 1
    assert region_map.previews[0][2]["region"] == [region_map.region_uri]
    draw = next(control for control in map_widget.controls if isinstance(control, DrawControl))
    geometry = {
        "type": "Polygon",
        "coordinates": [[[130, -24], [131, -24], [131, -23], [130, -23], [130, -24]]],
    }
    event = {"type": "Feature", "geometry": geometry}
    draw._draw_callbacks(draw, "created", event)
    assert len(region_map.previews) == 2
    assert region_map.previews[-1][3] == geometry
    draw._draw_callbacks(draw, "created", event)
    assert len(region_map.previews) == 2


def test_repeat_region_selection_reuses_data_and_boundary_requests(region_map):
    _, _, region_type, region, actions = region_map.build()
    region_type.value = "https://linked.data.gov.au/dataset/ibra7"
    region.value = region_map.region_uri
    counts = (len(region_map.previews), len(region_map.wfs), len(region_map.discoveries))
    region.value = None
    region.value = region_map.region_uri
    actions.children[1].value = False
    actions.children[1].value = True
    assert (len(region_map.previews), len(region_map.wfs), len(region_map.discoveries)) == counts

    region_type.value = "https://linked.data.gov.au/dataset/ibra7/subregions"
    after_subregions = (len(region_map.previews), len(region_map.wfs), len(region_map.discoveries))
    region_type.value = "https://linked.data.gov.au/dataset/ibra7"
    region.value = region_map.region_uri
    assert (
        len(region_map.previews),
        len(region_map.wfs),
        len(region_map.discoveries),
    ) == after_subregions


def test_preview_cache_accounts_for_other_client_filters(region_map):
    _, _, region_type, region, _ = region_map.build()
    region_type.value = "https://linked.data.gov.au/dataset/ibra7"
    region.value = region_map.region_uri
    before = len(region_map.previews)
    region_map.client._query_filters["site_id"] = ["https://example.test/another-site"]
    region.value = None
    region.value = region_map.region_uri
    assert len(region_map.previews) == before + 2
    assert region_map.previews[-1][2]["site_id"] == ["https://example.test/another-site"]


def test_site_popups_are_created_only_when_clicked(region_map, monkeypatch):
    from ipyleaflet import LayerGroup, Popup

    from terndata.ecoplots import EcoPlots

    monkeypatch.setattr(
        EcoPlots,
        "_fetch_clusters",
        lambda self: {
            "sites": [
                {"site_id": "one", "latitude": -23, "longitude": 131},
                {"site_id": "two", "latitude": -22, "longitude": 132},
            ]
        },
    )
    _, map_widget, _, _, _ = region_map.build()
    layer = next(layer for layer in map_widget.layers if isinstance(layer, LayerGroup))
    assert len(layer.layers) == 2
    assert not any(isinstance(layer, Popup) for layer in map_widget.layers)
    layer.layers[0]._click_callbacks()
    assert sum(isinstance(layer, Popup) for layer in map_widget.layers) == 1
    layer.layers[1]._click_callbacks()
    assert sum(isinstance(layer, Popup) for layer in map_widget.layers) == 1


def test_map_honours_initial_view_outside_australia():
    from ipyleaflet import Map

    from terndata.ecoplots._gui import spatial_selector

    widget = spatial_selector(SimpleNamespace(_fetch_clusters=lambda: {}), center=(51, 0), zoom=2)
    map_widget = next(child for child in widget.children if isinstance(child, Map))
    assert map_widget.center == [51, 0]
    assert map_widget.zoom == 2


def test_display_boundary_is_lightweight_and_filter_stays_uri_based(region_map, monkeypatch):
    from ipyleaflet import GeoJSON
    from shapely.geometry import Point, mapping

    module = importlib.import_module("terndata.ecoplots._gui._regions")
    dense = Point(131, -23).buffer(1, quad_segs=1024)
    raw = {
        "type": "FeatureCollection",
        "features": [
            {"type": "Feature", "properties": {"unused": "metadata"}, "geometry": mapping(dense)}
        ],
    }
    monkeypatch.setattr(module, "_fetch_region", lambda label, uri: raw)
    _, map_widget, region_type, region, actions = region_map.build()
    region_type.value = "https://linked.data.gov.au/dataset/ibra7"
    region.value = region_map.region_uri
    highlight = next(layer for layer in map_widget.layers if isinstance(layer, GeoJSON))
    display = highlight.data["features"][0]["geometry"]["coordinates"][0]
    assert len(display) < len(dense.exterior.coords) / 10
    actions.children[0].click()
    assert region_map.client._query_filters["region"] == [region_map.region_uri]
    assert "spatial" not in region_map.client._query_filters


def test_bounds_zoom_uses_viewport_and_respects_map_zoom_levels():
    from ipyleaflet import Map

    from terndata.ecoplots._gui._map_view import _set_map_bounds

    map_widget = Map(min_zoom=1, max_zoom=18)
    map_widget.set_trait("pixel_bounds", ((100, 200), (1000, 760)))
    bounds = [[-25, 130], [-20, 132]]
    _set_map_bounds(map_widget, bounds)
    assert map_widget.zoom == 7
    map_widget.set_trait("pixel_bounds", ((100, 200), (500, 500)))
    _set_map_bounds(map_widget, bounds)
    assert map_widget.zoom == 6
    _set_map_bounds(map_widget, [[-85, -180], [85, 180]])
    assert map_widget.zoom == 1
    _set_map_bounds(map_widget, [[0, 0], [0.000001, 0.000001]])
    assert map_widget.zoom == 18


@pytest.mark.parametrize("failure", ["timeout", "empty", "invalid"])
def test_wfs_failure_leaves_map_usable(region_map, monkeypatch, failure):
    import requests
    from ipyleaflet import GeoJSON, WMSLayer

    module = importlib.import_module("terndata.ecoplots._gui._regions")
    widget, map_widget, region_type, region, actions = region_map.build()
    region_type.value = "https://linked.data.gov.au/dataset/ibra7"

    def fail(*args, **kwargs):
        if failure == "timeout":
            raise requests.Timeout("WFS unavailable")
        return SimpleNamespace(
            raise_for_status=lambda: None,
            json=lambda: (
                {"type": "FeatureCollection", "features": []} if failure == "empty" else {}
            ),
        )

    monkeypatch.setattr(module.requests, "get", fail)
    region.value = region_map.region_uri
    controls = next(
        child
        for child in widget.children
        if hasattr(child, "children") and child.children and hasattr(child.children[0], "children")
    )
    assert "Could not load region boundary" in controls.children[1].value
    assert not any(isinstance(layer, GeoJSON) for layer in map_widget.layers)
    assert any(isinstance(layer, WMSLayer) for layer in map_widget.layers)
    assert "region" not in region_map.client._filters
    actions.children[-1].click()
    assert region.disabled
