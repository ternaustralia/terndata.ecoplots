"""Regression checks for optional notebook map interactions."""

from types import SimpleNamespace

import pytest


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
