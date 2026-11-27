"""Region controls using the same GeoServer layers as the EcoPlots portal."""

from __future__ import annotations

import copy
from html import escape

import orjson
import requests
from ipyleaflet import GeoJSON, WMSLayer
from ipywidgets import HTML, Dropdown, HBox, Layout, VBox
from shapely.geometry import mapping, shape

from .._exceptions import EcoPlotsError
from .._nlp_utils import REGION_TYPES_MAP, resolve_region_type
from ._map_view import _set_map_bounds

_GEOSERVER = "https://geoserver.tern.org.au/geoserver/"
# Layer, URI property, and feature URI prefix. Some GeoServer layers use HTTP
# URIs even though EcoPlots uses HTTPS, so build the WFS URI from the region code.
_REGION_LAYERS = {
    "Indigenous Areas": (
        "aus_indigenous_area_2021",
        "loci_uri21",
        "http://linked.data.gov.au/dataset/asgsed3/IARE/",
    ),
    "Indigenous Locations": (
        "aus_indigenous_location_2021",
        "loci_uri21",
        "http://linked.data.gov.au/dataset/asgsed3/ILOC/",
    ),
    "Indigenous Regions": (
        "aus_indigenous_region_2021",
        "loci_uri21",
        "http://linked.data.gov.au/dataset/asgsed3/IREG/",
    ),
    "Local Government Areas": (
        "auslga_2023",
        "loci_uri21",
        "https://linked.data.gov.au/dataset/asgsed3/LGA2023/",
    ),
    "States and Territories": (
        "ausstates_2021",
        "loci_uri21",
        "http://linked.data.gov.au/dataset/asgsed3/STE/",
    ),
    "Terrestrial CAPAD regions": (
        "auscapad_terr_2022",
        "loci_uri22",
        "https://linked.data.gov.au/dataset/auscapad2022/",
    ),
    "NRM regions": ("ausnrm_2023", "loci_uri23", "https://linked.data.gov.au/dataset/ausnrm2023/"),
    "IBRA7 Bioregions": ("ibra7_region", "loci_uri7", "https://linked.data.gov.au/dataset/ibra7/"),
    "IBRA7 Subregions": (
        "ibra7_subregion",
        "loci_uri7",
        "https://linked.data.gov.au/dataset/ibra7/",
    ),
    "WWF ecoregions": ("wwfterr_2011", "loci_uri11", "https://linked.data.gov.au/dataset/wwf2011/"),
}


def _fetch_region(label, uri):
    """Fetch a region's polygons in longitude/latitude order from WFS."""
    layer, field, prefix = _REGION_LAYERS[label]
    value = (prefix + uri.rstrip("/").rsplit("/", 1)[-1]).replace("'", "''")
    response = requests.get(
        _GEOSERVER + "wfs",
        params={
            "service": "WFS",
            "version": "1.1.0",
            "request": "GetFeature",
            "typename": "administrative_area:" + layer,
            "cql_filter": f"{field}='{value}'",
            "srsName": "EPSG:4326",
            "outputFormat": "application/json",
        },
        timeout=30,
    )
    response.raise_for_status()
    data = response.json()
    if not isinstance(data, dict) or data.get("type") != "FeatureCollection":
        raise EcoPlotsError("WFS did not return a GeoJSON FeatureCollection.")
    features = [
        feature
        for feature in data.get("features", [])
        if (feature.get("geometry") or {}).get("type") in {"Polygon", "MultiPolygon"}
    ]
    if not features:
        raise EcoPlotsError("No boundary geometry was returned for this region.")
    return {"type": "FeatureCollection", "features": features}


def _copy_client(client):
    """Copy filter state while preserving mode, API origin, and client subclass."""
    preview = object.__new__(type(client))
    preview.__dict__.update(client.__dict__)
    preview._filters = copy.deepcopy(client._filters)
    preview._query_filters = copy.deepcopy(client._query_filters)
    return preview


def _remember(cache, key, value):
    """Bound per-widget caches so large region datasets do not accumulate."""
    if len(cache) >= 8:
        cache.pop(next(iter(cache)))
    cache[key] = value


class _RegionSelector:
    """Stage region selections until the map's Confirm button is pressed."""

    def __init__(self, client, map_widget, on_change):
        self.client = client
        self.map = map_widget
        self.on_change = on_change
        self.boundaries = None
        self.highlight = None
        self._updating = False
        self._restoring = False
        self._options_cache = {}
        self._geometry_cache = {}
        self.region_type = Dropdown(
            description="Region type:",
            options=[
                ("Choose a region type", None),
                *[(v, k) for k, v in REGION_TYPES_MAP.items()],
            ],
            layout=Layout(width="50%"),
            style={"description_width": "initial"},
        )
        self.region = Dropdown(
            description="Region:",
            options=[("Choose a region", None)],
            disabled=True,
            layout=Layout(width="50%"),
        )
        self.message = HTML(value="Select a region type to show its boundaries.")
        self.widget = VBox([HBox([self.region_type, self.region]), self.message])
        self.region_type.observe(self._change_type, names="value")
        self.region.observe(self._change_region, names="value")

    def _remove_layer(self, name):
        layer = getattr(self, name)
        if layer is not None and layer in self.map.layers:
            self.map.remove(layer)
        setattr(self, name, None)

    def _notify_change(self):
        if not self._restoring:
            self.on_change()

    def _region_boundary(self, label, uri):
        """Cache a lightweight display boundary, keeping its exact extent."""
        key = (label, uri)
        if key not in self._geometry_cache:
            data = _fetch_region(label, uri)
            geometries = [shape(feature["geometry"]) for feature in data["features"]]
            geometries = [geometry for geometry in geometries if not geometry.is_empty]
            if not geometries:
                raise EcoPlotsError("The selected region has an empty boundary.")
            bounds = [
                min(geometry.bounds[0] for geometry in geometries),
                min(geometry.bounds[1] for geometry in geometries),
                max(geometry.bounds[2] for geometry in geometries),
                max(geometry.bounds[3] for geometry in geometries),
            ]
            # Only the map display is simplified (about 100 m in latitude).
            # Confirm still applies the canonical region URI, not this geometry.
            display = {
                "type": "FeatureCollection",
                "features": [
                    {
                        "type": "Feature",
                        "properties": {},
                        "geometry": mapping(geometry.simplify(0.001, preserve_topology=True)),
                    }
                    for geometry in geometries
                ],
            }
            _remember(self._geometry_cache, key, (display, bounds))
        return self._geometry_cache[key]

    def preview_client(self):
        """Return an independent query using the staged region selection."""
        preview = _copy_client(self.client)
        if self.region_type.value:
            for facet in ("region_type", "region"):
                preview._filters.pop(facet, None)
                preview._query_filters.pop(facet, None)
            preview._filters["region_type"] = [REGION_TYPES_MAP[self.region_type.value]]
            preview._query_filters["region_type"] = [self.region_type.value]
            if self.region.value:
                preview._filters["region"] = [self.region.label]
                preview._query_filters["region"] = [self.region.value]
        return preview

    def _change_type(self, change):
        if self._updating:
            return
        self._remove_layer("boundaries")
        self._remove_layer("highlight")
        self._updating = True
        self.region.options = [("Choose a region", None)]
        self.region.value = None
        self.region.disabled = True
        self._updating = False
        _set_map_bounds(self.map, [[-44.0, 112.0], [-10.0, 154.0]])
        if not change["new"]:
            self.message.value = "Select a region type to show its boundaries."
            self._notify_change()
            return
        label = REGION_TYPES_MAP[change["new"]]
        layer = _REGION_LAYERS[label][0]
        # WMS keeps country-wide divisions tiled rather than downloading large
        # polygon datasets. WFS supplies the selected region's exact geometry.
        self.boundaries = WMSLayer(
            pane="ecop-region-types",
            url=_GEOSERVER + "wms",
            layers="administrative_area:" + layer,
            format="image/png",
            transparent=True,
            opacity=0.4,
            name=label,
        )
        self.map.add(self.boundaries)
        self.message.value = "Loading regions…"
        try:
            preview = self.preview_client()
            key = orjson.dumps(
                [preview._base_url, preview._mode, preview._query_filters],
                option=orjson.OPT_SORT_KEYS,
            )
            if key not in self._options_cache:
                frame = preview.get_regions(change["new"])
                options = [(str(row["key"]), str(row["uri"])) for row in frame.to_dict("records")]
                _remember(self._options_cache, key, options)
            options = self._options_cache[key]
            self.region.options = [("All regions", None), *options]
            self.region.disabled = not bool(options)
            self.message.value = (
                f"{len(options)} regions available. Choose a region to shade and zoom to it. "
                "Click <b>Confirm</b> to apply."
            )
        except (EcoPlotsError, requests.RequestException, KeyError, TypeError, ValueError) as exc:
            self.message.value = f"Could not load regions: {escape(str(exc))}"
        finally:
            self._notify_change()

    def _change_region(self, change):
        if self._updating:
            return
        self._remove_layer("highlight")
        if not change["new"]:
            _set_map_bounds(self.map, [[-44.0, 112.0], [-10.0, 154.0]])
            self.message.value = "Showing all divisions. Click <b>Confirm</b> to apply."
            self._notify_change()
            return
        self.message.value = "Loading region boundary…"
        try:
            label = REGION_TYPES_MAP[self.region_type.value]
            data, (west, south, east, north) = self._region_boundary(label, change["new"])
            self.highlight = GeoJSON(
                pane="ecop-regions",
                data=data,
                style={
                    "pane": "ecop-regions",
                    "color": "#ed694b",
                    "weight": 2,
                    "fillColor": "#ed694b",
                    "fillOpacity": 0.35,
                },
                name=self.region.label,
            )
            self.map.add(self.highlight)
            _set_map_bounds(self.map, [[south, west], [north, east]])
            self.message.value = (
                f"<b>{escape(self.region.label)}</b> selected. Click <b>Confirm</b> to apply."
            )
        except (EcoPlotsError, requests.RequestException, KeyError, TypeError, ValueError) as exc:
            self.message.value = f"Could not load region boundary: {escape(str(exc))}"
        finally:
            self._notify_change()

    def restore_selection(self):
        """Show the client's existing single region type and region on opening."""
        selected = self.client._filters.get("region_type") or self.client._query_filters.get(
            "region_type"
        )
        if not selected or len(selected) != 1:
            return
        label = resolve_region_type(selected[0])
        self._restoring = True
        try:
            self.region_type.value = next(
                uri for uri, name in REGION_TYPES_MAP.items() if name == label
            )
            regions = self.client._query_filters.get("region", [])
            if len(regions) == 1 and regions[0] in [value for _, value in self.region.options]:
                self.region.value = regions[0]
        finally:
            self._restoring = False

    def apply(self, wkt=None):
        """Validate staged filters and replace the client's region selection atomically."""
        candidate = _copy_client(self.client)
        for facet in ("region_type", "region"):
            candidate._filters.pop(facet, None)
            candidate._query_filters.pop(facet, None)
        filters = {"region_type": self.region_type.value}
        if self.region.value:
            filters["region"] = self.region.value
        if wkt:
            filters["spatial"] = wkt
        candidate.select(**filters)
        if candidate._query_filters.get("region_type") != [self.region_type.value]:
            raise EcoPlotsError("The selected region filters contain no matching records.")
        self.client._filters = candidate._filters
        self.client._query_filters = candidate._query_filters

    def clear(self):
        """Clear staged region controls and remove boundary layers."""
        self._updating = True
        self.region_type.value = None
        self.region.options = [("Choose a region", None)]
        self.region.disabled = True
        self._updating = False
        self._remove_layer("highlight")
        self._remove_layer("boundaries")
        self.message.value = "Select a region type to show its boundaries."
