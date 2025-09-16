"""Spatial selector widget for EcoPlots.

This module provides a Jupyter/ipywidgets-based map control that lets a user draw a
rectangle or polygon and then preview/confirm a WKT spatial filter that is passed to
`ecoplots.select(spatial=<WKT>)`. It embeds TERN/EcoPlots branding and keeps the map
bounded to Australia.
"""

from __future__ import annotations

import base64
import importlib.resources as ir
from typing import Any, Optional

from defusedxml import ElementTree as ET
from ipyleaflet import DrawControl, Map, WidgetControl, basemap_to_tiles, basemaps
from ipywidgets import HTML, Button, HBox, Layout, Output, VBox
from shapely.geometry import box as shp_box
from shapely.geometry import shape as shp_shape

_AU_BOUNDS = [[-43.8, 112.9], [-10.6, 153.6]]  # [ [south, west], [north, east] ]


def _extract_geometry(geo_json: dict[str, Any]) -> Optional[dict[str, Any]]:
    """Extract the GeoJSON geometry object from a Feature, FeatureCollection, or raw geometry dict.

    Args:
        geo_json: A GeoJSON-like dict emitted by ipyleaflet DrawControl.

    Returns:
        The geometry dict (containing "type" and "coordinates"), or None if not present.
    """
    if not geo_json:
        return None
    t = geo_json.get("type")
    if t == "Feature":
        return geo_json.get("geometry")
    if t == "FeatureCollection":
        feats = geo_json.get("features") or []
        return feats[0]["geometry"] if feats else None
    return geo_json if isinstance(geo_json, dict) and "coordinates" in geo_json else None


def _as_png_data_uri(b: bytes) -> str:
    """Encode PNG bytes as a data URI suitable for an <img> tag.

    Args:
        b: Raw PNG bytes.

    Returns:
        A data URI string in the form "data:image/png;base64,<...>".  # noqa: DAR203
    """

    return "data:image/png;base64," + base64.b64encode(b).decode("ascii")


def _as_svg_data_uri(
    svg_bytes: bytes,
    *,
    fill: Optional[str] = None,
) -> str:
    """Encode SVG bytes as a data URI, optionally overriding fill attributes.

    Args:
        svg_bytes: Raw SVG content as bytes.
        fill: Color to set as the fill for all elements. Defaults to None.

    Returns:
        A data URI string in the form "data:image/svg+xml;base64,<...>"
        using the recolored SVG.  # noqa: DAR203
    """
    root = ET.fromstring(
        svg_bytes.decode("utf-8"),
        forbid_dtd=True,
        forbid_entities=True,
        forbid_external=True,
    )
    # SVGs often have namespaces; keep it simple by setting attributes regardless.
    for el in root.iter():
        if fill is not None:
            el.set("fill", fill)
    recolored = ET.tostring(root, encoding="utf-8")
    b64 = base64.b64encode(recolored).decode("ascii")
    return f"data:image/svg+xml;base64,{b64}"


def spatial_selector(
    ecoplots: Any,
    *,
    center: tuple[float, float] = (-25.0, 133.0),
    zoom: int = 4,
) -> VBox:
    """Create a Jupyter widget to select a spatial filter and apply it via `select()`.

    Workflow:
      1) Draw rectangle or polygon on the map
      2) Click **Preview** to print the WKT `POLYGON(...)`
      3) Click **Confirm** to call `ecoplots.select(spatial=<WKT>)`
      4) **Clear** resets selection and zooms back to Australia

    Args:
        ecoplots: EcoPlots instance to update upon Confirm.
        center: Initial center (lat, lon).
        zoom: Initial zoom level.

    Returns:
        ipywidgets.VBox for display in Jupyter.
    """
    # Map
    m = Map(center=center, zoom=zoom, scroll_wheel_zoom=True, attribution_control=False)
    m.layout = Layout(width="100%", height="500px")
    m.add_layer(basemap_to_tiles(basemaps.CartoDB.Positron))
    m.fit_bounds(_AU_BOUNDS)
    m.max_bounds = _AU_BOUNDS

    # DrawControl: enable rectangle + polygon
    draw = DrawControl(
        circlemarker={},
        circle={},
        polyline={},
        marker={},
        rectangle={"shapeOptions": {"color": "#ed694b", "fillOpacity": 0.15}},
        polygon={"shapeOptions": {"color": "#ed694b", "fillOpacity": 0.15}},
        edit=False,  # hide edit hooks at Python level
    )
    m.add_control(draw)

    # CSS: buttons + hide leaflet-draw edit/remove/actions
    css = HTML(
        value="""
        <style>
          /* Hide Leaflet Draw edit/remove toolbar and actions */
          .leaflet-draw-toolbar .leaflet-draw-edit-edit,
          .leaflet-draw-toolbar .leaflet-draw-edit-remove,
          .leaflet-draw-actions { display: none !important; }

          /* Styled action buttons */
          .ecop-btn { color:#fff !important; border:none !important; }
          /* Preview */
          .ecop-btn-preview { background-color:#006381 !important; }
          .ecop-btn-preview:hover { background-color:#A5C8DC !important; color:#006381 !important; }
          /* Confirm */
          .ecop-btn-confirm { background-color:#043E4F !important; }
          .ecop-btn-confirm:hover { background-color:#B3D4C9 !important; color:#043E4F !important; }
          /* Clear */
          .ecop-btn-clear { background-color:#ED694B !important; }
          .ecop-btn-clear:hover { background-color:#FBC9A0 !important; color:#5b2d1f !important; }
        </style>
        """
    )

    # Top-right "TERN logo" overlay
    css_overlay = HTML(
        value="""
        <style>
            /* Remove background/shadow/border for controls on the right only */
            .leaflet-top.leaflet-right .leaflet-control,
            .leaflet-bottom.leaflet-right .leaflet-control {
                background: transparent !important;
                box-shadow: none !important;
                border: none !important;
                padding: 0 !important;
            }
        </style>
        """
    )

    with ir.files("terndata.ecoplots.assets").joinpath("TERN_logo.png").open("rb") as f:
        overlay_logo_bytes = f.read()

    top_right_logo = HTML(
        value=f'<img src="{_as_png_data_uri(overlay_logo_bytes)}" '
        f'style="height:5rem; display:block; pointer-events:none;" />'
    )
    m.add_control(WidgetControl(widget=top_right_logo, position="topright"))

    # Bottom-right “Powered by” overlay
    with ir.files("terndata.ecoplots.assets").joinpath("ecoplots_logo.svg").open("rb") as f:
        brand_logo_bytes = f.read()

    branding = HTML(
        value=f"""
        <div style="
            background: rgba(255,255,255,0.5);
            padding: 2px 6px;
            border-radius: 6px;
            font: 500 11px/1.2 Inter, system-ui, Arial, sans-serif;
            color: #333;
            margin: 0;
            white-space: nowrap;">
        Powered by
        <img src="{_as_svg_data_uri(brand_logo_bytes, fill="#6EB3A6")}"
            style="height:10px; vertical-align:middle; padding-bottom:2px;" />
        <a href="https://ecoplots.tern.org.au" target="_blank" rel="noopener noreferrer"
            style="color:#F5A26C; text-decoration:none; font-weight:700; margin-left:1px;">
            EcoPlots
        </a>
        </div>
        """
    )
    m.add_control(WidgetControl(widget=branding, position="bottomright"))

    # Controls & state
    preview_btn = Button(description="Preview")
    confirm_btn = Button(description="Confirm")
    clear_btn = Button(description="Clear")

    clear_btn.layout = Layout(margin="0 0 0 auto")  # right-align Clear button

    for b, cls in (
        (preview_btn, "ecop-btn-preview"),
        (confirm_btn, "ecop-btn-confirm"),
        (clear_btn, "ecop-btn-clear"),
    ):
        b.add_class("ecop-btn")
        b.add_class(cls)

    status = HTML(value="Draw a rectangle or polygon, then click <b>Preview</b>.")
    out = Output(layout={"border": "1px solid #e5e5e5", "max_height": "220px", "overflow": "auto"})

    drawn_geom: Optional[dict[str, Any]] = None

    @draw.on_draw
    def _on_draw(_self, action, geo_json):  # noqa: ARG001 (callback signature)
        """Draw control callback for ipyleaflet.

        Captures the drawn shape and zoom the map to its bounds.

        Args:
            _self: The DrawControl instance (unused; required by callback signature).
            action (str): The draw action name (e.g., 'created', 'edited').
            geo_json (dict[str, Any]): The GeoJSON payload from the draw event.
        """

        nonlocal drawn_geom
        drawn_geom = _extract_geometry(geo_json)
        status.value = "Shape captured. Click <b>Preview</b> to view WKT."
        # Zoom to selection
        if drawn_geom:
            g = shp_shape(drawn_geom)
            minx, miny, maxx, maxy = g.bounds
            m.fit_bounds([[miny, minx], [maxy, maxx]])

    def _current_wkt() -> Optional[str]:
        """Return WKT for the currently drawn geometry.

        Returns:
            A WKT `POLYGON(...)` string for the current selection, or None if nothing is drawn.
        """
        if not drawn_geom:
            return None
        # If it's a rectangle, normalise to bbox polygon; polygon stays as drawn
        g = shp_shape(drawn_geom)
        if g.geom_type == "Polygon":
            # Might be rectangle or polygon.
            # If rectangle-like, bbox is fine, still polygon WKT.
            pass
        minx, miny, maxx, maxy = g.bounds
        # If the user drew a free polygon, we keep exact WKT.
        # If rectangle, this gives a proper POLYGON box.
        return (
            g.wkt
            if (g.geom_type == "Polygon" and len(g.exterior.coords) > 5)
            else shp_box(minx, miny, maxx, maxy).wkt
        )

    def _preview(_btn=None) -> None:
        """Render the current selection's WKT into the output area.

        Args:
            _btn: The button instance triggering the callback (unused).

        Returns:
            None.
        """

        wkt = _current_wkt()
        with out:
            out.clear_output()
            if not wkt:
                # print("No geometry drawn.")
                out.append_stdout("No geometry drawn.\n")
                return
            # print("[Selected spatial filter — WKT POLYGON]")
            # print(wkt)
            # print("\nUse in code:\n  ec.select(spatial=<WKT>)")
            out.append_stdout("[Selected spatial filter — WKT POLYGON]\n")
            out.append_stdout(f"{str(wkt)}\n")
            out.append_stdout("\nUse in code:\n  ec.select(spatial=<WKT>)\n")
        status.value = "Review the WKT above. Click <b>Confirm</b> to apply, or <b>Clear</b>."

    def _confirm(_btn=None) -> None:
        """Apply the current selection by calling `ecoplots.select(spatial=<WKT>)`.

        Args:
            _btn: The button instance triggering the callback (unused).
        """
        wkt = _current_wkt()
        if not wkt:
            status.value = "<b>No geometry selected.</b> Draw and Preview first."
            return
        try:
            ecoplots.select(spatial=wkt)
            status.value = "<b>Applied.</b> The spatial filter has been set via select()."
        except (AttributeError, ValueError, RuntimeError) as e:
            status.value = f"<b>Failed to apply:</b> {type(e).__name__}: {e}"

    def _clear(_btn=None) -> None:
        """Clear the current selection and reset the preview output and zoom.

        Args:
            _btn: The button instance triggering the callback (unused).
        """
        nonlocal drawn_geom
        draw.clear()
        drawn_geom = None
        out.clear_output()
        m.fit_bounds(_AU_BOUNDS)  # reset view to Australia
        status.value = "Cleared. Draw again, then click <b>Preview</b>."

    preview_btn.on_click(_preview)
    confirm_btn.on_click(_confirm)
    clear_btn.on_click(_clear)

    controls = HBox([preview_btn, confirm_btn, clear_btn])

    # Stack everything: CSS, map, buttons, status, preview output
    return VBox([css, css_overlay, m, controls, status, out])
