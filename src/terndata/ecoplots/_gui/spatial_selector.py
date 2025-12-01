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
from ipyleaflet import (
    DrawControl,
    Map,
    WidgetControl,
    basemap_to_tiles,
    basemaps,
    Heatmap,
    CircleMarker,
    Marker,
    Popup,
    LayerGroup,
)
from ipywidgets import HTML, Button, Checkbox, HBox, Layout, Output, VBox, ToggleButton
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

          /* Overlay toggle (off-white style) */
          .widget-toggle-button.ecop-overlay-toggle {
              background-color: #f5f5f5 !important;
              color: #043E4F !important;
              border: none !important;
              box-shadow: none !important;
              min-height: 32px !important;
              line-height: 1.5 !important;
          }
          .widget-toggle-button.ecop-overlay-toggle:hover {
              background-color: #ffffff !important;
              color: #043E4F !important;
              border: none !important;
          }
          .widget-toggle-button.ecop-overlay-toggle:focus,
          .widget-toggle-button.ecop-overlay-toggle:focus-visible {
              outline: none !important;
              box-shadow: none !important;
          }
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

    # Controls & state (Preview removed – auto rendering enabled)
    confirm_btn = Button(description="Confirm")
    clear_btn = Button(description="Clear")

    for b, cls in (
        (confirm_btn, "ecop-btn-confirm"),
        (clear_btn, "ecop-btn-clear"),
    ):
        b.add_class("ecop-btn")
        b.add_class(cls)

    status = HTML(
        value="<b>Draw a shape:</b> Use rectangle or polygon. Overlay updates automatically. Click <b>Confirm</b> to apply spatial filter."
    )
    out = Output(layout={"border": "1px solid #e5e5e5", "max_height": "220px", "overflow": "auto"})

    drawn_geom: Optional[dict[str, Any]] = None
    # Overlays for dynamic rendering (clusters heatmap or site markers)
    overlay_heatmap: Optional[Heatmap] = None
    overlay_sites_group: Optional[LayerGroup] = None
    selection_site_count: int = 0  # Track last counted sites for confirmation feedback

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
        nonlocal selection_site_count
        drawn_geom = _extract_geometry(geo_json)
        
        # Zoom to selection
        if drawn_geom:
            g = shp_shape(drawn_geom)
            minx, miny, maxx, maxy = g.bounds
            m.fit_bounds([[miny, minx], [maxy, maxx]])
            
            # Fetch clusters/sites within the drawn bounds
            try:
                # Pass geometry directly
                # Format: {"type": "Polygon", "coordinates": [...]}
                geojson_geom = drawn_geom
                
                # Fetch clusters for this specific geometry
                fetch_fn = getattr(ecoplots, "_fetch_clusters", None)

                if callable(fetch_fn):
                    selection_data = fetch_fn(geojson_geom)
                    
                    # Count total sites in selection
                    total_sites = 0
                    clusters = selection_data.get("clusters", []) or []
                    sites = selection_data.get("sites", []) or []
                    
                    if sites:
                        total_sites = len(sites)
                    elif clusters:
                        try:
                            total_sites = sum(int(c.get("num_sites", 0)) for c in clusters)
                        except Exception:
                            total_sites = len(clusters)
                    
                    selection_site_count = total_sites
                    status.value = (
                        f"<b>{total_sites} {'site' if total_sites == 1 else 'sites'} in selection. </b>"
                        f"Click <b>Confirm</b> to apply spatial filter or <b>Clear</b> to reset."
                    )
                    # Auto-render overlay after drawing
                    _preview()
                else:
                    status.value = "<b>Shape captured.</b> Click <b>Confirm</b> to apply spatial filter or <b>Clear</b>."
            except Exception as e:
                # If fetch fails, show basic message
                status.value = f"<b>Shape captured.</b> Click <b>Confirm</b> to apply spatial filter. ({e})"
        else:
            status.value = "<b>Shape captured.</b> Click <b>Confirm</b> to apply spatial filter."

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
        nonlocal overlay_heatmap
        nonlocal overlay_sites_group

        nonlocal selection_site_count
        wkt = _current_wkt()
        with out:
            out.clear_output()
            if wkt:
                out.append_stdout("─" * 60 + "\n")
                out.append_stdout("📍 Selected Spatial Filter (WKT POLYGON)\n")
                out.append_stdout("─" * 60 + "\n")
                out.append_stdout(f"{str(wkt)}\n\n")
                out.append_stdout("💡 Usage in code:\n")
                out.append_stdout("   ecoplots.select(spatial=<WKT>)\n")
                out.append_stdout("─" * 60 + "\n")
            else:
                out.append_stdout("ℹ️  No geometry drawn. Rendering preview from current filters…\n")
        status.value = (
            "<b>WKT ready.</b> Rendering preview overlay… Click <b>Confirm</b> to apply, or <b>Clear</b>."
        )

        # Remove any existing overlays before rendering a new one
        if overlay_heatmap is not None:
            try:
                m.remove_layer(overlay_heatmap)
            except Exception:
                pass
        if overlay_sites_group is not None:
            try:
                m.remove_layer(overlay_sites_group)
            except Exception:
                pass

        # Fetch cluster/sites preview data from client, if available
        preview_data: Optional[dict[str, Any]] = None
        try:
            # Support both names: _fetch_clusters (preferred) and _fetch_cluster (legacy)
            fetch_fn = getattr(ecoplots, "_fetch_clusters", None)
            if not callable(fetch_fn):
                fetch_fn = getattr(ecoplots, "_fetch_cluster", None)
            if callable(fetch_fn):
                # If we have a drawn geometry, fetch for that specific area
                # Otherwise fetch based on current filters
                if drawn_geom:
                    preview_data = fetch_fn(drawn_geom)
                else:
                    preview_data = fetch_fn()
            else:
                with out:
                    out.append_stdout(
                        "ℹ️  Cluster preview unavailable (missing _fetch_clusters/_fetch_cluster).\n"
                    )
        except Exception as e:
            with out:
                out.append_stdout(f"⚠️  Could not fetch cluster preview: {e}\n")

        # Render either sites (if < 250) or clusters as heatmap
        if preview_data:
            clusters = preview_data.get("clusters", []) or []
            sites = preview_data.get("sites", []) or []

            # Helper to parse site title from URI
            def _site_label(site_uri: str) -> str:
                if not isinstance(site_uri, str):
                    return str(site_uri)
                # Use trailing component of URI as label
                return site_uri.rstrip("/").split("/")[-1]

            if sites and len(sites) < 250:
                # Render sites as dark green dots with popup on hover/click
                markers: list[CircleMarker] = []
                shadows: list[CircleMarker] = []
                for s in sites:
                    try:
                        lat = float(s.get("latitude"))
                        lon = float(s.get("longitude"))
                    except Exception:
                        continue
                    label = _site_label(s.get("site_id", "site"))
                    # Shadow circle (slightly larger, low opacity) to distinguish close sites
                    shadow = CircleMarker(
                        location=(lat, lon),
                        radius=7,
                        color="#000000",
                        opacity=0.25,
                        fill=True,
                        fill_color="#000000",
                        fill_opacity=0.25,
                    )
                    # Main circle
                    cm = CircleMarker(
                        location=(lat, lon),
                        radius=5,
                        color="#043e4f",
                        fill=True,
                        fill_color="#043e4f",
                        fill_opacity=0.85,
                    )
                    # Attach a popup (opens on click). ipyleaflet tooltips are limited.
                    popup = Popup(location=(lat, lon), child=HTML(value=label))
                    def _bind_popup(marker: CircleMarker, p: Popup) -> None:
                        # On click, show popup near marker
                        def _on_click(**_kwargs):
                            try:
                                m.add_layer(p)
                            except Exception:
                                pass
                        marker.on_click(_on_click)

                    _bind_popup(cm, popup)

                    # Hover interactions: make marker appear to "come up"
                    def _on_mouseover(**_kwargs):
                        try:
                            cm.radius = 7
                            cm.fill_opacity = 1.0
                            cm.opacity = 1.0
                        except Exception:
                            pass

                    def _on_mouseout(**_kwargs):
                        try:
                            cm.radius = 5
                            cm.fill_opacity = 0.95
                            cm.opacity = 1.0
                        except Exception:
                            pass

                    try:
                        cm.on_mouseover(_on_mouseover)
                        cm.on_mouseout(_on_mouseout)
                    except Exception:
                        # Some environments may not support these events; ignore gracefully
                        pass

                    shadows.append(shadow)
                    markers.append(cm)

                # Add shadows first so markers render above them
                overlay_sites_group = LayerGroup(layers=shadows + markers)
                if overlay_checkbox.value:
                    m.add_layer(overlay_sites_group)
                status.value = (
                    f"<b>Viewing {len(markers)} {'Sites' if len(markers) != 1 else 'Site'}. </b>"
                    f"Draw a shape to subset and click <b>Confirm</b> to apply."
                )

            elif clusters:
                # Render clusters as heatmap, weight by num_sites (normalised)
                pts = []
                weights = []
                max_sites = max((c.get("num_sites", 1) for c in clusters), default=1)
                for c in clusters:
                    loc = c.get("location", {}) or {}
                    lat = loc.get("lat")
                    lon = loc.get("lon")
                    ns = c.get("num_sites", 1)
                    if lat is None or lon is None:
                        continue
                    try:
                        latf = float(lat)
                        lonf = float(lon)
                    except Exception:
                        continue
                    pts.append((latf, lonf))
                    # Avoid divide-by-zero
                    w = (float(ns) / float(max_sites)) if max_sites else 1.0
                    weights.append(w)

                if pts:
                    overlay_heatmap = Heatmap(
                        locations=pts,
                        weights=weights,
                        radius=25,
                        blur=15,
                        min_opacity=0.3,
                        max_zoom=13,
                        gradient={
                            0.00: "#0d0633",
                            0.08: "#0e1f55",
                            0.16: "#0f407d",
                            0.28: "#0068a8",
                            0.38: "#008fc7",
                            0.48: "#00b7c4",
                            0.58: "#00d48b",
                            0.66: "#3be35d",
                            0.74: "#b3f13b",
                            0.82: "#ffe640",
                            0.90: "#ff9d23",
                            0.96: "#ff4b18",
                            1.00: "#e60000",
                        },
                    )
                    if overlay_checkbox.value:
                        m.add_layer(overlay_heatmap)
                    total_sites = 0
                    try:
                        total_sites = int(sum(int(c.get("num_sites", 0)) for c in clusters))
                    except Exception:
                        total_sites = len(pts)
                    status.value = (
                        f"<b>Viewing {total_sites} Sites.</b> Draw a shape and click <b>Confirm</b> to apply spatial filter."
                    )
                else:
                    status.value = (
                        "<b>ℹ️ No preview points.</b> Cluster data contained no usable coordinates."
                    )
            else:
                status.value = (
                    "<b>ℹ️ No sites or clusters.</b> Nothing to render for current selection."
                )
        else:
            # No preview data returned
            status.value = (
                "<b>ℹ️ Preview unavailable.</b> Proceed to Confirm or adjust filters."
            )

    def _confirm(_btn=None) -> None:
        """Apply the current selection by calling `ecoplots.select(spatial=<WKT>)`.

        Args:
            _btn: The button instance triggering the callback (unused).
        """
        wkt = _current_wkt()
        if not wkt:
            status.value = (
                "<b>⚠️ No geometry selected.</b> Draw a shape first; overlay updates automatically."
            )
            return

        # Store current spatial filter state before applying
        had_spatial_before = "spatial" in ecoplots._filters
        spatial_before = ecoplots._filters.get("spatial") if had_spatial_before else None

        try:
            ecoplots.select(spatial=wkt)

            # Check if the filter was actually applied or rolled back
            spatial_after = ecoplots._filters.get("spatial")

            if spatial_after == wkt:
                # Filter successfully applied
                status.value = (
                    f"<b>✅ Spatial filter applied.</b> {selection_site_count} sites captured in selection."
                )
                with out:
                    out.clear_output()
                    out.append_stdout("✅ Filter applied successfully.\n\n")
                    out.append_stdout("─" * 60 + "\n")
                    out.append_stdout("💡 Usage in code:\n")
                    out.append_stdout("─" * 60 + "\n")
                    out.append_stdout(f"ecoplots.select(spatial='{wkt}')\n")
                    out.append_stdout("─" * 60 + "\n")
            elif spatial_after == spatial_before:
                # Filter was rolled back (validation returned zero records)
                status.value = (
                    "<b>⚠️ Filter rolled back.</b> The selected area contains no matching records. "
                    "Try a different region or adjust other filters first."
                )
                with out:
                    out.clear_output()
                    out.append_stdout(
                        "⚠️  The selected spatial area resulted in zero matching records.\n"
                    )
                    out.append_stdout("    Filter has been rolled back to previous state.\n\n")
                    out.append_stdout("💡 Suggestions:\n")
                    out.append_stdout("   • Try a different geographic region\n")
                    out.append_stdout("   • Adjust other active filters to broaden results\n")
                    out.append_stdout("   • Check if data exists in the selected area\n")
            else:
                # Unexpected state (shouldn't happen)
                status.value = f"<b>✅ Spatial filter updated.</b> {selection_site_count} sites captured."
                with out:
                    out.clear_output()
                    out.append_stdout("✅ Filter applied successfully.\n\n")
                    out.append_stdout("─" * 60 + "\n")
                    out.append_stdout("💡 Usage in code:\n")
                    out.append_stdout("─" * 60 + "\n")
                    out.append_stdout(f"ecoplots.select(spatial='{wkt}')\n")
                    out.append_stdout("─" * 60 + "\n")

        except (AttributeError, ValueError, RuntimeError) as e:
            status.value = f"<b>❌ Error:</b> {type(e).__name__}: {e}"
            with out:
                out.clear_output()
                out.append_stdout(f"❌ Error applying filter:\n   {type(e).__name__}: {e}\n")

    def _clear(_btn=None) -> None:
        """Clear the current selection and reset the preview output and zoom.

        Args:
            _btn: The button instance triggering the callback (unused).
        """
        nonlocal drawn_geom
        nonlocal overlay_heatmap
        nonlocal overlay_sites_group
        draw.clear()
        drawn_geom = None
        out.clear_output()
        # Remove overlays if present
        if overlay_heatmap is not None:
            try:
                m.remove_layer(overlay_heatmap)
            except Exception:
                pass
            overlay_heatmap = None
        if overlay_sites_group is not None:
            try:
                m.remove_layer(overlay_sites_group)
            except Exception:
                pass
            overlay_sites_group = None
        # Reset view to full Australia bounds
        m.fit_bounds(_AU_BOUNDS)
        status.value = (
            "<b>Selection cleared.</b> Draw a new shape; overlay will auto-update. Click <b>Confirm</b> to apply."
        )

        # Re-render preview overlay based on current filters after clearing
        try:
            _preview()
        except Exception:
            pass

    # Preview button removed (auto-preview active)
    confirm_btn.on_click(_confirm)
    clear_btn.on_click(_clear)

    # Overlay checkbox (simple toggle)
    overlay_checkbox = Checkbox(
        value=True,
        description="Show Overlay",
        layout=Layout(margin="0 0 0 10px")
    )

    # Place overlay selector at the end of the container
    controls = HBox([confirm_btn, overlay_checkbox, clear_btn])
    # Spread controls so Clear sits at the far end
    controls.layout = Layout(justify_content="space-between", width="100%")

    # Layer visibility toggle (replaced by overlay_toggle above)

    def _toggle_layer(change):
        """Show or hide the overlay layers based on checkbox state."""
        nonlocal overlay_heatmap
        nonlocal overlay_sites_group
        show = change['new']
        
        if show:
            # Re-add layers if they exist and are not already on map
            if overlay_heatmap is not None:
                try:
                    if overlay_heatmap not in m.layers:
                        m.add_layer(overlay_heatmap)
                except Exception:
                    pass
            if overlay_sites_group is not None:
                try:
                    if overlay_sites_group not in m.layers:
                        m.add_layer(overlay_sites_group)
                except Exception:
                    pass
        else:
            # Hide layers
            if overlay_heatmap is not None:
                try:
                    if overlay_heatmap in m.layers:
                        m.remove_layer(overlay_heatmap)
                except Exception:
                    pass
            if overlay_sites_group is not None:
                try:
                    if overlay_sites_group in m.layers:
                        m.remove_layer(overlay_sites_group)
                except Exception:
                    pass

    overlay_checkbox.observe(_toggle_layer, names='value')

    # Add CSS for output formatting
    # fmt: off
    output_css = HTML(
        value="""
        <style>
          /* Monospace font for code output with text wrapping */
          .ecop-output pre,
          .ecop-output code {
              font-family: Monaco, Menlo, 'Ubuntu Mono', Consolas, monospace !important;
              font-size: 12px;
              line-height: 1.4;
              white-space: pre-wrap !important;
              word-wrap: break-word !important;
              overflow-wrap: break-word !important;
          }
        </style>
        """
    )
    # fmt: on
    out.add_class("ecop-output")

    # Render initial preview immediately on widget load (based on current filters)
    try:
        # Ensure map starts at full Australia bounds before initial preview
        m.fit_bounds(_AU_BOUNDS)
        _preview()
    except Exception:
        # Non-fatal; widget still usable
        pass

    # Stack everything: CSS, map, buttons (with overlay toggle + opacity), status, preview output
    return VBox([css, css_overlay, output_css, m, controls, status, out])
