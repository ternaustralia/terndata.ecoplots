"""Notebook widget for browsing sample images.

Provides a lightweight ipywidgets-based viewer for the ``sample_images`` column
returned by samples-mode queries.
"""

from __future__ import annotations

import base64
import importlib.resources as ir
from typing import Any

import pandas as pd
from ipywidgets import Button, Dropdown, HTML, HBox, Layout, VBox


def _as_data_uri(binary: bytes, mime: str) -> str:
    return f"data:{mime};base64," + base64.b64encode(binary).decode("ascii")


def _italicize_text(text: str) -> str:
    upper_start = ord("𝑨")
    lower_start = ord("𝒂")
    chars = []
    for ch in text:
        if "A" <= ch <= "Z":
            chars.append(chr(upper_start + (ord(ch) - ord("A"))))
        elif "a" <= ch <= "z":
            chars.append(chr(lower_start + (ord(ch) - ord("a"))))
        else:
            chars.append(ch)
    return "".join(chars)


def _extract_image_options(value: Any) -> list[tuple[str, str]]:
    """Extract labeled image URLs from mixed list/dict/string structures."""
    options: list[tuple[str, str]] = []

    if isinstance(value, str):
        if value.strip():
            return [("image", value)]
        return []

    if isinstance(value, dict):
        for key, candidate in value.items():
            if isinstance(candidate, str) and candidate.strip() and key in {
                "original",
                "thumbnail_320",
                "thumbnail_640",
                "thumbnail_1280",
                "url",
                "href",
                "src",
                "download_url",
                "thumbnail",
            }:
                options.append((key, candidate))
            elif isinstance(candidate, (list, dict)):
                options.extend(_extract_image_options(candidate))

        return options

    if isinstance(value, list):
        for item in value:
            options.extend(_extract_image_options(item))

    # Preserve order, remove duplicates
    deduped: list[tuple[str, str]] = []
    seen = set()
    for label, url in options:
        if url not in seen:
            seen.add(url)
            deduped.append((label, url))

    order = {"original": 0, "thumbnail_1280": 1, "thumbnail_640": 2, "thumbnail_320": 3}
    deduped.sort(key=lambda item: (order.get(item[0], 50), item[0]))

    return deduped


def sample_image_viewer(
    df: pd.DataFrame,
    image_column: str = "sample_images",
    sample_id_column: str = "sample_id",
    sample_name_column: str = "sample_name",
    scientific_name_column: str = "scientific_name",
) -> VBox:
    """Create an interactive image browser for samples in Jupyter notebooks.

    Args:
        df: DataFrame containing sample rows.
        image_column: Column containing image URLs or image metadata.
        sample_id_column: Optional sample id column used in row labels.
        sample_name_column: Optional sample name column for row labels.
        scientific_name_column: Optional scientific name column to display beside image.

    Returns:
        ipywidgets.VBox containing controls and image preview.
    """
    if image_column not in df.columns:
        return VBox([HTML(f"<b>No '{image_column}' column found in DataFrame.</b>")])

    rows_with_images: list[tuple[int, str, str, list[tuple[str, str]]]] = []
    for idx, row in df.iterrows():
        image_options = _extract_image_options(row.get(image_column))
        if image_options:
            sample_id = row.get(sample_id_column) if sample_id_column in df.columns else None
            sample_name = row.get(sample_name_column) if sample_name_column in df.columns else None
            scientific_name = (
                row.get(scientific_name_column) if scientific_name_column in df.columns else None
            )

            scientific_display = "N/A"
            if scientific_name is not None and pd.notna(scientific_name):
                scientific_display = str(scientific_name)

            if sample_name is not None and pd.notna(sample_name):
                base_label = str(sample_name)
            elif sample_id is not None and pd.notna(sample_id):
                base_label = str(sample_id)
            else:
                base_label = f"Sample {idx}"

            sci_for_label = _italicize_text(scientific_display) if scientific_display != "N/A" else "N/A"
            label = f"{base_label} ({sci_for_label})"

            rows_with_images.append((idx, label, scientific_display, image_options))

    if not rows_with_images:
        return VBox([HTML("<b>No rows with images were found.</b>")])


    row_dropdown = Dropdown(
        options=[(label, idx) for idx, label, _, _ in rows_with_images],
        description="Sample:",
        layout=Layout(flex="1 1 220px", min_width="180px"),
    )

    resolution_dropdown = Dropdown(
        description="Image:",
        layout=Layout(flex="0 1 180px", min_width="140px"),
    )

    zoom_in_btn = Button(description="＋  Zoom In", layout=Layout(width="auto", min_width="80px"))
    zoom_out_btn = Button(description="−  Zoom Out", layout=Layout(width="auto", min_width="80px"))
    zoom_reset_btn = Button(description="↺  Reset", layout=Layout(width="auto", min_width="70px"))
    for b in (zoom_in_btn, zoom_out_btn, zoom_reset_btn):
        b.style.button_color = "#006381"
        b.style.text_color = "white"

    meta = HTML()
    image_html = HTML(layout=Layout(width="100%"))

    with ir.files("terndata.ecoplots.assets").joinpath("TERN_logo.png").open("rb") as f:
        tern_logo_uri = _as_data_uri(f.read(), "image/png")
    with ir.files("terndata.ecoplots.assets").joinpath("ecoplots_logo.svg").open("rb") as f:
        ecoplots_logo_uri = _as_data_uri(f.read(), "image/svg+xml")

    # CSS filter that recolours a dark/black SVG → #6db3a6 (EcoPlots teal)
    ecoplots_svg_filter = (
        "brightness(0) saturate(100%) "
        "invert(67%) sepia(28%) saturate(490%) "
        "hue-rotate(131deg) brightness(0.93) contrast(91%)"
    )

    branding = HTML(
        value=(
            "<div style='position:relative; padding:4px 2px 4px 2px; margin-bottom:6px;'>"
            "<div style='display:flex; align-items:center; gap:10px; flex-wrap:wrap; padding-right:140px;'>"
            f"<img src='{tern_logo_uri}' style='height:72px; flex-shrink:0;'/>"
            "<span style='color:#043E4F; font-weight:700; font-size:28px; letter-spacing:0.2px;'>"
            "Sample Image Viewer"
            "</span>"
            "</div>"
            "<div style='position:absolute; right:2px; bottom:2px; display:flex; align-items:center; "
            "gap:3px; color:#000; font-size:10px; font-weight:500;'>"
            "<span style='font-size:10px;'>Powered by</span>"
            f"<img src='{ecoplots_logo_uri}' style='height:12px; margin-bottom:2px; filter:{ecoplots_svg_filter};'/>"
            "<span style='color:#f8c59d; margin-left:1px;'>EcoPlots</span>"
            "</div>"
            "</div>"
        )
    )

    images_by_index = {idx: image_options for idx, _, _, image_options in rows_with_images}
    scientific_by_index = {idx: scientific_name for idx, _, scientific_name, _ in rows_with_images}
    zoom_pct = 100

    def _set_resolution_options(sample_idx: int) -> None:
        image_options = images_by_index.get(sample_idx, [])
        opts = []
        default_index = None
        for i, (label, _url) in enumerate(image_options):
            display_label = label.replace("_", " ").title()
            opts.append((display_label, i))
            if label == "thumbnail_1280":
                default_index = i
        resolution_dropdown.options = opts
        if not opts:
            resolution_dropdown.value = None
        elif default_index is not None:
            resolution_dropdown.value = default_index
        else:
            resolution_dropdown.value = 0

    def _render() -> None:
        sample_idx = row_dropdown.value
        image_idx = resolution_dropdown.value
        image_options = images_by_index.get(sample_idx, [])

        if sample_idx is None or image_idx is None or not image_options:
            meta.value = "No image selected."
            image_html.value = ""
            return

        image_idx = int(image_idx)
        image_idx = max(0, min(image_idx, len(image_options) - 1))
        current_label, current_url = image_options[image_idx]
        scientific_name = scientific_by_index.get(sample_idx, "N/A")

        meta.value = (
            f"<b>Row:</b> {sample_idx} &nbsp; "
            f"<b>Scientific name:</b> <i>{scientific_name}</i> &nbsp; "
            f"<b>Image:</b> {current_label} ({image_idx + 1}/{len(image_options)}) &nbsp; "
            f"<b>Zoom:</b> {zoom_pct}% &nbsp; "
            f"<a href='{current_url}' target='_blank' rel='noopener'>open original</a>"
        )

        zoom_scale = max(0.2, zoom_pct / 100.0)
        img_style = (
            "max-width:100%; max-height:100%; width:auto; height:auto; "
            "display:block; user-select:none; -webkit-user-drag:none; pointer-events:none; "
            "transform-origin:center center; "
            f"transform:translate(0px,0px) scale({zoom_scale:.4f});"
        )

        if zoom_pct <= 100:
            container_cursor = "default"
            pointer_attrs = ""
        else:
            container_cursor = "grab"
            pointer_attrs = (
                "onpointerdown=\"this.dataset.drag='1';"
                "this.setPointerCapture(event.pointerId);"
                "this.dataset.sx=event.clientX;this.dataset.sy=event.clientY;"
                "this.style.cursor='grabbing';\" "
                "onpointermove=\"if(this.dataset.drag==='1'){"
                "var dx=event.clientX-Number(this.dataset.sx);"
                "var dy=event.clientY-Number(this.dataset.sy);"
                "var tx=Number(this.dataset.ox||0)+dx;"
                "var ty=Number(this.dataset.oy||0)+dy;"
                f"this.querySelector('img').style.transform='translate('+tx+'px,'+ty+'px) scale({zoom_scale:.4f})';"
                "this.dataset.cx=tx;this.dataset.cy=ty;}\" "
                "onpointerup=\"this.dataset.drag='0';"
                "this.dataset.ox=this.dataset.cx||'0';"
                "this.dataset.oy=this.dataset.cy||'0';"
                "this.style.cursor='grab';\" "
                "onpointercancel=\"this.dataset.drag='0';this.style.cursor='grab';\" "
            )

        image_html.value = (
            f"<div "
            f"style='border:1px solid #ddd; border-radius:8px; padding:8px; "
            f"overflow:hidden; cursor:{container_cursor}; width:100%; height:720px; "
            f"display:flex; align-items:center; justify-content:center; "
            f"touch-action:none; box-sizing:border-box;' "
            f"{pointer_attrs}>"
            f"<img src='{current_url}' draggable='false' style='{img_style}'/>"
            "</div>"
        )

    def _on_sample_change(change):
        if change.get("name") == "value":
            _set_resolution_options(change["new"])
            _render()

    def _on_resolution_change(change):
        if change.get("name") == "value":
            _render()

    def _on_zoom_in(_):
        nonlocal zoom_pct
        zoom_pct = min(1000, zoom_pct + 20)
        _render()

    def _on_zoom_out(_):
        nonlocal zoom_pct
        zoom_pct = max(20, zoom_pct - 20)
        _render()

    def _on_zoom_reset(_):
        nonlocal zoom_pct
        zoom_pct = 100
        _render()

    row_dropdown.observe(_on_sample_change, names="value")
    resolution_dropdown.observe(_on_resolution_change, names="value")
    zoom_in_btn.on_click(_on_zoom_in)
    zoom_out_btn.on_click(_on_zoom_out)
    zoom_reset_btn.on_click(_on_zoom_reset)

    _set_resolution_options(rows_with_images[0][0])
    row_dropdown.value = rows_with_images[0][0]
    _render()

    # Sample + resolution selectors
    controls = HBox(
        [row_dropdown, resolution_dropdown],
        layout=Layout(flex_flow="row wrap", align_items="center", gap="4px", width="100%"),
    )

    # Zoom controls row sits directly above the image
    zoom_controls = HBox(
        [zoom_in_btn, zoom_out_btn, zoom_reset_btn],
        layout=Layout(align_items="center", gap="4px"),
    )

    return VBox(
        [branding, controls, zoom_controls, meta, image_html],
        layout=Layout(width="100%"),
    )
