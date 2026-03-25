"""Notebook widget for browsing sample IGSN DOI pages.

Provides an ipywidgets-based viewer that lists sample-derived IGSNs in a
Dropdown and renders the selected DOI landing page in an iframe.
"""

from __future__ import annotations

import base64
import importlib.resources as ir
import re
from typing import Optional
from urllib.parse import urlparse

import pandas as pd
from ipywidgets import Dropdown, HTML, Layout, VBox


def _as_data_uri(binary: bytes, mime: str) -> str:
    return f"data:{mime};base64," + base64.b64encode(binary).decode("ascii")


def _normalize_igsn_input(igsn: str) -> Optional[str]:
    """Normalize user input into an IGSN identifier (e.g. 10.60792/abc-123).

    Accepts bare identifiers and DOI URLs/hosts.
    """
    value = (igsn or "").strip()
    if not value:
        return None

    if value.startswith("http://") or value.startswith("https://"):
        parsed = urlparse(value)
        if parsed.netloc.lower() != "doi.org":
            return None
        value = parsed.path.lstrip("/")
    elif value.lower().startswith("doi.org/"):
        value = value[8:]

    if " " in value:
        return None

    if not re.fullmatch(r"10\.\d{4,9}/\S+", value):
        return None

    return value


def igsn_viewer(df: pd.DataFrame, iframe_height: str = "640px", igsn: Optional[str] = None) -> VBox:
    """Create an interactive IGSN viewer for Jupyter notebooks.

    Args:
        df: DataFrame with at least ``sample_name`` and ``igsn`` columns.
        iframe_height: CSS height for the iframe area.
        igsn: Optional direct IGSN identifier or DOI URL. When provided,
            the widget renders only that DOI iframe.

    Returns:
        ipywidgets.VBox containing controls and DOI iframe.
    """
    with ir.files("terndata.ecoplots.assets").joinpath("TERN_logo.png").open("rb") as f:
        tern_logo_uri = _as_data_uri(f.read(), "image/png")
    with ir.files("terndata.ecoplots.assets").joinpath("ecoplots_logo.svg").open("rb") as f:
        ecoplots_logo_uri = _as_data_uri(f.read(), "image/svg+xml")

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
            "Sample IGSN Viewer"
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

    status = HTML()
    frame = HTML(layout=Layout(width="100%"))

    def _render(igsn_value: Optional[str]) -> None:
        if not igsn_value:
            status.value = "No IGSN selected."
            frame.value = ""
            return

        doi_url = f"https://doi.org/{igsn_value}"
        status.value = (
            f"<b>DOI:</b> <a href='{doi_url}' target='_blank' rel='noopener'>{doi_url}</a>"
        )
        frame.value = (
            "<iframe "
            f"src='{doi_url}' "
            "style='width:100%; border:1px solid #ddd; border-radius:8px;' "
            f"height='{iframe_height}' "
            "loading='lazy' referrerpolicy='no-referrer-when-downgrade'>"
            "</iframe>"
        )

    # Direct mode: render only the requested DOI iframe.
    if igsn is not None:
        normalized = _normalize_igsn_input(igsn)
        if normalized is None:
            return VBox(
                [
                    branding,
                    HTML(
                        "<b>Invalid IGSN input.</b> "
                        "Use one of: <code>10.60792/...</code>, "
                        "<code>doi.org/10.60792/...</code>, or "
                        "<code>https://doi.org/10.60792/...</code>."
                    ),
                ],
                layout=Layout(width="100%"),
            )

        _render(normalized)
        return VBox([branding, status, frame], layout=Layout(width="100%"))

    required = {"sample_name", "igsn"}
    if df.empty or not required.issubset(df.columns):
        return VBox([branding, HTML("<b>No IGSN records available to display.</b>")])

    options = []
    for _, row in df.iterrows():
        sample_name = str(row.get("sample_name", ""))
        igsn_value = str(row.get("igsn", ""))
        if not igsn_value:
            continue
        label = f"{sample_name} ({igsn_value})"
        options.append((label, igsn_value))

    if not options:
        return VBox([branding, HTML("<b>No valid IGSN rows were found.</b>")])

    selector = Dropdown(
        options=options,
        description="IGSN:",
        layout=Layout(width="100%"),
    )

    def _on_change(change):
        if change.get("name") == "value":
            _render(change.get("new"))

    selector.observe(_on_change, names="value")
    selector.value = options[0][1]
    _render(selector.value)

    return VBox(
        [branding, selector, status, frame],
        layout=Layout(width="100%"),
    )
