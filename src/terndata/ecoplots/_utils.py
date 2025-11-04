import asyncio
import re
from collections.abc import Coroutine
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional, TypeVar

import aiohttp
import diskcache
import geopandas as gpd
import orjson
import pandas as pd

from ._config import (
    API_BASE_URL,
    CACHE_DIR,
    CACHE_EXPIRE_SECONDS,
    VOCAB_FACETS,
)

_WKT_RE = re.compile(
    r"^\s*(SRID=\d+;)?\s*(POINT|LINESTRING|POLYGON|MULTIPOINT|MULTILINESTRING|MULTIPOLYGON|GEOMETRYCOLLECTION)\s*\(.*\)\s*$",  # noqa: E501
    re.IGNORECASE | re.DOTALL,
)

_GEOJSON_GEOM = {
    "Point",
    "LineString",
    "Polygon",
    "MultiPoint",
    "MultiLineString",
    "MultiPolygon",
    "GeometryCollection",
}

_T = TypeVar("_T")


async def _get_single_label(facet: str) -> dict[str, str]:
    """Fetch labels for a single facet from the API.

    Performs an asynchronous GET request and returns the label payload for the
    requested facet.

    Args:
        facet: Facet name to fetch labels for.

    Returns:
        Labels associated with the facet as returned by the API.

    Notes:
        - Performs network I/O
        - Intended for internal use only.
    """
    url = f"{API_BASE_URL}/api/v1.0/data/label/{facet}"
    async with aiohttp.ClientSession() as session:
        async with session.get(url) as resp:
            resp.raise_for_status()
            result = await resp.json(loads=orjson.loads)
            return result[facet]


async def _cache_labels() -> dict:
    """Fetch and cache labels for all known facets.

    Uses `asyncio.gather` to load labels concurrently and stores each facet's
    labels in the on-disk cache with an expiration.

    Returns:
        A mapping of facet to labels for all fetched facets.

    Notes:
        - Performs network I/O and disk writes
        - Intended for internal use only.
    """
    cache = diskcache.Cache(CACHE_DIR)

    async def fetch_and_cache(facet):
        # Only fetch if cache is missing or expired
        # Check if cache exists and is not expired
        if cache.get(facet, default=None, read=True) is not None:
            # print(f"Using cached labels for facet: {facet}")
            return facet, cache[facet]
        labels = await _get_single_label(facet)
        cache.set(facet, labels, expire=CACHE_EXPIRE_SECONDS)
        return facet, labels

    tasks = [fetch_and_cache(facet) for facet in VOCAB_FACETS]
    results = await asyncio.gather(*tasks)
    return dict(results)


def _background_cache_loader() -> None:
    """Run the asynchronous `_cache_labels` coroutine using `asyncio.run`.

    It is intended to be executed periodically to refresh the cache of labels.

    Notes:
        - This function is intended to be run in the background.
        - Intended for internal use only.
    """
    asyncio.run(_cache_labels())


def _run_sync(coro: Coroutine[Any, Any, _T]) -> _T:
    """Run an async coroutine from synchronous code.

    Detects an existing event loop (e.g., in notebooks) and applies a
    compatibility shim to allow awaiting from sync contexts; otherwise runs the
    coroutine in a new event loop.

    Args:
        coro: Coroutine object to execute.

    Returns:
        The result produced by the coroutine.

    Notes:
        - Imports and applies `nest_asyncio` when an active loop is detected.
        - Intended for internal use only.
    """
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        loop = None
    if loop and loop.is_running():
        # Jupyter/IPython or already running event loop
        import nest_asyncio  # type: ignore

        nest_asyncio.apply()
        return loop.run_until_complete(coro)
    return asyncio.run(coro)


def _get_cached_labels(facet: Optional[str] = None) -> dict:
    """Return cached labels for a single facet.

    Looks up labels in the on-disk cache and returns the cached value for the
    requested facet.

    Args:
        facet: Facet name to retrieve from cache.

    Returns:
        Labels for the facet if present in cache; otherwise None when the facet
        argument is omitted.

    Raises:
        KeyError: If the facet is provided but no cached labels are found.

    Notes:
        - Intended for internal use only.
    """
    cache = diskcache.Cache(CACHE_DIR)
    if facet:
        labels = cache.get(facet)
        if labels is None:
            raise KeyError(f"No cached labels found for facet: {facet}")
        return labels
    return {}


def _is_wkt(s: str) -> bool:
    """Check whether a string looks like WKT.

    Performs lightweight syntactic checks (balanced parentheses and a geometry
    keyword) without parsing.

    Args:
        s: String to check.

    Returns:
        True if the input resembles WKT; otherwise False.

    Notes:
        This is a shallow validator; intended for internal use only.
    """
    if not isinstance(s, str):
        return False
    if s.count("(") != s.count(")"):
        return False  # balanced parens
    return bool(_WKT_RE.match(s.strip()))


def _is_geojson(obj: Any) -> bool:
    """Check whether a string looks like GeoJSON.

    Performs lightweight syntactic checks (balanced parentheses and a geometry
    keyword) without parsing.

    Args:
        obj: Object to check.

    Returns:
        True if the input resembles GeoJSON; otherwise False.

    Notes:
        This is a shallow validator; intended for internal use only.
    """
    if not isinstance(obj, dict):
        return False
    t = obj.get("type")
    if t == "Feature":
        return isinstance(obj.get("geometry"), (dict, type(None)))
    if t == "FeatureCollection":
        feats = obj.get("features")
        return isinstance(feats, list)  # don’t deep-validate each feature here
    if t in _GEOJSON_GEOM:
        # Geometry must have 'coordinates' or 'geometries' (for collection)
        return ("coordinates" in obj) or (t == "GeometryCollection" and "geometries" in obj)
    return False


def _is_bbox4(v: Any) -> bool:
    """Check whether a value is a numeric bbox of length four.

    Accepts lists or tuples and ensures all four entries are numeric-like.

    Args:
        v: Value to check.

    Returns:
        True if the input is a list or tuple of four numeric values; otherwise False.

    Notes:
        Exact string forms like ``BBOX(...)`` are not accepted here.
        Intended for internal use only.
    """
    # EXACT requirement: bbox passed as array/tuple of length 4 (no "BBOX(...)" strings)
    if not (isinstance(v, (list, tuple)) and len(v) == 4):
        return False
    try:
        for x in v:
            float(x)  # numeric-ish
    except (TypeError, ValueError):
        return False
    return True


def _validate_spatial_input(value: Any) -> str:
    """Validate the shape of a spatial filter without transforming it.

    Accepts one of:
      * WKT string
      * GeoJSON mapping (Geometry, Feature, or FeatureCollection)
      * Bounding box as a four-element sequence ``[minx, miny, maxx, maxy]``

    Args:
        value: Spatial value to validate.

    Returns:
        String of detected type: ``"wkt"``, ``"geojson"``, or ``"bbox"``.

    Raises:
        ValueError: If the input is not a valid WKT, GeoJSON, or bbox value.

    Notes:
        - Performs only structural checks; no parsing or reprojection is attempted.
        - Intended for internal use only.
    """
    if isinstance(value, str) and _is_wkt(value):
        return "wkt"
    if isinstance(value, dict) and _is_geojson(value):
        return "geojson"
    if _is_bbox4(value):
        return "bbox"
    raise ValueError(
        "'spatial' must be WKT string, GeoJSON dict, or bbox [minx, miny, maxx, maxy]."
    )


def _utc_stamp() -> str:
    """Return a compact UTC timestamp string.

    Returns:
        A timestamp in the form ``YYYYMMDD_HHMMSSZ`` representing current UTC time.

    Notes:
        Intended for internal use only.
    """
    return datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%SZ")


def _ensure_ecoproj_path(path: str | Path | None) -> Path:
    """Normalise a target path to a `.ecoproj` filename.

    Resolution rules:
        * If the argument is omitted, return a timestamped filename in the
            current working directory.
        * If the argument has no `.ecoproj` suffix and no parent directory,
            return the name in the current working directory with a `.ecoproj`
            suffix.
        * If the argument already ends with `.ecoproj`, return it unchanged.

    Args:
        path: Target path or bare name.

    Returns:
        A normalized filesystem path ending with `.ecoproj`.

    Notes:
        Intended for internal use only.
    """
    if path is None:
        return Path.cwd() / f"ecoplots_{_utc_stamp()}.ecoproj"
    p = Path(path)
    if p.suffix != ".ecoproj":
        p = (Path.cwd() / p.name) if p.parent == Path(p.name).parent else p  # name-only goes to CWD
        p = p.with_suffix(".ecoproj")
    return p  # noqa: R504


def _align_and_concat(dfs: list[pd.DataFrame]) -> pd.DataFrame:
    """Align columns of multiple DataFrames and concatenate them.

    Ensures that all input DataFrames have the same columns in the same order,
    filling missing columns with "N/A" before concatenation.

    Args:
        dfs: List of pandas DataFrames to align and concatenate.

    Returns:
        A single DataFrame with aligned columns and concatenated rows.

    Notes:
        - Intended for internal use only.
    """
    if not dfs:
        return pd.DataFrame()
    all_cols, seen = [], set()
    for df in dfs:
        for c in df.columns:
            if c not in seen:
                seen.add(c)
                all_cols.append(c)
    aligned = [df.reindex(columns=all_cols) for df in dfs]
    out = pd.concat(aligned, ignore_index=True)
    return out.fillna("N/A")


def _to_geopandas(df: pd.DataFrame) -> gpd.GeoDataFrame:
    """Convert a DataFrame to a GeoDataFrame if possible.

    If the DataFrame contains 'longitude_Degree' and 'latitude_Degree' columns,
    these are used to create Point geometries. If not, a GeoDataFrame with
    null geometries is returned.

    Args:
        df: Input pandas DataFrame

    Returns:
        A GeoDataFrame with Point geometries if coordinates are present; otherwise
        a GeoDataFrame with null geometries.

    Notes:
        - Intended for internal use only.
    """
    if {"longitude_Degree", "latitude_Degree"} <= set(df.columns):
        return gpd.GeoDataFrame(
            df,
            geometry=gpd.points_from_xy(
                pd.to_numeric(df["longitude_Degree"], errors="coerce"),
                pd.to_numeric(df["latitude_Degree"], errors="coerce"),
                crs="EPSG:4326",
            ),
        )
    return gpd.GeoDataFrame(df, geometry=[None] * len(df), crs="EPSG:4326")
