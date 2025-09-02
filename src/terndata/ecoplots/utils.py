import os
import re
import aiohttp
import asyncio
import diskcache
import orjson


from pathlib import Path
from typing import Dict, Union, List, Any

from .config import (
    API_BASE_URL,
    CACHE_DIR,
    CACHE_EXPIRE_SECONDS,
    VOCAB_FACETS,
)


_WKT_RE = re.compile(
    r"^\s*(SRID=\d+;)?\s*(POINT|LINESTRING|POLYGON|MULTIPOINT|MULTILINESTRING|MULTIPOLYGON|GEOMETRYCOLLECTION)\s*\(.*\)\s*$",
    re.IGNORECASE | re.DOTALL,
)

_GEOJSON_GEOM = {
    "Point","LineString","Polygon","MultiPoint","MultiLineString","MultiPolygon","GeometryCollection"
}


async def _get_single_label(facet):
    print("Fetching labels for facet:", facet)
    url = f"{API_BASE_URL}/api/v1.0/data/label/{facet}"
    async with aiohttp.ClientSession() as session:
        async with session.get(url) as resp:
            resp.raise_for_status()
            result = await resp.json(loads=orjson.loads)
            return result[facet]


async def _cache_labels():
    """
    Fetches and caches labels for all facets using asyncio.gather.
    Stores each as a key in diskcache.
    """
    cache = diskcache.Cache(CACHE_DIR)
    async def fetch_and_cache(facet):
        # Only fetch if cache is missing or expired
        # Check if cache exists and is not expired
        if cache.get(facet, default=None, read=True) is not None:
            print(f"Using cached labels for facet: {facet}")
            return facet, cache[facet]
        labels = await _get_single_label(facet)
        cache.set(facet, labels, expire=CACHE_EXPIRE_SECONDS)
        return facet, labels

    tasks = [fetch_and_cache(facet) for facet in VOCAB_FACETS]
    results = await asyncio.gather(*tasks)
    return dict(results)


def _background_cache_loader():
    """
    Background task to cache labels for all facets.
    This can be run periodically to refresh the cache.
    """
    asyncio.run(_cache_labels())


def _run_sync(coro: asyncio.coroutine) -> any:
    """
    Run an async coroutine as a sync call.
    Works in scripts, console, AND notebooks.
    """
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        loop = None
    if loop and loop.is_running():
        # We're in Jupyter/IPython or already running event loop
        import nest_asyncio
        nest_asyncio.apply()
        return loop.run_until_complete(coro)
    else:
        return asyncio.run(coro)
    

def _get_cached_labels(facet: str = None) -> Dict[str, str]:
    cache = diskcache.Cache(CACHE_DIR)
    if facet:
        labels = cache.get(facet)
        if labels is None:
            raise KeyError(f"No cached labels found for facet: {facet}")
        return labels
    

def _normalise_to_list(value: Union[str, List[str]]) -> List[str]:
    """
    Normalizes a single string or a list of strings to a list.
    """
    if isinstance(value, str):
        return [v.strip() for v in value.split(",") if v.strip()]
    elif isinstance(value, list):
        return [str(v).strip() for v in value if str(v).strip()]
    else:
        raise TypeError(f"Invalid filter value type: {type(value)}")


def _atomic_replace(tmp_path: Path, final_path: Path) -> None:
    final_path.parent.mkdir(parents=True, exist_ok=True)
    os.replace(tmp_path, final_path)

def _is_zip_project(path: Path) -> bool:
    return path.suffix.lower() == ".ecoproj"

def _is_wkt(s: str) -> bool:
    # Fast checks only (no parsing):
    if not isinstance(s, str): return False
    if s.count("(") != s.count(")"): return False  # balanced parens
    return bool(_WKT_RE.match(s.strip()))

def _is_geojson(obj: Any) -> bool:
    if not isinstance(obj, dict): return False
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
    # EXACT requirement: bbox passed as array/tuple of length 4 (no "BBOX(...)" strings)
    if not (isinstance(v, (list, tuple)) and len(v) == 4):
        return False
    try:
        _ = [float(x) for x in v]  # numeric-ish
    except Exception:
        return False
    return True

def _validate_spatial_input(value: Any) -> str:
    """
    Validate 'spatial' input format only.
    Returns one of {'wkt','geojson','bbox'} if valid; raises ValueError otherwise.
    """
    if isinstance(value, str) and _is_wkt(value):
        return "wkt"
    if isinstance(value, dict) and _is_geojson(value):
        return "geojson"
    if _is_bbox4(value):
        return "bbox"
    raise ValueError("'spatial' must be WKT string, GeoJSON dict, or bbox [minx, miny, maxx, maxy].")
