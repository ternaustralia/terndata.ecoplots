import os
import ijson
import pandas as pd
import geopandas as gpd

from concurrent.futures import ProcessPoolExecutor
from pathlib import Path
from typing import Any, Dict, Iterator, List, Optional, Tuple, Union, IO
from shapely.geometry import shape

from ._workers import _base_from_feature, _rows_from_sitevisit_task

_Source = Union[str, Path, IO[str], Dict[str, Any]]

def _iter_features(source: _Source) -> Iterator[Dict[str, Any]]:
    """
    Yield feature dicts from:
      - file path or file-like: streamed via ijson
      - in-memory dict: iterate geojson['features']
    """
    if isinstance(source, (str, Path)):
        with open(source, "rb") as f:
            for feat in ijson.items(f, "features.item"):
                yield feat
    elif hasattr(source, "read"):
        # file-like
        for feat in ijson.items(source, "features.item"):
            yield feat
    elif isinstance(source, dict):
        for feat in (source.get("features", []) or []):
            yield feat
    else:
        raise TypeError(f"Unsupported source type: {type(source).__name__}")



def _iter_sitevisit_tasks_from(source: _Source) -> Iterator[Tuple[Dict[str, Any], Dict[str, Any], Optional[Dict[str, Any]]]]:
    """
    For each feature, yield one task per siteVisit.
    If a feature has no siteVisit list, yield a single empty-visit task so we still emit one row.
    """
    for feat in _iter_features(source):
        base, geom = _base_from_feature(feat)
        visits = (feat.get("properties", {}) or {}).get("siteVisit") or []
        if not visits:
            yield (base, {}, geom)
            continue
        for sv in visits:
            yield (base, sv, geom)


def _flatten_geojson(
    source: _Source,
    *,
    crs: str = "EPSG:4326",
    max_workers: Optional[int] = os.cpu_count(),
    chunksize: int = 32,
) -> Union[pd.DataFrame, gpd.GeoDataFrame]:
    """
    parallel flatten:
      - source: path/IO to a GeoJSON FeatureCollection, or an in-memory dict
      - parallelises across siteVisit tasks in a ProcessPool
      - returns a table (one row per observation record)
    """
    rows: List[Dict[str, Any]] = []

    tasks_iter = _iter_sitevisit_tasks_from(source)

    with ProcessPoolExecutor(max_workers=max_workers) as ex:
        for chunk_rows in ex.map(_rows_from_sitevisit_task, tasks_iter, chunksize=chunksize):
            # chunk_rows is a list of row dicts for one siteVisit
            rows.extend(chunk_rows)

    if not rows:
        # empty input -> empty frame
        return gpd.GeoDataFrame(columns=["geometry"], geometry="geometry", crs=crs)

    df = pd.DataFrame(rows)

    # Convert raw GeoJSON mappings to Shapely only once, in parent
    df["geometry"] = df["geometry"].apply(lambda g: shape(g) if g else None)
    return gpd.GeoDataFrame(df, geometry="geometry", crs=crs)
