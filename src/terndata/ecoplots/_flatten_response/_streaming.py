import os
from collections.abc import Iterator
from concurrent.futures import ProcessPoolExecutor
from pathlib import Path
from typing import IO, Any, Optional, Union

import geopandas as gpd
import ijson
import pandas as pd
from shapely.geometry import shape

from ._workers import _base_from_feature, _rows_from_sitevisit_task

_Source = Union[str, Path, IO[str], dict[str, Any]]


def _iter_features(source: _Source) -> Iterator[dict[str, Any]]:
    """Iterate feature dictionaries from a GeoJSON-like source.

    Streams features from a file path or file-like object using ``ijson`` to
    minimize memory usage, or iterates directly over an in-memory mapping’s
    ``features`` list.

    Args:
        source: Path to a GeoJSON FeatureCollection file, an open file-like
            object positioned at the start of a FeatureCollection, or an
            in-memory mapping containing a ``features`` array.

    Yields:
        Feature dictionaries as read from the source.

    Raises:
        TypeError: If the source is not a path, file-like object, or mapping
            with a ``features`` sequence.

    Notes:
        - Intended for internal use only.
    """
    if isinstance(source, (str, Path)):
        with open(source, "rb") as f:
            yield from ijson.items(f, "features.item")
    elif hasattr(source, "read"):
        # file-like
        yield from ijson.items(source, "features.item")
    elif isinstance(source, dict):
        yield from (feat for feat in source.get("features", []) or [])
    else:
        raise TypeError(f"Unsupported source type: {type(source).__name__}")


def _iter_sitevisit_tasks_from(
    source: _Source,
) -> Iterator[tuple[dict[str, Any], dict[str, Any], Optional[dict[str, Any]]]]:
    """Generate per-visit tasks from features.

    For each input feature, yield one task per entry in ``properties.siteVisit``.
    If a feature has no ``siteVisit`` list, yield a single task with an empty
    visit so that at least one row is emitted downstream.

    Args:
        source: Same kinds of sources accepted by ``_iter_features`` (path,
            file-like, or in-memory mapping with ``features``).

    Yields:
        A three-tuple ``(base, site_visit, geometry)`` where:
        - ``base`` is a dict of feature/base properties,
        - ``site_visit`` is a dict for one visit (or empty dict if none),
        - ``geometry`` is the raw GeoJSON geometry mapping or ``None``.

    Notes:
        - Intended for internal use only.
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
    max_workers: Optional[int] = None,
    chunksize: int = 32,
) -> Union[pd.DataFrame, gpd.GeoDataFrame]:
    """Flatten a GeoJSON FeatureCollection into a tabular structure.

    Expands each feature's ``properties.siteVisit`` entries into rows and
    assembles a tidy table (one row per observation record). Work is parallelized
    across site-visit tasks using a ``ProcessPoolExecutor``. Geometries are
    converted to Shapely objects in the parent process and attached as a
    ``geometry`` column.

    Args:
        source: Path, file-like object, or in-memory mapping containing a
            GeoJSON FeatureCollection.
        crs: Coordinate reference system for the output GeoDataFrame. Defaults
            to ``"EPSG:4326"``.
        max_workers: Maximum worker processes for parallel expansion. Defaults
            to ``os.cpu_count()``.
        chunksize: Number of tasks submitted to workers per batch.

    Returns:
        A GeoDataFrame with one row per observation and
        a ``geometry`` column. If there are no rows, returns an empty GeoDataFrame
        with the specified CRS and a ``geometry`` column.

    Notes:
        - Intended for internal use only.
        - Uses process-based parallelism for CPU-bound expansion while deferring
        geometry construction to the parent to avoid repeated conversions in workers.
    """
    rows: list[dict[str, Any]] = []

    tasks_iter = _iter_sitevisit_tasks_from(source)

    max_workers = max_workers if max_workers and max_workers > 0 else os.cpu_count()

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
