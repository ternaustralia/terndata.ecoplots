"""EcoPlots: Python clients for TERN's EcoPlots REST API.

This package provides high-level clients that **abstract data discovery and access**
from the `EcoPlots Portal <https://ecoplots.tern.org.au>`_ into a small, Pythonic API.
Use it to discover datasets, apply filters, preview results, and retrieve tidy data
structures for analysis.

Operational Modes:
        EcoPlots supports two modes via the ``mode`` argument on client construction:

        - ``observations`` (default): access observation workflows including feature
            types, observed properties, and GeoJSON-oriented retrieval.
        - ``samples``: access material-sample workflows (for example,
            ``material_sample_type`` and ``used_procedure``) with samples-specific
            discovery and retrieval semantics.

Public API:
    - :class:`~terndata.ecoplots.ecoplots.EcoPlots` — synchronous client for scripts and notebooks
    - :class:`~terndata.ecoplots.ecoplots.AsyncEcoPlots` — async counterpart for ASGI apps or
        parallel I/O, installed with ``terndata.ecoplots[async]``

    Returned results integrate naturally with analysis tools (e.g., ``pandas.DataFrame``,
    ``geopandas.GeoDataFrame``), Parquet bytes, or raw GeoJSON for observations.

Install modules:
    - ``pip install terndata.ecoplots``: sync client, observations/samples workflows,
      discovery, filtering, data and attribute retrieval, Parquet output.
    - ``pip install "terndata.ecoplots[async]"``: async transport and streaming.
    - ``pip install "terndata.ecoplots[gui]"``: Jupyter/ipyleaflet/ipywidgets helpers.

Side Effects:
    On import, EcoPlots may start a **best-effort, background cache preloader** to warm up
    lightweight metadata used for discovery and validation. This is deliberately minimal;
    if the loader is unavailable (e.g., no network), the import still succeeds and normal
    client operations continue without caching.

Quick Start:
    Synchronous:

    .. code-block:: python

        from terndata.ecoplots import EcoPlots

        ec = EcoPlots()
        ec.select(site_id="TCFTNS0002")        # add filters
        preview = ec.preview().head()           # quick look
        gdf = ec.get_data()                     # full pull (GeoDataFrame)

    Synchronous (samples mode):

    .. code-block:: python

        from terndata.ecoplots import EcoPlots

        ec = EcoPlots("samples")
        ec.select(material_sample_type="Plant Tissue Sample")
        gdf = ec.get_data(dformat="gpd")
        df = ec.get_data(dformat="pd")
        parquet_bytes = ec.get_data(dformat="pq")

    Asynchronous:

    .. code-block:: python

        import asyncio
        from terndata.ecoplots import AsyncEcoPlots

        async def main():
            ec = AsyncEcoPlots()
            ec.select(site_id="TCFTNS0002")
            gdf = await ec.get_data()
            async for chunk in ec.get_data_stream(dformat="gpd"):
                ...
            return gdf

        # asyncio.run(main())

    Asynchronous (samples mode):

    .. code-block:: python

        import asyncio
        from terndata.ecoplots import AsyncEcoPlots

        async def main_samples():
            ec = AsyncEcoPlots("samples")
            ec.select(material_sample_type="Plant Tissue Sample")
            gdf = await ec.get_data(dformat="gpd")
            df = await ec.get_data(dformat="pd")
            return gdf, df

        # asyncio.run(main_samples())

Notes:
    - The package surface is intentionally small; modules and names prefixed with ``_`` are
        **internal** and may change without notice.
    - Projects can be saved/loaded via ``.ecoproj`` files for reproducible workflows.
    - In ``samples`` mode, exactly one ``material_sample_type`` must be selected
        before data retrieval.

See Also:
    - EcoPlots Portal: https://ecoplots.tern.org.au
    - :class:`~terndata.ecoplots.ecoplots.EcoPlots`
    - :class:`~terndata.ecoplots.ecoplots.AsyncEcoPlots`
"""

import threading

from ._exceptions import EcoPlotsError
from ._utils import _background_cache_loader
from .ecoplots import AsyncEcoPlots, EcoPlots

__all__ = ["EcoPlots", "AsyncEcoPlots", "EcoPlotsError"]

# Start the background cache loading thread at import time (best-effort).
_cache_thread = threading.Thread(target=_background_cache_loader, daemon=True)
_cache_thread.start()
