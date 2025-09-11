"""EcoPlots: Python clients for TERN's EcoPlots REST API.

This package provides high-level clients that **abstract data discovery and access**
from the `EcoPlots Portal <https://ecoplots.tern.org.au>`_ into a small, Pythonic API.
Use it to discover datasets, apply filters, preview results, and retrieve tidy data
structures for analysis.

Public API:
    - :class:`~terndata.ecoplots.ecoplots.EcoPlots` — synchronous client for scripts and notebooks
    - :class:`~terndata.ecoplots.ecoplots.AsyncEcoPlots` — async counterpart for ASGI apps or
        parallel I/O in notebooks

    Returned results integrate naturally with analysis tools (e.g., ``pandas.DataFrame``,
    ``geopandas.GeoDataFrame``) or can be consumed as raw GeoJSON.

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

    Asynchronous:

    .. code-block:: python

        import asyncio
        from terndata.ecoplots import AsyncEcoPlots

        async def main():
            ec = AsyncEcoPlots()
            ec.select(site_id="TCFTNS0002")
            gdf = await ec.get_data()
            return gdf

        # asyncio.run(main())

Notes:
    - The package surface is intentionally small; modules and names prefixed with ``_`` are
        **internal** and may change without notice.
    - Projects can be saved/loaded via ``.ecoproj`` files for reproducible workflows.

See Also:
    - EcoPlots Portal: https://ecoplots.tern.org.au
    - :class:`~terndata.ecoplots.ecoplots.EcoPlots`
    - :class:`~terndata.ecoplots.ecoplots.AsyncEcoPlots`
"""

import threading

from ._utils import _background_cache_loader
from .ecoplots import AsyncEcoPlots, EcoPlots

__all__ = ["EcoPlots", "AsyncEcoPlots"]

# Start the background cache loading thread at import time (best-effort).
_cache_thread = threading.Thread(target=_background_cache_loader, daemon=True)
_cache_thread.start()
