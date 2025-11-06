"""Provide synchronous and asynchronous clients for the EcoPlots REST API.

This module exposes :class:`~terndata.ecoplots.ecoplots.EcoPlots` (sync) and
:class:`~terndata.ecoplots.ecoplots.AsyncEcoPlots` (async) for discovering,
filtering, previewing, and retrieving ecological plot data. Results can be
returned as ``pandas.DataFrame``/``geopandas.GeoDataFrame`` or raw GeoJSON.
Projects are saveable/loadable via ``.ecoproj`` files for reproducible workflows.

Examples:
    Synchronous:

    .. code-block:: python

        from terndata.ecoplots import EcoPlots

        ec = EcoPlots()
        ec.select(site_id="TCFTNS0002")
        gdf = ec.get_data()

    Asynchronous:

    .. code-block:: python

        from terndata.ecoplots import AsyncEcoPlots

        async def main():
            aec = AsyncEcoPlots()
            aec.select(site_id="TCFTNS0002")
            gdf = await aec.get_data()
            return gdf

        # In a script: asyncio.run(main())
        # In a notebook: await main()
"""

import asyncio
import io
from typing import Optional, Union, cast

import geopandas as gpd
import orjson
import pandas as pd
from diskcache import Cache

from ._base import EcoPlotsBase
from ._config import CACHE_DIR
from ._flatten_response._streaming import _flatten_geojson
from ._gui import spatial_selector
from ._utils import (
    _align_and_concat,
    _run_sync,
    _to_geopandas,
)


class EcoPlots(EcoPlotsBase):
    """High-level Python client for the EcoPlots REST API.

    Provides a small, Pythonic surface for **discovering**, **filtering**, **previewing**,
    and **retrieving** ecological plot data. Returns tidy structures for analysis
    (``pandas.DataFrame``, ``geopandas.GeoDataFrame``) or raw GeoJSON.

    The class mirrors the async runtime (`AsyncEcoPlots`) but is synchronous for
    notebook and script workflows. Projects can be serialised and reloaded via
    ``.ecoproj`` files to make analyses reproducible.

    Examples:
        Basic usage:

        .. code-block:: python

            from terndata.ecoplots import EcoPlots

            ecoplots = EcoPlots()
            ecoplots.get_datasources().head()         # discover datasets
            ecoplots.select(site_id="TCFTNS0002")     # add filters (validated & fuzzy-resolved)
            ecoplots.preview().head()                 # quick look (first page)
            df = ecoplots.get_data()                  # full pull as GeoDataFrame

        Save / load a project:

        .. code-block:: python

            path = ecoplots.save("myproject.ecoproj")
            ecoplots2 = EcoPlots.load(path)
            ecoplots2.get_filter("site_id")           # ['TCFTNS0002']

    See Also:
        :class:`~terndata.ecoplots.ecoplots.AsyncEcoPlots`: Async counterpart with the same surface area.
    """

    def __init__(self, filterset: Optional[dict] = None, query_filters: Optional[dict] = None):
        """Initialise the EcoPlots client with filters.

        If no base filter is provided, it defaults to the one specified in the config.

        Args:
            filterset: Initial filter set. Defaults to None.
            query_filters: Initial query filters. Defaults to None.
        """
        super().__init__(filterset=filterset, query_filters=query_filters)

    def summary(self, dformat: Optional[str] = None) -> Union[pd.DataFrame, str]:
        """Summarize the EcoPlots data.

        Args:
            dformat: The desired format for the summary.
                If "json", returns a JSON string. Defaults to None,
                which returns a Pandas DataFrame.

        Returns:
            A DataFrame containing the summary of the EcoPlots data.
        """
        data = self.summarise_data()
        if dformat == "json":
            return orjson.dumps(data, option=orjson.OPT_INDENT_2).decode("utf-8")

        pairs = {"total_doc": data["total_doc"], **data["unique_count"]}

        return pd.Series(pairs, name="count").rename_axis("metric").reset_index()

    def preview(self, dformat: Optional[str] = None) -> Union[gpd.GeoDataFrame, dict]:
        """Fetch a small, first-page preview of EcoPlots data.

        Args:
            dformat: Output format.
                - If "geojson" or "json", returns a pretty-printed GeoJSON string.
                - If "pandas" or "geopandas" (default), returns a GeoDataFrame.

        Returns:
            Preview data in the requested format.

        Raises:
            ValueError: If an invalid dformat is provided.
        """
        geojson_data = _run_sync(self.fetch_data(page_number=1, page_size=10))
        if dformat == "geojson" or dformat == "json":
            return orjson.dumps(geojson_data, option=orjson.OPT_INDENT_2).decode("utf-8")

        if dformat not in (None, "pandas", "geopandas", "pd", "gpd"):
            raise ValueError(
                "Invalid 'dformat' specified. Supported values are: None, "
                "'pandas' (or 'pd'), 'geopandas' (or 'gpd'), 'geojson', and 'json'."
            )
        return _flatten_geojson(geojson_data)

    def get_datasources(self) -> pd.DataFrame:
        """Get the data sources available for applied filters.

        Returns:
            A DataFrame containing the data sources.
        """
        data = self.discover("dataset")
        return pd.DataFrame(data)

    def get_datasources_attributes(self) -> pd.DataFrame:
        """Get the attributes of data sources from the applied filters.

        Returns:
            A DataFrame containing the attributes of the data sources.
        """
        data = self.discover_attributes("dataset")
        uris = data.get("dataset_attributes", []) or []
        rows = []
        with Cache(CACHE_DIR) as cache:
            ds_map = cache.get("attributes", {}) or {}
            for uri in uris:
                val = ds_map.get(uri)

                row = {"key": val, "uri": uri}

                rows.append(row)

        return pd.DataFrame(rows)

    def get_sites(self) -> pd.DataFrame:
        """Get the sites from the applied filters.

        Returns:
            A DataFrame containing the sites.
        """
        data = self.discover("site_id")
        return pd.DataFrame(data)

    def get_sites_attributes(self) -> pd.DataFrame:
        """Get the attributes of sites from the applied filters.

        Returns:
            A DataFrame containing the attributes of the sites.
        """
        data = self.discover_attributes("site")
        uris = data.get("site_attributes", []) or []
        rows = []
        with Cache(CACHE_DIR) as cache:
            site_map = cache.get("attributes", {}) or {}
            for uri in uris:
                val = site_map.get(uri)

                row = {"key": val, "uri": uri}

                rows.append(row)

        return pd.DataFrame(rows)

    def get_site_visit_attributes(self) -> pd.DataFrame:
        """Get the attributes of site visits from the applied filters.

        Returns:
            A DataFrame containing the attributes of the site visits.
        """
        data = self.discover_attributes("site_visit")
        uris = data.get("site_visit_attributes", []) or []
        rows = []
        with Cache(CACHE_DIR) as cache:
            sv_map = cache.get("attributes", {}) or {}
            for uri in uris:
                val = sv_map.get(uri)

                row = {"key": val, "uri": uri}

                rows.append(row)

        return pd.DataFrame(rows)

    def get_region_types(self) -> pd.DataFrame:
        """Get the available region types from the applied filters.

        Returns:
            A DataFrame containing the region types.
        """
        data = self.discover("region_type")
        return pd.DataFrame(data)

    def get_regions(self, region_type: str) -> pd.DataFrame:
        """Get the available regions for a specific region type from the applied filters.

        Args:
            region_type: The region type to retrieve regions for.

        Returns:
            A DataFrame containing the regions for the specified region type.
        """
        data = self.discover("region", region_type=region_type)
        return pd.DataFrame(data)

    def get_feature_types(self) -> pd.DataFrame:
        """Get the feature types from the applied filters.

        Returns:
            A DataFrame containing the feature types.
        """
        data = self.discover("feature_type")
        return pd.DataFrame(data)

    def get_observed_properties(self) -> pd.DataFrame:
        """Get the observed properties from the applied filters.

        Returns:
            A DataFrame containing the observed properties.
        """
        data = self.discover("observed_property")
        return pd.DataFrame(data)

    def get_observation_attributes(self) -> pd.DataFrame:
        """Get the attributes of observations from the applied filters.

        Returns:
            A DataFrame containing the attributes of the observations.
        """
        data = self.discover_attributes("observation")
        uris = data.get("observation_attributes", []) or []
        rows = []
        with Cache(CACHE_DIR) as cache:  # or self.CACHE_DIR if that's how you store it
            obs_map = cache.get("attributes", {}) or {}
            for uri in uris:
                val = obs_map.get(uri)

                row = {"key": val, "uri": uri}

                rows.append(row)

        return pd.DataFrame(rows)

    def get_data(
        self, allow_full_download: Optional[bool] = False, dformat: Optional[str] = "gpd"
    ) -> gpd.GeoDataFrame:
        """Retrieve EcoPlots data based on the current filters.

        Args:
            allow_full_download: If True, allows downloading the full
                dataset without filters. Defaults to False.
            dformat: Output format.
                - "geojson" or "json": returns a pretty-printed GeoJSON string.
                - "pandas" (or 'pd'): returns a pandas DataFrame.
                - "geopandas" (or 'gpd') (default): returns a GeoDataFrame.

        Raises:
            RuntimeError: If no filters are set and allow_full_download is False.
            ValueError: If an invalid dformat is provided.

        Returns:
            Data in the requested format.
        """
        if not self._query_filters and not allow_full_download:
            raise RuntimeError(
                "No filters specified! Downloading full EcoPlots dataset "
                "can crash your environment. Proceed with caution!\n"
                "If you are sure, call get_data(allow_full_download=True)."
            )

        if dformat in ("geojson", "json"):
            data = _run_sync(self.fetch_data())
            return orjson.dumps(data, option=orjson.OPT_INDENT_2).decode("utf-8")

        if dformat not in ("pandas", "geopandas", "pd", "gpd"):
            raise ValueError(
                "Invalid 'dformat' specified. Supported values are: None, "
                "'pandas' (or 'pd'), 'geopandas' (or 'gpd'), 'geojson', and 'json'."
            )

        feature_types_df = self.get_feature_types()

        if "uri" not in feature_types_df.columns:
            raise RuntimeError("No feature types found; cannot fetch data.")

        uris = feature_types_df["uri"].dropna().astype(str).tolist()

        if not uris:
            # No feature types found, so no data to fetch;
            # return empty gdf
            return gpd.GeoDataFrame()

        dfs = []
        for uri in uris:
            csv_bytes = cast(bytes, _run_sync(self.fetch_data(dformat="csv", feature_type=[uri])))
            df = pd.read_csv(io.StringIO(csv_bytes.decode("utf-8")))
            dfs.append(df)

        aligned_df = _align_and_concat(dfs)

        if dformat in ("pandas", "pd"):
            return aligned_df

        return _to_geopandas(aligned_df)

    def select_spatial(self, **kwargs):
        """Open the spatial selection widget.

        A minimal map based spatial selector, similar to spatial selection tool in
        EcoPlots Portal.

        Args:
            **kwargs: Additional keyword arguments to pass to the widget.

        Returns:
            ipywidgets.VBox: The widget. Use it in a notebook cell to display.
        """
        return spatial_selector(self, **kwargs)


class AsyncEcoPlots(EcoPlots):
    """High-level **async** client for the EcoPlots REST API.

    Provides an awaitable `get_data()` for large/long-running fetches while
    reusing the synchronous ergonomics elsewhere. Ideal for web backends
    (ASGI) or notebooks wanting to parallelise I/O heavy pulls.

    Examples:
        Basic async usage:

        .. code-block:: python

            from terndata.ecoplots import AsyncEcoPlots

            ec = AsyncEcoPlots()
            ec.select(site_id="TCFTNS0002")    # selection etc. is sync but cheap
            gdf = await ec.get_data()          # await the heavy network call

    Notes:
        - Only `get_data()` is async here. Other methods inherited from
          `EcoPlots` are synchronous and **will block**.
        - Safety guard: `get_data()` raises `RuntimeError` when no filters
          are set unless `allow_full_download=True`.
    """

    def __init__(self, filterset: Optional[dict] = None, query_filters: Optional[dict] = None):
        """Initialize the AsyncEcoPlots client with filters.

        If no base filter is provided, it defaults to the one specified in the config.

        Args:
            filterset: Initial filter set. Defaults to None.
            query_filters: Initial query filters. Defaults to None.
        """
        super().__init__(filterset=filterset, query_filters=query_filters)

    async def get_data(
        self, allow_full_download: Optional[bool] = False, dformat: Optional[str] = "gpd"
    ) -> gpd.GeoDataFrame:  # noqa: DAR401
        """Retrieve EcoPlots data asynchronously based on the current filters.

        Args:
            allow_full_download: If True, allows downloading the full
                 dataset without filters. Defaults to False.
            dformat: Output format.
                - "geojson" or "json": returns a pretty-printed GeoJSON string.
                - "pandas" (or "pd"): returns a pandas DataFrame.
                - "geopandas" (or "gpd") (default): returns a GeoDataFrame.

        Raises:
            RuntimeError: If no filters are set and allow_full_download is False.
            ValueError: If an invalid dformat is provided.
            BaseException: Propagated from underlying fetch tasks when data retrieval fails.  #noqa: DAR402

        Returns:
            Data in the requested format.
        """
        if not self._filters and not allow_full_download:
            raise RuntimeError(
                "No filters specified! Downloading full EcoPlots dataset "
                "can crash your environment. Proceed with caution!\n"
                "If you are sure, call get_data(allow_full_download=True)."
            )

        if dformat in ("geojson", "json"):
            data = await self.fetch_data()
            return orjson.dumps(data, option=orjson.OPT_INDENT_2).decode("utf-8")

        if dformat not in (None, "pandas", "geopandas", "pd", "gpd"):
            raise ValueError(
                "Invalid 'dformat' specified. Supported values are: None, "
                "'pandas' (or 'pd'), 'geopandas' (or 'gpd'), 'geojson', and 'json'."
            )

        # for pandas/geopandas output, we request one csv per feature type and merge
        feature_types_df = self.get_feature_types()

        if "uri" not in feature_types_df.columns:
            raise RuntimeError("No feature types found; cannot fetch data.")
        uris = feature_types_df["uri"].dropna().astype(str).tolist()

        if not uris:
            # No feature types found, so no data to fetch;
            # return empty gdf
            return gpd.GeoDataFrame()

        tasks = [self.fetch_data(dformat="csv", feature_type=[uri]) for uri in uris]
        csv_payloads = await asyncio.gather(*tasks, return_exceptions=True)

        dfs = []
        for payload in csv_payloads:
            if isinstance(payload, BaseException):
                raise payload
            csv_bytes = cast(bytes, payload)
            df = pd.read_csv(io.StringIO(csv_bytes.decode("utf-8")))
            dfs.append(df)

        aligned_df = _align_and_concat(dfs)

        if dformat in ("pandas", "pd"):
            return aligned_df

        return _to_geopandas(aligned_df)
