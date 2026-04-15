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
import pandas as pd
from diskcache import Cache

from ._base import EcoPlotsBase
from ._config import CACHE_DIR, MATERIAL_SAMPLE_TYPE_MAP
from ._exceptions import EcoPlotsError
from ._gui import igsn_viewer, sample_image_viewer, spatial_selector
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

    def __init__(
        self,
        filterset: Optional[dict] = None,
        query_filters: Optional[dict] = None,
        mode: Optional[str] = "observations"
    ):
        """Initialise the EcoPlots client.

        All parameters default to empty/``None``; the typical workflow is to
        create the client first and then apply filters via :meth:`select`.

        Args:
            filterset: Initial filter set. Defaults to None.
            query_filters: Initial query filters. Defaults to None.
            mode: The mode of operation. Defaults to "observations".
        """
        super().__init__(filterset=filterset, query_filters=query_filters, mode=mode)

    def summary(self, dformat: Optional[str] = None) -> Union[pd.DataFrame, str]:
        """Summarize the EcoPlots data.

        Args:
            dformat: The desired format for the summary.
                If ``"json"``, returns the raw summary ``dict`` from the API.
                Defaults to ``None``, which returns a :class:`pandas.DataFrame`.

        Returns:
            When *dformat* is ``"json"``, returns the raw summary ``dict``
            from the API. Otherwise, returns a :class:`pandas.DataFrame`
            with columns ``metric`` and ``count`` summarising the current
            selection (e.g. total observations, unique sites, datasets).
        """
        data = self.summarise_data()
        if dformat == "json":
            return data

        if self._mode == "observations":
            pairs = {"observations": data["total_doc"], **data["unique_count"]}
        elif self._mode == "samples":
            pairs = {**data["unique_count"]}

        return pd.Series(pairs, name="count").rename_axis("metric").reset_index()

    def preview(self, dformat: Optional[str] = None) -> Union[gpd.GeoDataFrame, dict, str]:
        """Fetch a small preview of EcoPlots data.  # noqa: DAR401, D415

        Mirrors :meth:`get_data` but limits results to 10 records for a quick look.

        In ``observations`` mode, fetches CSV from up to 2 feature types (5 rows each).
        In ``samples`` mode, calls the samples endpoint and returns the first 10 rows;
        ``"geojson"``/``"json"`` formats are not supported in this mode.

        Args:
            dformat: Output format.
                - ``"geojson"`` or ``"json"``: returns a GeoJSON dict (observations only).
                - ``"pandas"`` (or ``"pd"``): returns a :class:`pandas.DataFrame`.
                - ``"geopandas"`` (or ``"gpd"``) (default): returns a :class:`~geopandas.GeoDataFrame`.

        Returns:
            Preview data in the requested format.

        Raises:
            EcoPlotsError: If an invalid dformat is provided.
            RuntimeError: If no feature types found (observations mode).
        """
        if self._mode == "samples":
            if dformat not in (None, "pandas", "geopandas", "pd", "gpd"):
                raise EcoPlotsError(
                    "In 'samples' mode, supported dformat values are: "
                    "'pandas' (or 'pd') and 'geopandas' (or 'gpd')."
                )
            samples_gdf = cast(gpd.GeoDataFrame, _run_sync(self.fetch_samples_data()))
            samples_gdf = samples_gdf.head(10)
            if dformat in ("pandas", "pd"):
                return pd.DataFrame(samples_gdf)
            return samples_gdf

        if dformat in ("geojson", "json"):
            geojson_data = _run_sync(self.fetch_data(page_number=1, page_size=10))
            return geojson_data

        if dformat not in (None, "pandas", "geopandas", "pd", "gpd"):
            raise EcoPlotsError(
                "Invalid 'dformat' specified. Supported values are: None, "
                "'pandas' (or 'pd'), 'geopandas' (or 'gpd'), 'geojson', and 'json'."
            )

        # Same strategy as get_data(): fetch CSV per feature type, but limit to 2 feature types
        feature_types_df = self.get_feature_types()

        if "uri" not in feature_types_df.columns:
            raise RuntimeError("No feature types found; cannot preview data.")

        uris = feature_types_df["uri"].dropna().astype(str).tolist()

        if not uris:
            # No feature types found, return empty gdf
            return gpd.GeoDataFrame()

        # Limit to first 2 feature types for preview
        preview_uris = uris[:2]

        # Fetch paginated data (10 records per feature type)
        async def _fetch_preview():
            tasks = [
                self.fetch_data(page_number=1, page_size=5, dformat="csv", feature_type=[uri])
                for uri in preview_uris
            ]
            return await asyncio.gather(*tasks, return_exceptions=True)

        csv_payloads = _run_sync(_fetch_preview())

        dfs = []
        for payload in csv_payloads:
            if isinstance(payload, BaseException):
                raise payload
            csv_bytes = cast(bytes, payload)
            df = pd.read_csv(io.StringIO(csv_bytes.decode("utf-8")))
            dfs.append(df)

        aligned_df = _align_and_concat(dfs)

        # Limit preview to 10 records maximum
        if len(aligned_df) > 10:
            aligned_df = aligned_df.head(10)

        if dformat in ("pandas", "pd"):
            return aligned_df

        return _to_geopandas(aligned_df)

    def get_datasources(self) -> pd.DataFrame:
        """Get the data sources available for applied filters.

        Returns:
            A DataFrame containing the data sources.
        """
        if self._mode == "observations":
            data = self.discover("dataset")
        elif self._mode == "samples":
            # Hardcoded value for now until 
            # we have more datasets with samples
            data = self.discover_samples("dataset")
        else:
            raise EcoPlotsError(f"Unsupported mode '{self._mode}' for discovering data sources.")
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
        if self._mode == "samples":
            data = self.discover_samples("site_id")
        else:
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
        if self._mode == "samples":
            # For samples, we have a fixed region type of "plot"
            data = self.discover_samples("region_type")
        else:
            data = self.discover("region_type")
        return pd.DataFrame(data)

    def get_regions(self, region_type: str) -> pd.DataFrame:
        """Get the available regions for a specific region type from the applied filters.

        Args:
            region_type: The region type to retrieve regions for.

        Returns:
            A DataFrame containing the regions for the specified region type.
        """
        if self._mode == "samples":
            # For samples, we have a fixed region type of "plot", so we ignore the input and use "plot"
            data = self.discover_samples("region", region_type=region_type)
        else:
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

    def get_used_procedures(self) -> pd.DataFrame:
        """Get the used procedures available for the current filters.

        Available in both ``observations`` and ``samples`` modes.

        Returns:
            A DataFrame containing the used procedures.
        """
        if self._mode == "samples":
            data = self.discover_samples("used_procedure")
        else:
            data = self.discover("used_procedure")
        return pd.DataFrame(data)

    def get_observation_attributes(self) -> pd.DataFrame:
        """Get the attributes of observations from the applied filters.
        Available only in "observations" mode.

        Returns:
            A DataFrame containing the attributes of the observations.

        Raises:
            EcoPlotsError: If called in a mode other than "observations".
        """
        if self._mode != "observations":
            raise EcoPlotsError("Observation attributes are only available in 'observations' mode.")

        data = self.discover_attributes("observation")
        uris = data.get("observation_attributes", []) or []
        rows = []
        with Cache(CACHE_DIR) as cache:
            obs_map = cache.get("attributes", {}) or {}
            for uri in uris:
                val = obs_map.get(uri)

                row = {"key": val, "uri": uri}

                rows.append(row)

        return pd.DataFrame(rows)
    
    def get_material_sample_types(self) -> pd.DataFrame:
        """Get the material sample types from the applied filters.
        Available only in "samples" mode.

        Returns:
            A DataFrame containing the material sample types.

        Raises:
            EcoPlotsError: If called in a mode other than "samples".
        """
        if self._mode != "samples":
            raise EcoPlotsError("Material sample types are only available in 'samples' mode.")

        data = self.discover_samples("material_sample_type")
        return pd.DataFrame(data)


    def get_sample_igsn(self) -> pd.DataFrame:
        """Get sample names and derived IGSN values.

        Available only in ``samples`` mode. This method discovers
        ``sample_name`` values using the current query filters, then returns
        a DataFrame with:
        - ``sample_name``: sample name with alphabetic characters capitalized.
        - ``igsn``: derived as ``10.60792/{sample_name_raw}``.

        Returns:
            A DataFrame with columns ``sample_name`` and ``igsn``.

        Raises:
            EcoPlotsError: If called in a mode other than ``samples``.
        """
        if self._mode != "samples":
            raise EcoPlotsError("Sample IGSN lookup is only available in 'samples' mode.")

        data = self.discover_samples("sample_name")

        rows = []
        for item in data:
            if not isinstance(item, dict):
                continue
            sample_name_raw = item.get("key")
            if not isinstance(sample_name_raw, str) or not sample_name_raw:
                continue

            sample_name = "".join(
                ch.upper() if ch.isalpha() else ch for ch in sample_name_raw
            )
            rows.append(
                {
                    "sample_name": sample_name,
                    "igsn": f"10.60792/{sample_name_raw}",
                }
            )

        return pd.DataFrame(rows, columns=["sample_name", "igsn"])

    def view_sample_igsn(self, igsn: Optional[str] = None):
        """Open an interactive notebook viewer for sample IGSN DOI pages.

        Available only in ``samples`` mode. This method discovers sample names,
        builds IGSN values, and displays either:
        - a dropdown + iframe widget (default), or
        - a single iframe for a provided IGSN/DOI value.

        Args:
            igsn: Optional IGSN value or DOI URL. Accepted inputs include
                ``10.60792/...``, ``doi.org/10.60792/...``, and
                ``https://doi.org/10.60792/...``.

        Returns:
            ipywidgets.VBox: Interactive IGSN viewer widget.

        Raises:
            EcoPlotsError: If called in a mode other than ``samples``.
            EcoPlotsError: If no material sample type is selected.
        """
        if self._mode != "samples":
            raise EcoPlotsError("IGSN viewer is only available in 'samples' mode.")

        self._ensure_required_material_sample_types(
            list(MATERIAL_SAMPLE_TYPE_MAP.values()),
            "IGSN viewer",
        )

        # FIXME: aggregation requests sent to Elasticsearch with an empty query
        # result in a 404 response. Which is unlike any other discovery facet.
        # The root cause is unknown and needs to be investigated.
        igsn_df = self.get_sample_igsn()
        return igsn_viewer(igsn_df, igsn=igsn)
    
    def get_soil_depth_range(self) -> gpd.GeoDataFrame:
        """Get the soil depth range for the current filters.

        Available only in "samples" mode.

        Returns:
            A GeoDataFrame containing aggregated soil depth range values.

        Raises:
            EcoPlotsError: If called in a mode other than "samples".
            EcoPlotsError: If none of the required material sample types are selected.
        """
        if self._mode != "samples":
            raise EcoPlotsError("Soil depth range is only available in 'samples' mode.")

        self._ensure_required_material_sample_types(
            ["Soil Subsite Sample", "Soil Pit Sample"],
            "Soil depth range",
        )

        return cast(gpd.GeoDataFrame, self.discover_soil_depth_range())

    def get_soilpit(self) -> pd.DataFrame:
        """Get soil pit distribution for the current filters.

        Available only in "samples" mode.

        Returns:
            A DataFrame with two columns: ``soilpit`` and ``counts``.

        Raises:
            EcoPlotsError: If called in a mode other than "samples".
            EcoPlotsError: If none of the required material sample types are selected.
        """
        if self._mode != "samples":
            raise EcoPlotsError("Soil pit distribution is only available in 'samples' mode.")

        self._ensure_required_material_sample_types(
            ["Soil Metagenomic Sample", "Soil Subsite Sample"],
            "Soil pit distribution",
        )

        return cast(pd.DataFrame, self.discover_soilpit())

    def get_speciesname(self) -> pd.DataFrame:
        """Get species name distribution for the current filters.

        Available only in "samples" mode.

        This method preserves all current query filters, including ``has_image``.

        Returns:
            A DataFrame with two columns: ``speciesname`` and ``count``.

        Raises:
            EcoPlotsError: If called in a mode other than "samples".
            EcoPlotsError: If none of the required material sample types are selected.
        """
        if self._mode != "samples":
            raise EcoPlotsError("Species distribution is only available in 'samples' mode.")

        self._ensure_required_material_sample_types(
            ["Plant Tissue Sample", "Plant Voucher Specimen"],
            "Species distribution",
        )

        return cast(pd.DataFrame, self.discover_species())

    def get_data(
        self,
        allow_full_download: Optional[bool] = False,
        dformat: Optional[str] = "gpd",
    ) -> gpd.GeoDataFrame:
        """Retrieve EcoPlots data based on the current filters.

        Args:
            allow_full_download: If True, allows downloading the full
                dataset without filters. Defaults to False.
            dformat: Output format.
                - "geojson" or "json": returns a pretty-printed GeoJSON string.
                - "pandas" (or 'pd'): returns a pandas DataFrame.
                - "geopandas" (or 'gpd') (default): returns a GeoDataFrame.

                In "samples" mode, only "pandas"/"pd" and "geopandas"/"gpd"
                are supported (no "geojson"/"json").

                In "samples" mode, exactly one ``material_sample_type`` must be
                selected at a time.

        Raises:
            RuntimeError: If no filters are set and allow_full_download is False.
            EcoPlotsError: If an invalid dformat is provided.

        Returns:
            Data in the requested format.
        """
        if self._mode == "samples":
            if dformat not in ("pandas", "geopandas", "pd", "gpd"):
                raise EcoPlotsError(
                    "In 'samples' mode, supported dformat values are: "
                    "'pandas' (or 'pd') and 'geopandas' (or 'gpd')."
                )

            samples_gdf = cast(gpd.GeoDataFrame, _run_sync(self.fetch_samples_data()))
            if dformat in ("pandas", "pd"):
                return pd.DataFrame(samples_gdf)
            return samples_gdf

        if not self._query_filters and not allow_full_download:
            raise RuntimeError(
                "No filters specified! Downloading full EcoPlots dataset "
                "can crash your environment. Proceed with caution!\n"
                "If you are sure, call get_data(allow_full_download=True)."
            )

        if dformat in ("geojson", "json"):
            data = _run_sync(self.fetch_data())
            return data

        if dformat not in ("pandas", "geopandas", "pd", "gpd"):
            raise EcoPlotsError(
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

    def view_sample_images(
        self,
        data: Optional[pd.DataFrame] = None,
        image_column: str = "sample_images",
        sample_id_column: str = "sample_id",
        sample_name_column: str = "sample_name",
        scientific_name_column: str = "scientific_name",
    ):
        """Open an interactive notebook image browser for sample images.

        Args:
            data: Optional DataFrame to browse. If omitted, data is fetched in
                samples mode using current filters.
            image_column: Name of image column in dataframe.
            sample_id_column: Name of sample identifier column in dataframe.
            sample_name_column: Name of sample name column in dataframe.
            scientific_name_column: Name of scientific name column in dataframe.

        Returns:
            ipywidgets.VBox: Interactive viewer widget.
        """
        if data is None:
            if self._mode != "samples":
                raise EcoPlotsError("Sample image viewer is only available in 'samples' mode.")

            has_image_selected = bool(self._query_filters.get("has_image", False))
            if not has_image_selected:
                raise EcoPlotsError(
                    "To inspect images, set has_image via select(), for example: "
                    "select(has_image=True)."
                )

            plant_voucher_uri = (
                "http://linked.data.gov.au/def/tern-cv/18317af1-7c83-468d-883e-ba791500c6e3"
            )
            selected_mst = self._query_filters.get("material_sample_type", [])
            if plant_voucher_uri not in selected_mst:
                raise EcoPlotsError(
                    "Image viewer currently requires material_sample_type to be "
                    "'Plant Voucher Specimen'. Please select it before viewing images."
                )

            fetched = self.get_data(dformat="pd")
            if asyncio.iscoroutine(fetched):
                fetched = _run_sync(fetched)
            data = cast(pd.DataFrame, fetched)

        if not isinstance(data, pd.DataFrame):
            data = pd.DataFrame(data)

        return sample_image_viewer(
            data,
            image_column=image_column,
            sample_id_column=sample_id_column,
            sample_name_column=sample_name_column,
            scientific_name_column=scientific_name_column,
        )


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

    def __init__(
        self,
        filterset: Optional[dict] = None,
        query_filters: Optional[dict] = None,
        mode: Optional[str] = "observations"
    ):
        """Initialise the AsyncEcoPlots client.

        All parameters default to empty/``None``; the typical workflow is to
        create the client first and then apply filters via :meth:`select`.

        Args:
            filterset: Initial filter set. Defaults to None.
            query_filters: Initial query filters. Defaults to None.
            mode: The mode of operation. Defaults to "observations".
        """
        super().__init__(filterset=filterset, query_filters=query_filters, mode=mode)

    async def get_data(
        self,
        allow_full_download: Optional[bool] = False,
        dformat: Optional[str] = "gpd",
    ) -> gpd.GeoDataFrame:  # noqa: DAR401
        """Retrieve EcoPlots data asynchronously based on the current filters.

        Args:
            allow_full_download: If True, allows downloading the full
                 dataset without filters. Defaults to False.
            dformat: Output format.
                - "geojson" or "json": returns a pretty-printed GeoJSON string.
                - "pandas" (or "pd"): returns a pandas DataFrame.
                - "geopandas" (or "gpd") (default): returns a GeoDataFrame.

                In "samples" mode, only "pandas"/"pd" and "geopandas"/"gpd"
                are supported (no "geojson"/"json").

                In "samples" mode, exactly one ``material_sample_type`` must be
                selected at a time.

        Raises:
            RuntimeError: If no filters are set and allow_full_download is False.
            EcoPlotsError: If an invalid dformat is provided.
            BaseException: Propagated from underlying fetch tasks when data retrieval fails.  #noqa: DAR402

        Returns:
            Data in the requested format.
        """
        if self._mode == "samples":
            if dformat not in ("pandas", "geopandas", "pd", "gpd"):
                raise EcoPlotsError(
                    "In 'samples' mode, supported dformat values are: "
                    "'pandas' (or 'pd') and 'geopandas' (or 'gpd')."
                )

            samples_gdf = cast(gpd.GeoDataFrame, await self.fetch_samples_data())
            if dformat in ("pandas", "pd"):
                return pd.DataFrame(samples_gdf)
            return samples_gdf

        if not self._filters and not allow_full_download:
            raise RuntimeError(
                "No filters specified! Downloading full EcoPlots dataset "
                "can crash your environment. Proceed with caution!\n"
                "If you are sure, call get_data(allow_full_download=True)."
            )

        if dformat in ("geojson", "json"):
            data = await self.fetch_data()
            return data

        if dformat not in (None, "pandas", "geopandas", "pd", "gpd"):
            raise EcoPlotsError(
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
