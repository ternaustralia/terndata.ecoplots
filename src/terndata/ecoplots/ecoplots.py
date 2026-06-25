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
from pathlib import Path
from typing import Optional, Union, cast

import geopandas as gpd
import orjson
import pandas as pd
from diskcache import Cache

from ._base import EcoPlotsBase
from ._config import CACHE_DIR, MATERIAL_SAMPLE_TYPE_MAP
from ._exceptions import EcoPlotsError
from ._utils import (
    _align_and_concat,
    _run_sync,
    _to_geopandas,
)

_TABLE_FORMATS = ("pandas", "geopandas", "pd", "gpd", "parquet", "pq")
_VECTOR_FILE_SUFFIXES = {".gpkg", ".shp", ".fgb"}
_FILE_FORMAT_SUFFIXES = {
    "pq": {".parquet", ".pq"},
    "pd": {".csv"},
    "geojson": {".json", ".geojson"},
    "gpd": _VECTOR_FILE_SUFFIXES | {".json", ".geojson"},
}


def _dataframe_to_parquet_bytes(df: pd.DataFrame) -> bytes:
    """Serialise a dataframe to parquet bytes."""
    buffer = io.BytesIO()
    try:
        df.to_parquet(buffer, index=False)
    except (ImportError, ValueError) as exc:
        raise EcoPlotsError(
            "Parquet output requires pyarrow. Reinstall or upgrade with: "
            "pip install --upgrade terndata.ecoplots"
        ) from exc
    return buffer.getvalue()


def _csv_bytes_to_dataframe(csv_bytes: bytes) -> pd.DataFrame:
    """Parse CSV bytes into a dataframe, returning empty data for empty payloads."""
    if not csv_bytes or not csv_bytes.strip():
        return pd.DataFrame()
    return pd.read_csv(io.BytesIO(csv_bytes))


def _infer_file_dformat(path: Union[str, Path], mode: str) -> str:
    """Infer a get_data output format from a target file extension."""
    suffix = Path(path).suffix.lower()
    if suffix in (".parquet", ".pq"):
        return "pq"
    if suffix == ".csv":
        return "pd"
    if suffix in (".json", ".geojson"):
        return "geojson" if mode == "observations" else "gpd"
    if suffix in _VECTOR_FILE_SUFFIXES:
        return "gpd"
    raise EcoPlotsError(
        "Could not infer output format from file extension. Supported extensions are: "
        ".parquet, .pq, .csv, .json, .geojson, .gpkg, .shp, and .fgb. "
        "Pass dformat explicitly if needed."
    )


def _normalize_file_dformat(dformat: str) -> str:
    """Normalize user-facing file dformat aliases."""
    normalized = dformat.lower()
    normalized = "pq" if normalized == "parquet" else normalized
    normalized = "pd" if normalized in ("csv", "pandas") else normalized
    normalized = "gpd" if normalized == "geopandas" else normalized
    normalized = "geojson" if normalized == "json" else normalized
    if normalized not in _FILE_FORMAT_SUFFIXES:
        raise EcoPlotsError(
            "Invalid 'dformat' specified. Supported file formats are: "
            "'pandas'/'pd'/'csv', 'geopandas'/'gpd', 'parquet'/'pq', "
            "and 'geojson'/'json'."
        )
    return normalized


def _validate_file_extension(path: Union[str, Path], dformat: str) -> None:
    """Raise if a dformat cannot be written to the target extension."""
    suffix = Path(path).suffix.lower()
    allowed_suffixes = _FILE_FORMAT_SUFFIXES[dformat]
    if suffix not in allowed_suffixes:
        allowed = ", ".join(sorted(allowed_suffixes))
        raise EcoPlotsError(
            f"File extension {suffix!r} is not compatible with dformat={dformat!r}. "
            f"Use one of: {allowed}."
        )


def _write_data_to_file(data, path: Union[str, Path], dformat: str) -> str:
    """Write get_data output to disk and return the resolved path."""
    target = Path(path).expanduser()
    target.parent.mkdir(parents=True, exist_ok=True)
    normalized = _normalize_file_dformat(dformat)
    _validate_file_extension(target, normalized)

    if normalized == "pq":
        if not isinstance(data, (bytes, bytearray)):
            raise EcoPlotsError("Expected parquet bytes for dformat='pq'.")
        target.write_bytes(bytes(data))
    elif normalized == "pd":
        pd.DataFrame(data).to_csv(target, index=False)
    elif normalized == "geojson":
        if isinstance(data, (bytes, bytearray)):
            target.write_bytes(bytes(data))
        elif isinstance(data, str):
            target.write_text(data, encoding="utf-8")
        else:
            target.write_bytes(orjson.dumps(data))
    elif normalized == "gpd":
        if not isinstance(data, gpd.GeoDataFrame):
            data = gpd.GeoDataFrame(data)
        data.to_file(target)

    return str(target.resolve())


def _flatten_region_columns(rows: list[dict]) -> list[dict]:
    """Expand API site ``regions`` mappings into dataframe-ready columns."""
    flattened = []
    for row in rows:
        if not isinstance(row, dict):
            flattened.append(row)
            continue

        item = {key: value for key, value in row.items() if key != "regions"}
        regions = row.get("regions")
        if isinstance(regions, dict):
            item.update(regions)
        flattened.append(item)
    return flattened


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
        mode: Optional[str] = "observations",
        filterset: Optional[dict] = None,
        query_filters: Optional[dict] = None,
    ):
        """Initialise the EcoPlots client.

        All parameters default to empty/``None``; the typical workflow is to
        create the client first and then apply filters via :meth:`select`.

        Args:
            mode: The mode of operation. Defaults to "observations".
            filterset: Initial filter set. Defaults to None.
            query_filters: Initial query filters. Defaults to None.
        """
        if isinstance(mode, dict):
            legacy_filterset = mode
            legacy_query_filters = filterset
            legacy_mode = query_filters if isinstance(query_filters, str) else "observations"
            mode = legacy_mode
            filterset = legacy_filterset
            query_filters = legacy_query_filters

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

    def discover(self, facet: str, **kwargs) -> pd.DataFrame:
        """Discover available values for a facet using the public get_* methods.

        This is a convenience dispatcher for users who prefer a single discovery
        entry point. Existing methods such as :meth:`get_datasources`,
        :meth:`get_sites`, and :meth:`get_feature_types` remain the canonical
        explicit methods.

        Args:
            facet: Discovery facet or common alias, for example ``"dataset"``,
                ``"site_id"``, ``"region"``, or ``"feature_type"``.
            **kwargs: Extra arguments required by specific facets. ``region``
                discovery requires ``region_type``.

        Returns:
            A pandas DataFrame containing discovered values.
        """
        normalized = facet.strip().lower().replace("-", "_").replace(" ", "_")
        method_names = {
            "dataset": "get_datasources",
            "datasource": "get_datasources",
            "datasources": "get_datasources",
            "dataset_attributes": "get_datasources_attributes",
            "datasource_attributes": "get_datasources_attributes",
            "site": "get_sites",
            "site_id": "get_sites",
            "sites": "get_sites",
            "site_attributes": "get_sites_attributes",
            "site_attributes_data": "get_site_attributes_data",
            "site_attribute_data": "get_site_attributes_data",
            "site_visit_attributes": "get_site_visit_attributes",
            "site_visit_attributes_data": "get_site_visit_attributes_data",
            "site_visit_attribute_data": "get_site_visit_attributes_data",
            "region_type": "get_region_types",
            "region_types": "get_region_types",
            "region": "get_regions",
            "regions": "get_regions",
            "feature_type": "get_feature_types",
            "feature_types": "get_feature_types",
            "observed_property": "get_observed_properties",
            "observed_properties": "get_observed_properties",
            "used_procedure": "get_used_procedures",
            "used_procedures": "get_used_procedures",
            "observation_attributes": "get_observation_attributes",
            "material_sample_type": "get_material_sample_types",
            "material_sample_types": "get_material_sample_types",
            "sample_igsn": "get_sample_igsn",
            "soil_depth_range": "get_soil_depth_range",
            "soilpit": "get_soilpit",
            "speciesname": "get_speciesname",
        }
        method_name = method_names.get(normalized)
        if not method_name:
            raise EcoPlotsError(f"Invalid discovery facet: {facet}")

        method = getattr(self, method_name)
        if method_name == "get_regions":
            region_type = kwargs.get("region_type")
            if not region_type:
                raise EcoPlotsError("region_type is required when discovering regions")
            return method(region_type=region_type)
        if method_name == "get_sites":
            return method(include_region=kwargs.get("include_region", False))
        return method()

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
            samples_gdf = cast(gpd.GeoDataFrame, self.fetch_samples_data_sync())
            samples_gdf = samples_gdf.head(10)
            if dformat in ("pandas", "pd"):
                return pd.DataFrame(samples_gdf)
            return samples_gdf

        if dformat in ("geojson", "json"):
            geojson_data = self.fetch_data_sync(page_number=1, page_size=10)
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

        csv_payloads = [
            self.fetch_data_sync(
                page_number=1,
                page_size=5,
                dformat="csv",
                feature_type=[uri],
            )
            for uri in preview_uris
        ]

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
            data = super().discover("dataset")
        elif self._mode == "samples":
            # Hardcoded value for now until
            # we have more datasets with samples
            data = super().discover_samples("dataset")
        else:
            raise EcoPlotsError(f"Unsupported mode '{self._mode}' for discovering data sources.")
        return pd.DataFrame(data)

    def get_datasources_attributes(self) -> pd.DataFrame:
        """Get the attributes of data sources from the applied filters.

        Returns:
            A DataFrame containing the attributes of the data sources.
        """
        data = super().discover_attributes("dataset")
        uris = data.get("dataset_attributes", []) or []
        rows = []
        with Cache(CACHE_DIR) as cache:
            ds_map = cache.get("attributes", {}) or {}
            for uri in uris:
                val = ds_map.get(uri)

                row = {"key": val, "uri": uri}

                rows.append(row)

        return pd.DataFrame(rows)

    def get_sites(self, include_region: bool = False) -> pd.DataFrame:
        """Get the sites from the applied filters.

        Args:
            include_region: When True, site discovery includes region membership
                metadata. Observation mode sends ``include-regions=True`` to
                the standard site discovery endpoint. Samples mode first
                discovers sample site buckets, then calls
                ``/discovery/site_id?include-regions=True`` with a temporary
                query containing only those site URLs. The API returns region
                types in a nested ``regions`` mapping; this method flattens
                that mapping so region types become DataFrame columns, for
                example ``IBRA7 Bioregions`` and ``States and Territories``.

        Returns:
            A DataFrame containing the sites.

        Examples:
            .. code-block:: python

                ec = EcoPlots()
                sites = ec.get_sites()
                sites_with_regions = ec.get_sites(include_region=True)

                samples = EcoPlots("samples")
                samples.select(material_sample_type="Soil Pit Sample")
                sample_sites = samples.get_sites(include_region=True)
        """
        if self._mode == "samples":
            data = super().discover_samples("site_id")
            if include_region:
                site_urls = [
                    item.get("uri") for item in data if isinstance(item, dict) and item.get("uri")
                ]
                if not site_urls:
                    return pd.DataFrame()
                data = super().discover(
                    "site_id",
                    include_region=True,
                    query_filters={"site_id": site_urls},
                )
                if isinstance(data, list):
                    data = _flatten_region_columns(data)
        else:
            data = super().discover("site_id", include_region=include_region)
            if include_region and isinstance(data, list):
                data = _flatten_region_columns(data)
        return pd.DataFrame(data)

    def get_site_attributes(self) -> pd.DataFrame:
        """Get the attributes of sites from the applied filters.

        Returns:
            A DataFrame containing the attributes of the sites.
        """
        data = super().discover_attributes("site")
        uris = data.get("site_attributes", []) or []
        rows = []
        with Cache(CACHE_DIR) as cache:
            site_map = cache.get("attributes", {}) or {}
            for uri in uris:
                val = site_map.get(uri)

                row = {"key": val, "uri": uri}

                rows.append(row)

        return pd.DataFrame(rows)

    def get_sites_attributes(self) -> pd.DataFrame:
        """Alias for :meth:`get_site_attributes`."""
        return self.get_site_attributes()

    def get_site_attributes_data(self) -> pd.DataFrame:
        """Get site attribute values for the current filters.

        Calls ``/data/attributes/site`` with ``dformat=csv`` as a URL
        parameter and the current query filters in the request body. The API
        returns one CSV row per site, with columns such as ``id``, ``label``,
        ``wkt_point``, ``bioregion``, ``locationName``, ``plotArea``, and
        ``siteNote``.

        Returns:
            A DataFrame containing site attribute values for the current filters.

        Examples:
            .. code-block:: python

                ec = EcoPlots()
                ec.select(site_id="TCFTNS0002")
                site_attributes = ec.get_site_attributes_data()
        """
        csv_bytes = super().fetch_attributes_data("site")
        return _csv_bytes_to_dataframe(csv_bytes)

    def get_site_visit_attributes(self) -> pd.DataFrame:
        """Get the attributes of site visits from the applied filters.

        Returns:
            A DataFrame containing the attributes of the site visits.
        """
        data = super().discover_attributes("site_visit")
        uris = data.get("site_visit_attributes", []) or []
        rows = []
        with Cache(CACHE_DIR) as cache:
            sv_map = cache.get("attributes", {}) or {}
            for uri in uris:
                val = sv_map.get(uri)

                row = {"key": val, "uri": uri}

                rows.append(row)

        return pd.DataFrame(rows)

    def get_site_visit_attributes_data(self) -> pd.DataFrame:
        """Get site-visit attribute values for the current filters.

        Calls ``/data/attributes/site-visit`` with ``dformat=csv`` as a URL
        parameter and the current query filters in the request body. The API
        returns CSV data for site visits matching the current filters.

        Returns:
            A DataFrame containing site-visit attribute values for the current
            filters.

        Examples:
            .. code-block:: python

                ec = EcoPlots()
                ec.select(site_id="TCFTNS0002")
                site_visit_attributes = ec.get_site_visit_attributes_data()
        """
        csv_bytes = super().fetch_attributes_data("site_visit")
        return _csv_bytes_to_dataframe(csv_bytes)

    def get_region_types(self) -> pd.DataFrame:
        """Get the available region types from the applied filters.

        Returns:
            A DataFrame containing the region types.
        """
        if self._mode == "samples":
            # For samples, we have a fixed region type of "plot"
            data = super().discover_samples("region_type")
        else:
            data = super().discover("region_type")
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
            data = super().discover_samples("region", region_type=region_type)
        else:
            data = super().discover("region", region_type=region_type)
        return pd.DataFrame(data)

    def get_feature_types(self) -> pd.DataFrame:
        """Get the feature types from the applied filters.

        Returns:
            A DataFrame containing the feature types.
        """
        data = super().discover("feature_type")
        return pd.DataFrame(data)

    def get_observed_properties(self) -> pd.DataFrame:
        """Get the observed properties from the applied filters.

        Returns:
            A DataFrame containing the observed properties.
        """
        data = super().discover("observed_property")
        return pd.DataFrame(data)

    def get_used_procedures(self) -> pd.DataFrame:
        """Get the used procedures available for the current filters.

        Available in both ``observations`` and ``samples`` modes.

        Returns:
            A DataFrame containing the used procedures.
        """
        if self._mode == "samples":
            data = super().discover_samples("used_procedure")
        else:
            data = super().discover("used_procedure")
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

        data = super().discover_attributes("observation")
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

        data = super().discover_samples("material_sample_type")
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

        data = super().discover_samples("sample_name")

        rows = []
        for item in data:
            if not isinstance(item, dict):
                continue
            sample_name_raw = item.get("key")
            if not isinstance(sample_name_raw, str) or not sample_name_raw:
                continue

            sample_name = "".join(ch.upper() if ch.isalpha() else ch for ch in sample_name_raw)
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
        from ._gui import igsn_viewer

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
            dformat: Output format. Use ``"geopandas"``/``"gpd"`` for a
                GeoDataFrame (default), ``"pandas"``/``"pd"`` for a DataFrame,
                ``"parquet"``/``"pq"`` for Parquet bytes, or
                ``"geojson"``/``"json"`` for GeoJSON in observations mode.
                In ``samples`` mode, ``"geojson"``/``"json"`` are not
                supported and exactly one ``material_sample_type`` must be
                selected before retrieval.

        Examples:
            Observations:

            .. code-block:: python

                ec = EcoPlots()
                ec.select(site_id="TCFTNS0002")
                gdf = ec.get_data()
                df = ec.get_data(dformat="pd")
                parquet_bytes = ec.get_data(dformat="pq")
                geojson = ec.get_data(dformat="geojson")

            Samples:

            .. code-block:: python

                ec = EcoPlots("samples")
                ec.select(material_sample_type="Plant Voucher Specimen")
                gdf = ec.get_data()
                parquet_bytes = ec.get_data(dformat="parquet")

        Raises:
            RuntimeError: If no filters are set and allow_full_download is False.
            EcoPlotsError: If an invalid dformat is provided.

        Returns:
            Data in the requested format.
        """
        if self._mode == "samples":
            if dformat not in _TABLE_FORMATS:
                raise EcoPlotsError(
                    "In 'samples' mode, supported dformat values are: "
                    "'pandas' (or 'pd'), 'geopandas' (or 'gpd'), and 'parquet' (or 'pq')."
                )

            samples_gdf = cast(gpd.GeoDataFrame, self.fetch_samples_data_sync())
            if dformat in ("parquet", "pq"):
                return _dataframe_to_parquet_bytes(pd.DataFrame(samples_gdf))
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
            data = self.fetch_data_sync()
            return data

        if dformat not in _TABLE_FORMATS:
            raise EcoPlotsError(
                "Invalid 'dformat' specified. Supported values are: None, "
                "'pandas' (or 'pd'), 'geopandas' (or 'gpd'), 'geojson', "
                "'json', and 'parquet' (or 'pq')."
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
            csv_bytes = cast(bytes, self.fetch_data_sync(dformat="csv", feature_type=[uri]))
            df = pd.read_csv(io.StringIO(csv_bytes.decode("utf-8")))
            dfs.append(df)

        aligned_df = _align_and_concat(dfs)

        if dformat in ("pandas", "pd"):
            return aligned_df
        if dformat in ("parquet", "pq"):
            return _dataframe_to_parquet_bytes(aligned_df)

        return _to_geopandas(aligned_df)

    def export_data(
        self,
        path: Union[str, Path],
        dformat: Optional[str] = None,
        allow_full_download: Optional[bool] = False,
    ) -> str:
        """Retrieve current data and export it to a file.

        The output format is inferred from ``path`` when ``dformat`` is not
        provided. Supported extensions are ``.parquet``/``.pq``, ``.csv``,
        ``.json``/``.geojson``, ``.gpkg``, ``.shp``, and ``.fgb``. Parent
        directories are created automatically.

        Args:
            path: Destination file path.
            dformat: Optional explicit output format. Use ``"pq"``/``"parquet"``,
                ``"pd"``/``"pandas"``, ``"gpd"``/``"geopandas"``, or
                ``"geojson"``/``"json"``.
            allow_full_download: If True, allows downloading the full dataset
                without filters. Defaults to False.

        Returns:
            Absolute path to the written file.

        Examples:
            .. code-block:: python

                ec = EcoPlots()
                ec.select(site_id="TCFTNS0002")

                ec.export_data("data/ecoplots.parquet")
                ec.export_data("data/ecoplots.csv")
                ec.export_data("data/ecoplots.geojson")

                EcoPlots("samples").select(
                    material_sample_type="Plant Voucher Specimen"
                ).export_data("samples.parquet")
        """
        target_dformat = dformat or _infer_file_dformat(path, self._mode)
        normalized = _normalize_file_dformat(target_dformat)
        _validate_file_extension(path, normalized)

        data = self.get_data(
            allow_full_download=allow_full_download,
            dformat=normalized,
        )
        return _write_data_to_file(data, path, normalized)

    def select_spatial(self, **kwargs):
        """Open the spatial selection widget.

        A minimal map based spatial selector, similar to spatial selection tool in
        EcoPlots Portal.

        Args:
            **kwargs: Additional keyword arguments to pass to the widget.

        Returns:
            ipywidgets.VBox: The widget. Use it in a notebook cell to display.
        """
        from ._gui import spatial_selector

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
        from ._gui import sample_image_viewer

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
        mode: Optional[str] = "observations",
        filterset: Optional[dict] = None,
        query_filters: Optional[dict] = None,
    ):
        """Initialise the AsyncEcoPlots client.

        All parameters default to empty/``None``; the typical workflow is to
        create the client first and then apply filters via :meth:`select`.

        Args:
            mode: The mode of operation. Defaults to "observations".
            filterset: Initial filter set. Defaults to None.
            query_filters: Initial query filters. Defaults to None.
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
            dformat: Output format. Use ``"geopandas"``/``"gpd"`` for a
                GeoDataFrame (default), ``"pandas"``/``"pd"`` for a DataFrame,
                ``"parquet"``/``"pq"`` for Parquet bytes, or
                ``"geojson"``/``"json"`` for GeoJSON in observations mode.
                In ``samples`` mode, ``"geojson"``/``"json"`` are not
                supported and exactly one ``material_sample_type`` must be
                selected before retrieval.

        Examples:
            .. code-block:: python

                ec = AsyncEcoPlots()
                ec.select(site_id="TCFTNS0002")
                gdf = await ec.get_data()
                parquet_bytes = await ec.get_data(dformat="pq")

        Raises:
            RuntimeError: If no filters are set and allow_full_download is False.
            EcoPlotsError: If an invalid dformat is provided.
            BaseException: Propagated from underlying fetch tasks when data retrieval fails.  #noqa: DAR402

        Returns:
            Data in the requested format.
        """
        if self._mode == "samples":
            if dformat not in _TABLE_FORMATS:
                raise EcoPlotsError(
                    "In 'samples' mode, supported dformat values are: "
                    "'pandas' (or 'pd'), 'geopandas' (or 'gpd'), and 'parquet' (or 'pq')."
                )

            samples_gdf = cast(gpd.GeoDataFrame, await self.fetch_samples_data())
            if dformat in ("parquet", "pq"):
                return _dataframe_to_parquet_bytes(pd.DataFrame(samples_gdf))
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

        if dformat not in (None, *_TABLE_FORMATS):
            raise EcoPlotsError(
                "Invalid 'dformat' specified. Supported values are: None, "
                "'pandas' (or 'pd'), 'geopandas' (or 'gpd'), 'geojson', "
                "'json', and 'parquet' (or 'pq')."
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

        dfs = []
        tasks = [
            asyncio.create_task(self.fetch_data(dformat="csv", feature_type=[uri])) for uri in uris
        ]
        for task in asyncio.as_completed(tasks):
            payload = await task
            csv_bytes = cast(bytes, payload)
            df = pd.read_csv(io.StringIO(csv_bytes.decode("utf-8")))
            dfs.append(df)

        aligned_df = _align_and_concat(dfs)

        if dformat in ("pandas", "pd"):
            return aligned_df
        if dformat in ("parquet", "pq"):
            return _dataframe_to_parquet_bytes(aligned_df)

        return _to_geopandas(aligned_df)

    async def get_data_stream(
        self,
        allow_full_download: Optional[bool] = False,
        dformat: str = "gpd",
    ):
        """Stream EcoPlots data asynchronously in the selected format.

        Args:
            allow_full_download: If True, allows downloading without filters.
            dformat: Stream output format. Supported values are:
                - ``"geojson"`` or ``"json"``: yields decoded GeoJSON stream lines.
                - ``"geopandas"`` or ``"gpd"``: yields one GeoDataFrame per feature type.
                - ``"parquet"`` or ``"pq"``: yields one parquet byte payload per feature type.

        Yields:
            GeoJSON lines, GeoDataFrames, or parquet bytes depending on
            ``dformat``.

        Examples:
            .. code-block:: python

                ec = AsyncEcoPlots()
                ec.select(site_id="TCFTNS0002")

                async for gdf_chunk in ec.get_data_stream(dformat="gpd"):
                    ...

                async for parquet_chunk in ec.get_data_stream(dformat="pq"):
                    ...

                async for geojson_line in ec.get_data_stream(dformat="geojson"):
                    ...
        """
        if self._mode == "samples":
            if dformat in ("geojson", "json"):
                raise EcoPlotsError(
                    "In 'samples' mode, streaming supports 'geopandas'/'gpd' " "and 'parquet'/'pq'."
                )
            data = await self.get_data(
                allow_full_download=allow_full_download,
                dformat=dformat,
            )
            yield data
            return

        if not self._filters and not allow_full_download:
            raise RuntimeError(
                "No filters specified! Downloading full EcoPlots dataset "
                "can crash your environment. Proceed with caution!\n"
                "If you are sure, call get_data_stream(allow_full_download=True)."
            )

        if dformat in ("geojson", "json"):
            async for line in self.iter_fetch_data_lines(dformat="geojson"):
                yield line
            return

        if dformat not in ("geopandas", "gpd", "parquet", "pq"):
            raise EcoPlotsError(
                "Invalid 'dformat' specified. Supported streaming values are: "
                "'geopandas' (or 'gpd'), 'parquet' (or 'pq'), and 'geojson'/'json'."
            )

        feature_types_df = self.get_feature_types()

        if "uri" not in feature_types_df.columns:
            raise RuntimeError("No feature types found; cannot fetch data.")

        uris = feature_types_df["uri"].dropna().astype(str).tolist()
        if not uris:
            return

        tasks = [
            asyncio.create_task(self.fetch_data(dformat="csv", feature_type=[uri])) for uri in uris
        ]
        for task in asyncio.as_completed(tasks):
            csv_bytes = cast(bytes, await task)
            df = pd.read_csv(io.StringIO(csv_bytes.decode("utf-8")))
            if dformat in ("parquet", "pq"):
                yield _dataframe_to_parquet_bytes(df)
            else:
                yield _to_geopandas(df)

    async def export_data(
        self,
        path: Union[str, Path],
        dformat: Optional[str] = None,
        allow_full_download: Optional[bool] = False,
    ) -> str:
        """Asynchronously retrieve current data and export it to a file.

        Args:
            path: Destination file path.
            dformat: Optional explicit output format. When omitted, the format
                is inferred from the file extension.
            allow_full_download: If True, allows downloading the full dataset
                without filters. Defaults to False.

        Returns:
            Absolute path to the written file.

        Examples:
            .. code-block:: python

                ec = AsyncEcoPlots()
                ec.select(site_id="TCFTNS0002")

                await ec.export_data("data/ecoplots.parquet")
                await ec.export_data("data/ecoplots.csv")
        """
        target_dformat = dformat or _infer_file_dformat(path, self._mode)
        normalized = _normalize_file_dformat(target_dformat)
        _validate_file_extension(path, normalized)

        data = await self.get_data(
            allow_full_download=allow_full_download,
            dformat=normalized,
        )
        return _write_data_to_file(data, path, normalized)
