import asyncio
import orjson
import pandas as pd
import geopandas as gpd

from diskcache import Cache
from typing import Optional, Dict, List, Union
from terndata.ecoplots._base import EcoPlotsBase
from terndata.ecoplots.config import QUERY_FACETS, CACHE_DIR
from terndata.ecoplots.utils import run_sync
from terndata.ecoplots.nlp_utils import resolve_facet
from terndata.ecoplots._flatten_response._streaming import _flatten_geojson

class EcoPlots(EcoPlotsBase):
    """
    A class to interact with the EcoPlots API.
    """

    def __init__(self, filterset: Optional[Dict] = None, query_filters: Optional[Dict] = None):
        """
        Initialize the EcoPlots client with a base URL.
        If no base URL is provided, it defaults to the one specified in the config.
        """
        super().__init__(
            filterset=filterset,
            query_filters=query_filters
        )

    
    def summary(self, dformat: Optional[str] = None) -> pd.DataFrame:
        data = run_sync(self.summarise_data())
        if dformat == "json":
            return orjson.dumps(data, option=orjson.OPT_INDENT_2)
        
        pairs = {"total_doc": data["total_doc"], **data["unique_count"]}
        df = (
            pd.Series(pairs, name="count")
                .rename_axis("metric")
                .reset_index()
        )
        return df

    
    def select(self, filters: Optional[Dict] = None, **kwargs):
        """
        Add filters to the current EcoPlots instance.
        
        - Accepts dict: ecoplots.select({"site_id": [...], "dataset": [...]})
        - Accepts kwargs: ecoplots.select(site_id=[...], dataset=[...])
        - Returns self to allow chaining
        """
        print(f"Current filters: {self._filters}")  # Debugging output
        # Merge filters from dict and kwargs
        input_filters = {}

        if filters:
            input_filters.update(filters)
        if kwargs:
            input_filters.update(kwargs)

        # 1. Validate allowed keys
        invalid_keys = set(input_filters) - set(QUERY_FACETS)
        if invalid_keys:
            raise ValueError(f"Invalid filter keys: {invalid_keys}. Allowed: {QUERY_FACETS}")

        # 2. Validate region logic
        if "region" in input_filters:
            region_type_now = "region_type" in input_filters
            region_type_before = "region_type" in self._filters
            if not (region_type_now or region_type_before):
                raise ValueError("'region_type' must be provided before or with 'region'.")

        # 3. Merge filters (always as list)
        for k, v in input_filters.items():
            if v is None:
                continue
            if not isinstance(v, (list, tuple)):
                v = [v]
            if k in self._filters:
                self._filters[k].extend(list(v))
            else:
                self._filters[k] = list(v)

        # 4. Validate filters
        self._validate_filters()

        print(f"Filters updated: {self._filters}")  # Debugging output

        return self


    def get_filter(self, facet: Optional[str] = None) -> Union[List, Dict]:
        """
        Get the current filter values for a specific facet.
        Returns None if the facet is not set.
        """
        if facet:
            facet_val = resolve_facet(facet, QUERY_FACETS)
            if facet_val:
                return self._filters.get(facet_val)
            else:
                raise ValueError(
                    f"Invalid facet name `{facet}`. Allowed facets: " + ", ".join(QUERY_FACETS)
                )
        
        return orjson.dumps(self._filters, option=orjson.OPT_INDENT_2)


    def get_query_filters(self, facet: str = None) -> Union[List, Dict]:
        """
        Get the current query filters.
        Returns a dictionary of filters.
        """
        if facet:
            facet_val = resolve_facet(facet, QUERY_FACETS)
            if facet_val:
                return self._query_filters.get(facet_val)
            else:
                raise ValueError(
                    f"Invalid facet name `{facet}`. Allowed facets: " + ", ".join(QUERY_FACETS)
                )

        return orjson.dumps(self._query_filters, option=orjson.OPT_INDENT_2)


    def preview(self, dformat: Optional[str] = "geopandas") -> gpd.GeoDataFrame:
        geojson_data = run_sync(self.fetch_data(page_number=1, page_size=10))
        if dformat == "geojson":
            return orjson.dumps(geojson_data, option=orjson.OPT_INDENT_2)
        # return gpd.GeoDataFrame.from_features(geojson_data["features"])
        return _flatten_geojson(geojson_data)


    def get_datasources(self) -> pd.DataFrame:
        data = run_sync(self.discover("dataset"))
        return pd.DataFrame(data)
    

    def get_datasources_attributes(self):
        data = run_sync(self.discover_attributes("dataset"))
        uris = data.get("dataset_attributes", []) or []
        rows = []
        with Cache(CACHE_DIR) as cache:
            ds_map = cache.get("attributes", {}) or {}
            for uri in uris:
                val = ds_map.get(uri, None)

                row = {"key": val, "uri": uri}

                rows.append(row)

        return pd.DataFrame(rows)


    def get_sites(self) -> pd.DataFrame:
        data = run_sync(self.discover("site_id"))
        return pd.DataFrame(data)
    
    
    def get_sites_attributes(self):
        data = run_sync(self.discover_attributes("site"))
        uris = data.get("site_attributes", []) or []
        rows = []
        with Cache(CACHE_DIR) as cache:
            site_map = cache.get("attributes", {}) or {}
            for uri in uris:
                val = site_map.get(uri, None)

                row = {"key": val, "uri": uri}

                rows.append(row)

        return pd.DataFrame(rows)
    

    def get_site_visit_attributes(self):
        data = run_sync(self.discover_attributes("site_visit"))
        uris = data.get("site_visit_attributes", []) or []
        rows = []
        with Cache(CACHE_DIR) as cache:
            sv_map = cache.get("attributes", {}) or {}
            for uri in uris:
                val = sv_map.get(uri, None)

                row = {"key": val, "uri": uri}

                rows.append(row)

        return pd.DataFrame(rows)


    def get_region_types(self) -> pd.DataFrame:
        """
        Get the available region types from the EcoPlots API.
        """
        data = run_sync(self.discover("region_type"))
        return pd.DataFrame(data)
    

    def get_regions(self, region_type: str) -> pd.DataFrame:
        """
        Get the available regions for a specific region type from the EcoPlots API.
        """
        data = run_sync(self.discover("region", region_type=region_type))
        return pd.DataFrame(data)


    def get_feature_types(self) -> pd.DataFrame:
        data = run_sync(self.discover("feature_type"))
        return pd.DataFrame(data)
    

    def get_observed_properties(self) -> pd.DataFrame:
        data = run_sync(self.discover("observed_property"))
        return pd.DataFrame(data)
    

    def get_observation_attributes(self):
        data = run_sync(self.discover_attributes("observation"))
        uris = data.get("observation_attributes", []) or []
        rows = []
        with Cache(CACHE_DIR) as cache:   # or self.CACHE_DIR if that's how you store it
            obs_map = cache.get("attributes", {}) or {}
            for uri in uris:
                val = obs_map.get(uri, None)

                row = {"key": val, "uri": uri}

                rows.append(row)

        return pd.DataFrame(rows)
    

    def get_data(
        self,
        allow_full_download: Optional[bool] = False,
        dformat: Optional[str] = "pandas"
    ) -> gpd.GeoDataFrame:
        """
        Get data from the EcoPlots API based on the current filters.
        """
        if not self._query_filters and not allow_full_download:
            raise RuntimeError(
                "No filters specified! Downloading full EcoPlots dataset "
                "can crash your environment. Proceed with caution!\n"
                "If you are sure, call get_data(allow_full_download=True)."
            )
        data = run_sync(self.fetch_data())

        if dformat == "geojson":
            return orjson.dumps(data, option=orjson.OPT_INDENT_2)
        
        flattened_data = _flatten_geojson(data)
        
        return flattened_data



class AsyncEcoPlots(EcoPlots):
    """
    A class to interact with the EcoPlots API.
    """

    def __init__(self, filterset: Optional[Dict] = None, query_filters: Optional[Dict] = None):
        """
        Initialize the EcoPlots client with a base URL.
        If no base URL is provided, it defaults to the one specified in the config.
        """
        super().__init__(
            filterset=filterset,
            query_filters=query_filters
        )

    
    async def summary(self, dformat: Optional[str] = None) -> pd.DataFrame:
        data = await self.summarise_data()
        if dformat == "json":
            return orjson.dumps(data, option=orjson.OPT_INDENT_2)
        
        pairs = {"total_doc": data["total_doc"], **data["unique_count"]}
        df = (
            pd.Series(pairs, name="count")
                .rename_axis("metric")
                .reset_index()
        )
        return df

    
    async def preview(self) -> gpd.GeoDataFrame:
        """
        Asynchronous method to get a summary of the EcoPlots data.
        """
        data = await self.fetch_data(page_number=1, page_size=10)
        gdf = await asyncio.to_thread(gpd.GeoDataFrame.from_features, data["features"])
        return gdf

    
    async def get_datasources(self) -> pd.DataFrame:
        """
        Asynchronous method to get the data sources from the EcoPlots API.
        """
        data =  await self.discover("dataset")
        # Convert to DataFrame in a thread-safe manner, keeping method non-blocking
        df = await asyncio.to_thread(pd.DataFrame, data)
        return df
    

    async def get_datasources_attributes(self):
        """
        Asynchronous method to get the attributes of data sources from the EcoPlots API.
        """
        data = await self.discover_attributes("dataset")
        uris = data.get("dataset_attributes", []) or []
        rows = []
        with Cache(CACHE_DIR) as cache:   # or self.CACHE_DIR if that's how you store it
            ds_map = cache.get("attributes", {}) or {}
            for uri in uris:
                val = ds_map.get(uri, None)

                row = {"key": val, "uri": uri}

                rows.append(row)
        
        df = await asyncio.to_thread(pd.DataFrame, rows)
        return df
    

    async def get_sites(self) -> pd.DataFrame:
        """
        Asynchronous method to get the sites from the EcoPlots API.
        """
        data = await self.discover("site_id")
        df = await asyncio.to_thread(pd.DataFrame, data)
        return df
    
    
    async def get_sites_attributes(self):
        """
        Asynchronous method to get the attributes of sites from the EcoPlots API.
        """
        data = await self.discover_attributes("site")
        uris = data.get("site_attributes", []) or []
        rows = []
        with Cache(CACHE_DIR) as cache:   # or self.CACHE_DIR if that's how you store it
            site_map = cache.get("attributes", {}) or {}
            for uri in uris:
                val = site_map.get(uri, None)

                row = {"key": val, "uri": uri}

                rows.append(row)

        return pd.DataFrame(rows)
    

    async def get_site_visit_attributes(self):
        """
        Asynchronous method to get the attributes of site visits from the EcoPlots API.
        """
        data = await self.discover_attributes("site_visit")
        uris = data.get("site_visit_attributes", []) or []
        rows = []
        with Cache(CACHE_DIR) as cache:   # or self.CACHE_DIR if that's how you store it
            sv_map = cache.get("attributes", {}) or {}
            for uri in uris:
                val = sv_map.get(uri, None)

                row = {"key": val, "uri": uri}

                rows.append(row)

        df = await asyncio.to_thread(pd.DataFrame, rows)
        return df
    

    async def get_region_types(self) -> pd.DataFrame:
        """
        Asynchronous method to get the available region types from the EcoPlots API.
        """
        data = await self.discover("region_type")
        df = await asyncio.to_thread(pd.DataFrame, data)
        return df
    

    async def get_regions(self, region_type: str) -> pd.DataFrame:
        """
        Asynchronous method to get the available regions for a specific region type from the EcoPlots API.
        """
        data = await self.discover("region", region_type=region_type)
        df = await asyncio.to_thread(pd.DataFrame, data)
        return df


    async def get_feature_types(self) -> pd.DataFrame:
        """
        Asynchronous method to get the feature types from the EcoPlots API.
        """
        data = await self.discover("feature_type")
        df = await asyncio.to_thread(pd.DataFrame, data)
        return df
    

    async def get_observed_propertiesc(self) -> pd.DataFrame:
        """
        Asynchronous method to get the observed properties from the EcoPlots API.
        """
        data = await self.discover("observed_property")
        df = await asyncio.to_thread(pd.DataFrame, data)
        return df
    

    async def get_observation_attributes(self):
        """
        Asynchronous method to get the attributes of observations from the EcoPlots API.
        """
        data = await self.discover_attributes("observation")
        uris = data.get("observation_attributes", []) or []
        rows = []
        with Cache(CACHE_DIR) as cache:
            obs_map = cache.get("attributes", {}) or {}
            for uri in uris:
                val = obs_map.get(uri, None)

                row = {"key": val, "uri": uri}

                rows.append(row)

        df = await asyncio.to_thread(pd.DataFrame, rows)
        return df


    async def get_data(
        self,
        allow_full_download: Optional[bool] = False,
        dformat: Optional[str] = "pandas"
    ) -> gpd.GeoDataFrame:
        """
        Asynchronous method to get data from the EcoPlots API based on the current filters.
        """
        if not self._filters and not allow_full_download:
            raise RuntimeError(
                "No filters specified! Downloading full EcoPlots dataset "
                "can crash your environment. Proceed with caution!\n"
                "If you are sure, call get_data(allow_full_download=True)."
            )
        data = await self.fetch_data()

        if dformat == "geojson":
            return orjson.dumps(data, option=orjson.OPT_INDENT_2)

        gdf = asyncio.to_thread(_flatten_geojson, data)
        return gdf