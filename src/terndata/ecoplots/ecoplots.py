import asyncio
import orjson
import pandas as pd
import geopandas as gpd

from typing import Optional, Dict
from terndata.ecoplots.api_calls import _EcoPlotsAPI
from terndata.ecoplots.config import API_BASE_URL
from terndata.ecoplots.utils import run_sync


class EcoPlots:
    """
    A class to interact with the EcoPlots API.
    """

    def __init__(self, filterset: Optional[Dict] = None):
        """
        Initialize the EcoPlots client with a base URL.
        If no base URL is provided, it defaults to the one specified in the config.
        """
        self._ecoplots_api = _EcoPlotsAPI(base_url=API_BASE_URL)
        self.filters = filterset or {} # EcoPlots serch query filters


    def select(self, **filters):
        """
        Set filters for the EcoPlots API calls.
        """
        self.filters.update(filters)

    
    def save_filterset(self, filepath: Optional[str] = None):
        """
        Save a filterset for later use.
        """
        path = filepath or "ecoplots_filterset.json"
        with open(path, "wb") as f:
            f.write(orjson.dumps(self.filters, option=orjson.OPT_INDENT_2))

    
    @classmethod
    def load_filterset(cls, filepath: str):
        """
        Load filters from a JSON file (saved with orjson) and return a new EcoPlots instance.
        """
        with open(filepath, "rb") as f:
            filters = orjson.loads(f.read())
        return cls(filterset=filters)
    

    def get_summary(self) -> gpd.GeoDataFrame:
        query = {
            "page_number": 1,
            "page_size": 10,
            "query": self.filters
        }

        geojson_data = run_sync(self._ecoplots_api.fetch_data(query=query))

        return gpd.GeoDataFrame.from_features(geojson_data["features"])

    

    async def get_summary_async(self) -> gpd.GeoDataFrame:
        """
        Asynchronous method to get a summary of the EcoPlots data.
        """
        query = {
            "page_number": 1,
            "page_size": 10,
            "query": self.filters
        }
        data = await self._ecoplots_api.fetch_data(query=query)
        gdf = await asyncio.to_thread(gpd.GeoDataFrame.from_features, data["features"])
        return gdf
    

    def get_datasources(self) -> pd.DataFrame:
        query = {
            "query": self.filters
        }

        data = run_sync(self._ecoplots_api.discover("dataset", query=query))
        return pd.DataFrame(data)
    
    
    async def get_datasources_async(self) -> pd.DataFrame:
        """
        Asynchronous method to get the data sources from the EcoPlots API.
        """
        query = {
            "query": self.filters
        }
        data =  await self._ecoplots_api.discover("dataset", query=query)
        # Convert to DataFrame in a thread-safe manner, keeping method non-blocking
        df = await asyncio.to_thread(pd.DataFrame, data)
        return df
    

    def get_datasources_attributes(self):
        pass  # TODO

    
    async def get_datasources_attributes_async(self):
        """
        Asynchronous method to get the attributes of data sources from the EcoPlots API.
        """
        pass  # TODO


    def get_sites(self) -> pd.DataFrame:
        query = {
            "query": self.filters
        }

        data = run_sync(self._ecoplots_api.discover("site_id", query=query))
        return pd.DataFrame(data)
    

    async def get_sites_async(self) -> pd.DataFrame:
        """
        Asynchronous method to get the sites from the EcoPlots API.
        """
        query = {
            "query": self.filters
        }
        data = await self._ecoplots_api.discover("site_id", query=query)
        df = await asyncio.to_thread(pd.DataFrame, data)
        return df
    
    def get_sites_attributes(self):
        pass  # TODO

    
    async def get_sites_attributes_async(self):
        """
        Asynchronous method to get the attributes of sites from the EcoPlots API.
        """
        pass  # TODO


    def get_region_types(self) -> pd.DataFrame:
        """
        Get the available region types from the EcoPlots API.
        """
        query = {
            "query": self.filters
        }
        data = run_sync(self._ecoplots_api.discover("region_type", query=query))
        return pd.DataFrame(data)
    

    async def get_region_types_async(self) -> pd.DataFrame:
        """
        Asynchronous method to get the available region types from the EcoPlots API.
        """
        query = {
            "query": self.filters
        }
        data = await self._ecoplots_api.discover("region_type", query=query)
        df = await asyncio.to_thread(pd.DataFrame, data)
        return df
    

    def get_regions(self, region_type: str) -> pd.DataFrame:
        """
        Get the available regions for a specific region type from the EcoPlots API.
        """
        query = {
            "query": self.filters
        }
        data = run_sync(self._ecoplots_api.discover("region", region_type=region_type, query=query))
        return pd.DataFrame(data)
    

    async def get_regions_async(self, region_type: str) -> pd.DataFrame:
        """
        Asynchronous method to get the available regions for a specific region type from the EcoPlots API.
        """
        query = {
            "query": self.filters
        }
        data = await self._ecoplots_api.discover("region", region_type=region_type, query=query)
        df = await asyncio.to_thread(pd.DataFrame, data)
        return df


    def get_feature_types(self) -> pd.DataFrame:
        query = {
            "query": self.filters
        }

        data = run_sync(self._ecoplots_api.discover("feature_type", query=query))
        return pd.DataFrame(data)
    

    async def get_feature_types_async(self) -> pd.DataFrame:
        """
        Asynchronous method to get the feature types from the EcoPlots API.
        """
        query = {
            "query": self.filters
        }
        data = await self._ecoplots_api.discover("feature_type", query=query)
        df = await asyncio.to_thread(pd.DataFrame, data)
        return df
    

    def get_observed_properties(self) -> pd.DataFrame:
        query = {
            "query": self.filters
        }

        data = run_sync(self._ecoplots_api.discover("observed_property", query=query))
        return pd.DataFrame(data)
    

    async def get_observed_properties_async(self) -> pd.DataFrame:
        """
        Asynchronous method to get the observed properties from the EcoPlots API.
        """
        query = {
            "query": self.filters
        }
        data = await self._ecoplots_api.discover("observed_property", query=query)
        df = await asyncio.to_thread(pd.DataFrame, data)
        return df
    

    def get_data(self, allow_full_download: bool = False) -> gpd.GeoDataFrame:
        """
        Get data from the EcoPlots API based on the current filters.
        """
        query = {
            "query": self.filters
        }
        if not self.filters and not allow_full_download:
            raise RuntimeError(
                "No filters specified! Downloading full EcoPlots dataset "
                "can crash your environment. Proceed with caution!\n"
                "If you are sure, call get_data(allow_full_download=True)."
            )
        data = run_sync(self._ecoplots_api.fetch_data(query=query))
        return gpd.GeoDataFrame.from_features(data["features"])
    

    async def get_data_async(self, allow_full_download: bool = False) -> gpd.GeoDataFrame:
        """
        Asynchronous method to get data from the EcoPlots API based on the current filters.
        """
        query = {
            "query": self.filters
        }
        if not self.filters and not allow_full_download:
            raise RuntimeError(
                "No filters specified! Downloading full EcoPlots dataset "
                "can crash your environment. Proceed with caution!\n"
                "If you are sure, call get_data(allow_full_download=True)."
            )
        data = await self._ecoplots_api.fetch_data(query=query)
        gdf = await asyncio.to_thread(gpd.GeoDataFrame.from_features, data["features"])
        return gdf
    