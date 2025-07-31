import asyncio
import orjson
from typing import Optional, Dict
from terndata.ecoplots.api_calls import _EcoPlotsAPI
from terndata.ecoplots.config import API_BASE_URL

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
    

    def get_summary(self):
        query = {
            "page_number": 1,
            "page_size": 10,
            "query": self.filters
        }

        return asyncio.run(self._ecoplots_api.fetch_data(query=query))
    

    async def get_summary_async(self):
        """
        Asynchronous method to get a summary of the EcoPlots data.
        """
        query = {
            "page_number": 1,
            "page_size": 10,
            "query": self.filters
        }
        return await self._ecoplots_api.fetch_data(query=query)
    

    def get_datasources(self):
        query = {
            "query": self.filters
        }

        return asyncio.run(self._ecoplots_api.discover("dataset", query=query))
    
    
    async def get_datasources_async(self):
        """
        Asynchronous method to get the data sources from the EcoPlots API.
        """
        query = {
            "query": self.filters
        }
        return await self._ecoplots_api.discover("dataset", query=query)
    

    def get_datasources_attributes(self):
        pass  # TODO

    
    async def get_datasources_attributes_async(self):
        """
        Asynchronous method to get the attributes of data sources from the EcoPlots API.
        """
        pass  # TODO


    def get_sites(self):
        query = {
            "query": self.filters
        }

        return asyncio.run(self._ecoplots_api.discover("site_id", query=query))
    

    async def get_sites_async(self):
        """
        Asynchronous method to get the sites from the EcoPlots API.
        """
        query = {
            "query": self.filters
        }
        return await self._ecoplots_api.discover("site_id", query=query)
    
    def get_sites_attributes(self):
        pass  # TODO

    
    async def get_sites_attributes_async(self):
        """
        Asynchronous method to get the attributes of sites from the EcoPlots API.
        """
        pass  # TODO


    def get_region_types(self):
        """
        Get the available region types from the EcoPlots API.
        """
        query = {
            "query": self.filters
        }
        return asyncio.run(self._ecoplots_api.discover("region_type", query=query))
    

    async def get_region_types_async(self):
        """
        Asynchronous method to get the available region types from the EcoPlots API.
        """
        query = {
            "query": self.filters
        }
        return await self._ecoplots_api.discover("region_type", query=query)
    

    def get_regions(self, region_type: str):
        """
        Get the available regions for a specific region type from the EcoPlots API.
        """
        query = {
            "query": self.filters
        }
        return asyncio.run(self._ecoplots_api.discover("region", region_type=region_type, query=query))
    

    async def get_regions_async(self, region_type: str):
        """
        Asynchronous method to get the available regions for a specific region type from the EcoPlots API.
        """
        query = {
            "query": self.filters
        }
        return await self._ecoplots_api.discover("region", region_type=region_type, query=query)


    def get_feature_types(self):
        query = {
            "query": self.filters
        }

        return asyncio.run(self._ecoplots_api.discover("feature_type", query=query))
    

    async def get_feature_types_async(self):
        """
        Asynchronous method to get the feature types from the EcoPlots API.
        """
        query = {
            "query": self.filters
        }
        return await self._ecoplots_api.discover("feature_type", query=query)
    

    def get_observed_properties(self):
        query = {
            "query": self.filters
        }

        return asyncio.run(self._ecoplots_api.discover("observed_property", query=query))
    

    async def get_observed_properties_async(self):
        """
        Asynchronous method to get the observed properties from the EcoPlots API.
        """
        query = {
            "query": self.filters
        }
        return await self._ecoplots_api.discover("observed_property", query=query)
    

    def get_data(self):
        """
        Get data from the EcoPlots API based on the current filters.
        """
        query = {
            "query": self.filters
        }
        return asyncio.run(self._ecoplots_api.fetch_data(query=query))
    

    async def get_data_async(self):
        """
        Asynchronous method to get data from the EcoPlots API based on the current filters.
        """
        query = {
            "query": self.filters
        }
        return await self._ecoplots_api.fetch_data(query=query)
    