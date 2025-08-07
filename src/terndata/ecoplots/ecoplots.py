import asyncio
import orjson
import pandas as pd
import geopandas as gpd

from typing import Optional, Dict, List, Union
from terndata.ecoplots.api_calls import _EcoPlotsAPI
from terndata.ecoplots.config import QUERY_FACETS
from terndata.ecoplots.utils import run_sync
from terndata.ecoplots.nlp_utils import resolve_facet


class EcoPlots(_EcoPlotsAPI):
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


    # def select(self, **filters):
    #     """
    #     Set filters for the EcoPlots API calls.
    #     """
    #     self._filters.update(filters)

    def select(self, filters: dict = None, **kwargs):
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


    def get_filter(self, facet: str = None) -> Union[List, Dict]:
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
    
    
    def save(self, filepath: Optional[str] = None):
        """
        Save a filterset for later use.
        """
        path = filepath or "ecoplots_filterset.json"
        with open(path, "wb") as f:
            f.write(orjson.dumps(self._filters, option=orjson.OPT_INDENT_2))

    
    @classmethod
    def load(cls, filepath: str) -> "EcoPlots":
        """
        Load filters from a JSON file (saved with orjson) and return a new EcoPlots instance.
        """
        with open(filepath, "rb") as f:
            filters = orjson.loads(f.read())
        return cls(filterset=filters)
    

    def get_summary(self) -> gpd.GeoDataFrame:
        geojson_data = run_sync(self.fetch_data(page_number=1, page_size=10))
        return gpd.GeoDataFrame.from_features(geojson_data["features"])

    
    async def get_summary_async(self) -> gpd.GeoDataFrame:
        """
        Asynchronous method to get a summary of the EcoPlots data.
        """
        data = await self.fetch_data(page_number=1, page_size=10)
        gdf = await asyncio.to_thread(gpd.GeoDataFrame.from_features, data["features"])
        return gdf
    

    def get_datasources(self) -> pd.DataFrame:
        data = run_sync(self.discover("dataset"))
        return pd.DataFrame(data)
    
    
    async def get_datasources_async(self) -> pd.DataFrame:
        """
        Asynchronous method to get the data sources from the EcoPlots API.
        """
        data =  await self.discover("dataset")
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
        data = run_sync(self.discover("site_id"))
        return pd.DataFrame(data)
    

    async def get_sites_async(self) -> pd.DataFrame:
        """
        Asynchronous method to get the sites from the EcoPlots API.
        """
        data = await self.discover("site_id")
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
        data = run_sync(self.discover("region_type"))
        return pd.DataFrame(data)
    

    async def get_region_types_async(self) -> pd.DataFrame:
        """
        Asynchronous method to get the available region types from the EcoPlots API.
        """
        data = await self.discover("region_type")
        df = await asyncio.to_thread(pd.DataFrame, data)
        return df
    

    def get_regions(self, region_type: str) -> pd.DataFrame:
        """
        Get the available regions for a specific region type from the EcoPlots API.
        """
        data = run_sync(self.discover("region", region_type=region_type))
        return pd.DataFrame(data)
    

    async def get_regions_async(self, region_type: str) -> pd.DataFrame:
        """
        Asynchronous method to get the available regions for a specific region type from the EcoPlots API.
        """
        data = await self.discover("region", region_type=region_type)
        df = await asyncio.to_thread(pd.DataFrame, data)
        return df


    def get_feature_types(self) -> pd.DataFrame:
        data = run_sync(self.discover("feature_type"))
        return pd.DataFrame(data)
    

    async def get_feature_types_async(self) -> pd.DataFrame:
        """
        Asynchronous method to get the feature types from the EcoPlots API.
        """
        data = await self.discover("feature_type")
        df = await asyncio.to_thread(pd.DataFrame, data)
        return df
    

    def get_observed_properties(self) -> pd.DataFrame:
        data = run_sync(self.discover("observed_property"))
        return pd.DataFrame(data)
    

    async def get_observed_properties_async(self) -> pd.DataFrame:
        """
        Asynchronous method to get the observed properties from the EcoPlots API.
        """
        data = await self.discover("observed_property")
        df = await asyncio.to_thread(pd.DataFrame, data)
        return df
    

    def get_data(self, allow_full_download: bool = False) -> gpd.GeoDataFrame:
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
        return gpd.GeoDataFrame.from_features(data["features"])
    

    async def get_data_async(self, allow_full_download: bool = False) -> gpd.GeoDataFrame:
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
        gdf = await asyncio.to_thread(gpd.GeoDataFrame.from_features, data["features"])
        return gdf


    # def _validate_filters(self):
    #     """[INTERNAL] Validate filters in parallel for all facets. Not for direct user use."""

    #     query_filters = self.query_filters
    #     all_unmatched = {}
    #     all_matched = self._filters

    #     with ThreadPoolExecutor() as executor:
    #         futures = {
    #             # NOTE: `validate_facet` uses rapidfuzz under the hood,
    #             # rapidfuzz itself releases the GIL (written in C++),
    #             # so we can leverage "true" parallelism here with ThreadPoolExecutor
    #             # for CPU bound fuzzy matching and is much faster than asyncio.gather.
    #             executor.submit(validate_facet, facet, value): facet
    #             for facet, value in self._filters.items()
    #         }
    #         # for future in as_completed(futures):
    #         #     facet, urls, matched, unmatched, corrected = future.result()
    #         #     if urls:
    #         #         if facet not in query_filters or query_filters[facet] is None:
    #         #             query_filters[facet] = urls
    #         #         else:
    #         #             query_filters[facet].extend(urls)
    #         #         query_filters[facet] = list(set(query_filters[facet]))  # Remove duplicates

    #         #     if matched:
    #         #         # remove all corrcted values from matched
    #         #         all_matched[facet] = [x for x in all_matched[facet] if x not in corrected]
    #         #         for val in matched:
    #         #             if val not in all_matched.get(facet, []):
    #         #                 all_matched[facet].append(val)
    #         #     if unmatched:
    #         #         all_unmatched[facet] = unmatched
    #         for future in as_completed(futures):
    #             facet, urls, matched, unmatched, corrected = future.result()
                
    #             query_filters.setdefault(facet, set())
    #             query_filters[facet].update(urls)
                
    #             all_matched.setdefault(facet, [])
    #             # ensure corrected values are excluded
    #             filtered_matched = [x for x in matched if x not in corrected]
    #             for val in filtered_matched:
    #                 if val not in all_matched[facet]:
    #                     all_matched[facet].append(val)
                
    #             if unmatched:
    #                 all_unmatched.setdefault(facet, [])
    #                 all_unmatched[facet].extend(unmatched)

    #         # convert sets to lists
    #         query_filters = {facet: list(urls) for facet, urls in query_filters.items()}
        
    #     if all_unmatched:
    #         msg = (
    #             "The following filter values could not be matched:\n" +
    #             "\n".join(
    #                 f"Facet '{facet}': {unmatched}" 
    #                 for facet, unmatched in all_unmatched.items()
    #             )
    #         )
    #         raise ValueError(msg)
        
    #     self.query_filters = query_filters
    #     self._filters = all_matched

    #     return True