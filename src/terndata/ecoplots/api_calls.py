import aiohttp
import copy
import orjson

from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Dict, Optional

from terndata.ecoplots.config import API_BASE_URL, DISCOVERY_FACETS, DISCOVERY_ATTRIBUTES
from terndata.ecoplots.nlp_utils import (
    resolve_facet,
    resolve_region_type,
    validate_facet
)

class _EcoPlotsAPI:
    def __init__(
        self,
        base_url: Optional[str] = None,
        filterset: Optional[Dict] = None,
        query_filters: Optional[Dict] = None
    ):
        self._base_url = base_url or API_BASE_URL
        self._filters = filterset or {}
        self._query_filters = query_filters or {}

    
    async def discover(
        self,
        discovery_facet: str,
        region_type: Optional[str] = None,
    ) -> dict:
        facet_pram = resolve_facet(discovery_facet, DISCOVERY_FACETS)

        if not facet_pram:
            raise ValueError(f"Invalid discovery facet: {discovery_facet}")

        if facet_pram == "region" and region_type:
            region_type_val = resolve_region_type(region_type)
            url = f"{self._base_url}/api/v1.0/discovery/{facet_pram}?region_type={region_type_val}"
        else:
            url = f"{self._base_url}/api/v1.0/discovery/{facet_pram}"

        payload = {"query": copy.deepcopy(self._query_filters)}

        timeout = aiohttp.ClientTimeout(total=30)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with session.post(url, json=payload) as resp:
                resp.raise_for_status()
                return await resp.json(loads=orjson.loads)

    
    async def fetch_data(
        self,
        page_number: Optional[int] = None,
        page_size: Optional[int] = None,
    ) -> dict:
        
        
        payload = {
            "query": copy.deepcopy(self._query_filters),
            # "page_number": page_number,
            # "page_size": page_size
        }

        if page_number and page_size:
            payload.update({
                "page_number": page_number,
                "page_size": page_size
            })
            timeout = aiohttp.ClientTimeout(total=60)
        else:
            timeout = aiohttp.ClientTimeout(total=300)

        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with session.post(f"{self._base_url}/api/v1.0/data?dformat=geojson", json=payload) as resp:
                resp.raise_for_status()
                data = await resp.read()
                return orjson.loads(data)
        

    async def discover_attributes(
        self,
        discovery_attribute: str,
    ) -> dict:
        facet_pram = resolve_facet(discovery_attribute, DISCOVERY_ATTRIBUTES)

        if not facet_pram:
            raise ValueError(f"Invalid discovery facet: {discovery_attribute}")

        url = f"{self._base_url}/api/v1.0/discovery/attributes"

        payload = {"query": copy.deepcopy(self._query_filters)}

        params = [("type", facet_pram)]

        timeout = aiohttp.ClientTimeout(total=30)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with session.post(url, params=params, json=payload) as resp:
                resp.raise_for_status()
                return await resp.json(loads=orjson.loads)

    
    # async def stream_data(self, query: dict = {}) -> dict:
    #     payload = copy.deepcopy(query)
    #     async with httpx.AsyncClient() as client:
    #         response = await client.post(f"{self.base_url}/api/v1.0/data/stream", json=payload)
    #         response.raise_for_status()
    #         return response.json()
        

    def _validate_filters(self):
        """[INTERNAL] Validate filters in parallel for all facets. Not for direct user use."""

        query_filters = copy.deepcopy(self._query_filters)
        all_unmatched = {}
        all_matched = copy.deepcopy(self._filters)

        with ThreadPoolExecutor() as executor:
            futures = {
                # NOTE: `validate_facet` uses rapidfuzz under the hood,
                # rapidfuzz itself releases the GIL (written in C++),
                # so we can leverage "true" parallelism here with ThreadPoolExecutor
                # for CPU bound fuzzy matching and is much faster than asyncio.gather.
                executor.submit(validate_facet, facet, value): facet
                for facet, value in self._filters.items()
            }
            # for future in as_completed(futures):
            #     facet, urls, matched, unmatched, corrected = future.result()
            #     if urls:
            #         if facet not in query_filters or query_filters[facet] is None:
            #             query_filters[facet] = urls
            #         else:
            #             query_filters[facet].extend(urls)
            #         query_filters[facet] = list(set(query_filters[facet]))  # Remove duplicates

            #     if matched:
            #         # remove all corrcted values from matched
            #         all_matched[facet] = [x for x in all_matched[facet] if x not in corrected]
            #         for val in matched:
            #             if val not in all_matched.get(facet, []):
            #                 all_matched[facet].append(val)
            #     if unmatched:
            #         all_unmatched[facet] = unmatched
            for future in as_completed(futures):
                facet, urls, matched, unmatched, corrected = future.result()
                
                # Convert to set for updating
                existing = set(query_filters.get(facet, []))
                existing.update(urls)
                query_filters[facet] = list(existing)
                            
                all_matched.setdefault(facet, [])
                # ensure corrected values are excluded
                all_matched[facet]= [x for x in matched if x not in corrected]
                # for val in filtered_matched:
                #     if val not in all_matched[facet]:
                #         all_matched[facet].append(val)
                
                if unmatched:
                    all_unmatched.setdefault(facet, [])
                    all_unmatched[facet].extend(unmatched)

            # convert sets to lists
            # query_filters = {facet: list(urls) for facet, urls in query_filters.items()}
        
        if all_unmatched:
            msg = (
                "The following filter values could not be matched:\n" +
                "\n".join(
                    f"Facet '{facet}': {unmatched}" 
                    for facet, unmatched in all_unmatched.items()
                )
            )
            raise ValueError(msg)
        
        self._query_filters = query_filters
        self._filters = all_matched

        return True