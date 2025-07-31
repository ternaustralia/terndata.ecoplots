import copy
import httpx

from terndata.ecoplots.nlp_utils import (
    resolve_discovery_facet,
    resolve_region_type,
)

class _EcoPlotsAPI:
    def __init__(self, base_url: str):
        self.base_url = base_url

    
    async def discover(
        self,
        discovery_facet: str,
        region_type: str = None,
        query: dict = {},
    ) -> dict:
        facet_pram = resolve_discovery_facet(discovery_facet)

        if facet_pram:
            if facet_pram == "region_type" and region_type:
                region_type_val = resolve_region_type(region_type)
                url = f"{self.base_url}/api/v1.0/discover/{facet_pram}?region_type={region_type_val}"
            else:
                url = f"{self.base_url}/api/v1.0/discover/{facet_pram}"

            # TODO: Resolve query parameters using NLP utilities
            payload = copy.deepcopy(query)

            async with httpx.AsyncClient() as client:
                response = await client.post(url, json=payload)
                response.raise_for_status()
                return response.json()
        else:
            raise ValueError(f"Invalid discovery facet: {discovery_facet}")

    
    async def fetch_data(self, query: dict = {}) -> dict:
        payload = copy.deepcopy(query)
        async with httpx.AsyncClient() as client:
            response = await client.post(f"{self.base_url}/api/v1.0/data", json=payload)
            response.raise_for_status()
            return response.json()
        
    
    async def stream_data(self, query: dict = {}) -> dict:
        payload = copy.deepcopy(query)
        async with httpx.AsyncClient() as client:
            response = await client.post(f"{self.base_url}/api/v1.0/data/stream", json=payload)
            response.raise_for_status()
            return response.json()