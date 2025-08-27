import os
import aiohttp
import asyncio
import diskcache
import orjson

from pathlib import Path
from typing import Dict, Union, List

from terndata.ecoplots.config import (
    API_BASE_URL,
    CACHE_DIR,
    CACHE_EXPIRE_SECONDS,
    VOCAB_FACETS,
)


async def get_single_label(facet):
    print("Fetching labels for facet:", facet)
    url = f"{API_BASE_URL}/api/v1.0/data/label/{facet}"
    async with aiohttp.ClientSession() as session:
        async with session.get(url) as resp:
            resp.raise_for_status()
            result = await resp.json(loads=orjson.loads)
            return result[facet]


async def cache_labels():
    """
    Fetches and caches labels for all facets using asyncio.gather.
    Stores each as a key in diskcache.
    """
    cache = diskcache.Cache(CACHE_DIR)
    async def fetch_and_cache(facet):
        # Only fetch if cache is missing or expired
        # Check if cache exists and is not expired
        if cache.get(facet, default=None, read=True) is not None:
            print(f"Using cached labels for facet: {facet}")
            return facet, cache[facet]
        labels = await get_single_label(facet)
        cache.set(facet, labels, expire=CACHE_EXPIRE_SECONDS)
        return facet, labels

    tasks = [fetch_and_cache(facet) for facet in VOCAB_FACETS]
    results = await asyncio.gather(*tasks)
    return dict(results)


def background_cache_loader():
    """
    Background task to cache labels for all facets.
    This can be run periodically to refresh the cache.
    """
    asyncio.run(cache_labels())


def run_sync(coro: asyncio.coroutine) -> any:
    """
    Run an async coroutine as a sync call.
    Works in scripts, console, AND notebooks.
    """
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        loop = None
    if loop and loop.is_running():
        # We're in Jupyter/IPython or already running event loop
        import nest_asyncio
        nest_asyncio.apply()
        return loop.run_until_complete(coro)
    else:
        return asyncio.run(coro)
    

def get_cached_labels(facet: str = None) -> Dict[str, str]:
    cache = diskcache.Cache(CACHE_DIR)
    if facet:
        labels = cache.get(facet)
        if labels is None:
            raise KeyError(f"No cached labels found for facet: {facet}")
        return labels
    

def normalise_to_list(value: Union[str, List[str]]) -> List[str]:
    """
    Normalizes a single string or a list of strings to a list.
    """
    if isinstance(value, str):
        return [v.strip() for v in value.split(",") if v.strip()]
    elif isinstance(value, list):
        return [str(v).strip() for v in value if str(v).strip()]
    else:
        raise TypeError(f"Invalid filter value type: {type(value)}")


def _atomic_replace(tmp_path: Path, final_path: Path) -> None:
    final_path.parent.mkdir(parents=True, exist_ok=True)
    os.replace(tmp_path, final_path)

def _is_zip_project(path: Path) -> bool:
    return path.suffix.lower() == ".ecoproj"
