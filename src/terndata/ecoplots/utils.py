import asyncio
import diskcache
import orjson
import warnings

from elasticsearch import AsyncElasticsearch
from elasticsearch.exceptions import ElasticsearchWarning
from typing import Dict, Union, List

from terndata.ecoplots.config import (
    CACHE_DIR,
    CACHE_EXPIRE_SECONDS,
    ELASTICSEARCH_INDEX_LABELS,
    ELASTICSEARCH_URL,
    VOCAB_FACETS,
)


warnings.simplefilter(
    "ignore",
    ElasticsearchWarning,
)

class OrjsonSerializer:
    """Custom serializer for Elasticsearch using orjson"""

    def dumps(self, data):
        # Serialize to JSON string (Elasticsearch expects str not bytes)
        return orjson.dumps(data).decode()

    def loads(self, data):
        # Parse response JSON bytes to Python dict
        return orjson.loads(data)


async def get_single_label(es_client, index, page_size=1000):
    """
    Fetches all labels for a given ES index using async search_after pagination.
    Returns: dict of {uri: label}
    """
    result = {}
    search_after = None
    more = True
    while more:
        body = {
            "size": page_size,
            "sort": [{"uri": "asc"}],
            "query": {"match_all": {}}
        }
        if search_after:
            body["search_after"] = search_after

        resp = await es_client.search(index=index, body=body)
        hits = resp["hits"]["hits"]
        for hit in hits:
            result[hit["_source"]["uri"]] = hit["_source"]["label"]
        if len(hits) < page_size:
            more = False
        else:
            search_after = hits[-1]["sort"]
    return result


async def cache_labels():
    """
    Fetches and caches labels for all facets using asyncio.gather.
    Stores each as a key in diskcache.
    """
    cache = diskcache.Cache(CACHE_DIR)
    es_client = AsyncElasticsearch(
        hosts=[ELASTICSEARCH_URL],
        retry_on_timeout=True,
        verify_certs=False,
        ssl_show_warn=False,
        serializer=OrjsonSerializer(),
        timeout=3000,
    )
    try:
        async def fetch_and_cache(facet):
            # Only fetch if cache is missing or expired
            # Check if cache exists and is not expired
            if cache.get(facet, default=None, read=True) is not None:
                print(f"Using cached labels for facet: {facet}")
                return facet, cache[facet]
            index = f"{ELASTICSEARCH_INDEX_LABELS}-{facet}"
            labels = await get_single_label(es_client, index)
            cache.set(facet, labels, expire=CACHE_EXPIRE_SECONDS)
            return facet, labels

        tasks = [fetch_and_cache(facet) for facet in VOCAB_FACETS]
        import time
        start_time = time.time()
        results = await asyncio.gather(*tasks)
        end_time = time.time()
        print(f"Cached labels for {len(results)} facets in {end_time - start_time} seconds.")
        return dict(results)
    finally:
        await es_client.close()


def background_cache_loader():
    """
    Background task to cache labels for all facets.
    This can be run periodically to refresh the cache.
    """
    asyncio.run(cache_labels())


def run_sync(coro):
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
