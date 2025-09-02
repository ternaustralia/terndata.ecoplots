import aiohttp
import copy
import orjson
import requests
import tempfile
import warnings
import zipfile

from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import (
    Dict,
    Optional,
    Union,
    TypeVar,
    Type,
    List,
    Iterable,
)

from .config import (
    API_BASE_URL,
    DISCOVERY_FACETS,
    DISCOVERY_ATTRIBUTES,
    QUERY_FACETS,
)
from .nlp_utils import (
    resolve_facet,
    resolve_region_type,
    validate_facet,
)
from .utils import _atomic_replace, _is_zip_project, _validate_spatial_input

SelfType = TypeVar("SelfType", bound="EcoPlotsBase")

class EcoPlotsBase:
    def __init__(
        self,
        base_url: Optional[str] = None,
        filterset: Optional[Dict] = None,
        query_filters: Optional[Dict] = None
    ):
        self._base_url = base_url or API_BASE_URL
        self._filters = filterset or {}
        self._query_filters = query_filters or {}


    def __eq__(self, other) -> bool:
        if type(self) is not type(other):
            return False
        return (
            self._filters == other._filters and
            self._query_filters == other._query_filters
        )
    

    def __bool__(self) -> bool:
        return bool(self._filters) and bool(self._query_filters)


    def __len__(self) -> int:
        return sum(len(v) for v in self._filters.values())


    def __copy__(self):
        return type(self)(
            filterset=copy.copy(self._filters),
            query_filters=copy.copy(self._query_filters),
        )


    def __deepcopy__(self, memo):
        return type(self)(
            filterset=copy.deepcopy(self._filters, memo),
            query_filters=copy.deepcopy(self._query_filters, memo),
        )
    

    def __contains__(self, item: str) -> bool:
        if item not in QUERY_FACETS:
            raise KeyError(f"Invalid key `{item}`. Allowed: " + ", ".join(QUERY_FACETS))
        return item in self._filters and item in self._query_filters


    def __getitem__(self, item: str) -> List:
        if item not in QUERY_FACETS:
            raise KeyError(f"Invalid key `{item}`. Allowed: " + ", ".join(QUERY_FACETS))
        if item not in self._filters:
            raise KeyError(f"Key `{item}` not present in current instance.")
        return self._filters.get(item, [])
    

    def __setitem__(self, facet: str, values: Union[str, List[str]]):
        if facet not in QUERY_FACETS:
            raise KeyError(f"Invalid key `{facet}`. Allowed: " + ", ".join(QUERY_FACETS))
        self.select(facet, values)

    
    def __delitem__(self, key) -> None:
        """Delegate deletions to :meth:`remove`.

        Usage:

        ```python
        del ec['site_id']                       # remove entire facet
        del ec['site_id', 'TCFTNS0002']         # remove a single value
        del ec['site_id', ['A','B','C']]        # remove multiple values
        ```
        """
        if isinstance(key, tuple):
            if len(key) != 2:
                raise KeyError("Expected ('facet', values) for value deletion.")
            facet, values = key
            # Allow human/canonical names here by resolving to canonical
            if facet not in QUERY_FACETS:
                raise KeyError(f"Unknown facet {facet!r}. Allowed: {', '.join(QUERY_FACETS)}")
            # Delegate; remove() expects canonical keys, same as select()
            self.remove(**{facet: values})
        else:
            if key not in QUERY_FACETS:
                raise KeyError(f"Unknown facet {key!r}. Allowed: {', '.join(QUERY_FACETS)}")
            self.remove(**{key: None})


    def select(self, filters: Optional[Dict] = None, **kwargs) -> SelfType:
        """Add/merge filters and validate them.

        Accepts either a dict or keyword arguments.

        Args:
            filters: Mapping like ``{"site_id": [...], "dataset": [...]}``.
            **kwargs: Alternative way to pass filters, e.g. ``site_id="ABC"``.

        Raises:
            ValueError: Unknown filter keys.
            ValueError: ``region`` provided without current or new ``region_type``.

        Returns:
            EcoPlots: ``self`` for chaining.
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
            if k == "spatial":
                _validate_spatial_input(v)  # validate spatial filter
                # replace any existing spatial filter
                self._filters["spatial"] = [v]
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
    

    def remove(self, filters: Optional[Dict] = None, **kwargs) -> SelfType:
        """Remove whole facets or specific values (same ergonomics as ``select``).

        Accepts either a dict or keyword arguments. For each facet:
          - value is ``None``  → remove the **entire facet**
          - value is a string  → remove that **single value**
          - value is a list/tuple → remove **those values**

        Args:
            filters: Mapping like ``{"site_id": ["TCFTNS0002"], "dataset": None}``.
            **kwargs: Alternative way to pass removals, e.g. ``site_id="TCFTNS0002"``.

        Raises:
            ValueError: Unknown filter keys (not in ``QUERY_FACETS``).
            KeyError: Facet not present in current filters.
            ValueError: Specific values requested but not found for that facet.

        Returns:
            EcoPlots: ``self`` (chainable).
        """
        # Merge inputs (dict + kwargs), exactly like select()
        input_filters: Dict[str, Union[None, str, Iterable[str]]] = {}
        if filters:
            input_filters.update(filters)
        if kwargs:
            input_filters.update(kwargs)

        # 1. Validate allowed keys
        invalid_keys = set(input_filters) - set(QUERY_FACETS)
        if invalid_keys:
            raise ValueError(f"Invalid filter keys: {invalid_keys}. Allowed: {QUERY_FACETS}")

        removed_facets = set()

        # 2. Apply removals
        for facet, vals in input_filters.items():
            if facet not in self._filters:
                raise KeyError(f"Facet {facet!r} not present in filters.")

            if vals is None:
                # remove entire facet
                self._filters.pop(facet, None)
                removed_facets.add(facet)
                continue

            if facet == "spatial" and vals is not None:
                raise ValueError("Cannot remove specific values from 'spatial' filter; use None to clear entire facet.")

            # normalize to a list
            if isinstance(vals, (str, bytes)) or not isinstance(vals, Iterable):
                vals = [vals]
            else:
                vals = list(vals)

            existing = self._filters.get(facet, [])
            missing = [v for v in vals if v not in existing]
            if missing:
                raise ValueError(f"Values not found in facet {facet!r}: {missing}")

            # remove requested values
            self._filters[facet] = [v for v in existing if v not in vals]
            if not self._filters[facet]:
                self._filters.pop(facet, None)
                removed_facets.add(facet)

        # 3. Handle region invariants
        # If region_type was removed, clear all 'region' facet.
        if "region_type" in removed_facets and "region" in self._filters:
            self._filters.pop("region", None)

        # 4. Rebuild API query filters
        self._query_filters = {}
        if self._filters:
            self._validate_filters()

        return self
    

    def clear(self) -> SelfType:
        """Clear all filters from the instance.

        The method mutates the instance and returns it to allow fluent/chained calls.

        Returns:
            EcoPlots: The same instance with its filters cleared.
        """
        self._filters = {}
        self._query_filters = {}
        return self


    def get_filter(self, facet: Optional[str] = None) -> Union[List, Dict]:
        """Return the current filter values for a specific facet or all applied filters.

        Args:
            facet (Optional[str], optional): The facet to retrieve the filter for. Defaults to All.

        Raises:
            ValueError: If an invalid facet name is provided.

        Returns:
            Union[List, Dict]: The current filter values for the specified facet as list.
            Returns a JSON string of all filters if facet is None.
        """
        if facet:
            facet_val = resolve_facet(facet, QUERY_FACETS)
            if facet_val:
                return self._filters.get(facet_val)
            else:
                raise ValueError(
                    f"Invalid facet name `{facet}`. Allowed facets: " + ", ".join(QUERY_FACETS)
                )
        
        return orjson.dumps(self._filters, option=orjson.OPT_INDENT_2).decode("utf-8")


    def get_api_query_filters(self, facet: str = None) -> Union[List, Dict]:
        """Return the current query filters for ecoplots API for a specified facet or all facet.

        Args:
            facet (str, optional): The facet to retrieve the query filters for. Defaults to None.

        Raises:
            ValueError: If an invalid facet name is provided.

        Returns:
            Union[List, Dict]: A dictionary of the current query filters.
            Returns a JSON string of all query filters if facet is None.
        """
        if facet:
            facet_val = resolve_facet(facet, QUERY_FACETS)
            if facet_val:
                return self._query_filters.get(facet_val)
            else:
                raise ValueError(
                    f"Invalid facet name `{facet}`. Allowed facets: " + ", ".join(QUERY_FACETS)
                )

        return orjson.dumps(self._query_filters, option=orjson.OPT_INDENT_2).decode("utf-8")

    
    def discover(
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

        resp = requests.post(url, json=payload, timeout=10)
        resp.raise_for_status()
        return orjson.loads(resp.content)

    
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
        

    def discover_attributes(
        self,
        discovery_attribute: str,
    ) -> dict:
        facet_pram = resolve_facet(discovery_attribute, DISCOVERY_ATTRIBUTES)

        if not facet_pram:
            raise ValueError(f"Invalid discovery facet: {discovery_attribute}")

        url = f"{self._base_url}/api/v1.0/discovery/attributes"

        payload = {"query": copy.deepcopy(self._query_filters)}

        params = [("type", facet_pram)]

        resp = requests.post(url, params=params, json=payload, timeout=30)
        resp.raise_for_status()
        return orjson.loads(resp.content)
            

    def summarise_data(self, query_filters: Optional[Dict] = None) -> dict:
        payload = {
            "query": copy.deepcopy(query_filters) if (
                query_filters is not None
            ) else copy.deepcopy(self._query_filters),
        }

        resp = requests.post(f"{self._base_url}/api/v1.0/data/summary", json=payload, timeout=30)
        resp.raise_for_status()
        return orjson.loads(resp.content)

    
    # async def stream_data(self, query: dict = {}) -> dict:
    #     payload = copy.deepcopy(query)
    #     async with httpx.AsyncClient() as client:
    #         response = await client.post(f"{self.base_url}/api/v1.0/data/stream", json=payload)
    #         response.raise_for_status()
    #         return response.json()
        

    def _validate_filters(self) -> bool:
        """[INTERNAL] Validate filters in parallel for all facets. Not for direct user use."""

        query_filters = copy.deepcopy(self._query_filters)
        all_unmatched = {}
        all_matched = copy.deepcopy(self._filters)

        if "spatial" in all_matched:
            query_filters["spatial"] = list(all_matched["spatial"])

        with ThreadPoolExecutor() as executor:
            futures = {
                # NOTE: `validate_facet` uses rapidfuzz under the hood,
                # rapidfuzz itself releases the GIL (written in C++),
                # so we can leverage "true" parallelism here with ThreadPoolExecutor
                # for CPU bound fuzzy matching and is much faster than asyncio.gather.
                executor.submit(validate_facet, facet, value): facet
                for facet, value in self._filters.items()
            }

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
        
        data = self.summarise_data(query_filters=query_filters)
        if data.get("total_doc", 0) == 0:
            warnings.warn(
                "The applied filters result in zero matching records. "
                "Please adjust your filters. Skipping current selection...",
                UserWarning
            )
            return False
        
        self._query_filters = query_filters
        self._filters = all_matched

        return True


    def save(self, path: Optional[Union[str, Path]] = None) -> str:
        """
        Save a minimal project containing filters + query_filters.

        Behavior:
          - path endswith .ecoproj -> write a single ZIP with filters.json + query_filters.json
          - path is a directory     -> write those two files into the directory
          - path is None            -> write into ./ecoplots_project (create if missing)
        """
        if path is None:
            target = Path.cwd() / "ecoplots_project"
            target.mkdir(parents=True, exist_ok=True)
            (target / "filters.json").write_bytes(orjson.dumps(self._filters, option=orjson.OPT_INDENT_2))
            (target / "query_filters.json").write_bytes(orjson.dumps(self._query_filters, option=orjson.OPT_INDENT_2))
            return str(target)

        target = Path(path)

        if _is_zip_project(target):
            tmp = tempfile.NamedTemporaryFile(delete=False, suffix=".ecoproj")
            tmp_path = Path(tmp.name)
            tmp.close()
            try:
                with zipfile.ZipFile(tmp_path, mode="w", compression=zipfile.ZIP_DEFLATED) as z:
                    z.writestr("filters.json", orjson.dumps(self._filters, option=orjson.OPT_INDENT_2))
                    z.writestr("query_filters.json", orjson.dumps(self._query_filters, option=orjson.OPT_INDENT_2))
                _atomic_replace(tmp_path, target)
            finally:
                if tmp_path.exists():
                    try: tmp_path.unlink()
                    except OSError: pass
            return str(target)

        target.mkdir(parents=True, exist_ok=True)
        (target / "filters.json").write_bytes(orjson.dumps(self._filters, option=orjson.OPT_INDENT_2))
        (target / "query_filters.json").write_bytes(orjson.dumps(self._query_filters, option=orjson.OPT_INDENT_2))
        return str(target)

    
    @classmethod
    def load(cls: Type[SelfType], path: Union[str, Path]) -> SelfType:
        """
        Load filters + query_filters from a .ecoproj ZIP or a directory.
        Returns an instance of `cls` (works for subclasses if they accept
        filterset/query_filters in __init__).
        """
        p = Path(path)
        if _is_zip_project(p):
            with zipfile.ZipFile(p, "r") as z:
                filters = orjson.loads(z.read("filters.json"))
                qfilters = orjson.loads(z.read("query_filters.json"))
        else:
            filters_path = p / "filters.json"
            qfilters_path = p / "query_filters.json"
            if not (filters_path.exists() and qfilters_path.exists()):
                raise FileNotFoundError(
                    f"Expected {filters_path} and {qfilters_path} in project directory."
                )
            filters = orjson.loads(filters_path.read_bytes())
            qfilters = orjson.loads(qfilters_path.read_bytes())

        return cls(filterset=filters, query_filters=qfilters)
