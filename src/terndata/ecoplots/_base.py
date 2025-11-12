import copy
import hashlib
import struct
import tempfile
import warnings
from collections.abc import Iterable
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import (
    Optional,
    TypeVar,
    Union,
)

import aiohttp
import orjson
import requests

from ._config import (
    API_BASE_URL,
    DISCOVERY_ATTRIBUTES,
    DISCOVERY_FACETS,
    MAGIC,
    QUERY_FACETS,
    VERSION,
)
from ._nlp_utils import (
    resolve_facet,
    resolve_region_type,
    validate_facet,
)
from ._utils import _ensure_ecoproj_path, _validate_spatial_input

SelfType = TypeVar("SelfType", bound="EcoPlotsBase")


class EcoPlotsBase:
    """Base class providing internal filter management and API query building.

    This class implements the low-level mechanics for holding, validating and
    converting user-supplied filters into API-ready query filters. It is
    intended for internal use by ecoplots subclasses and is not part of the
    user-facing public API.

    Attributes:
        _filters: Human/canonical filter values as provided.
        _query_filters: API-ready filter values (usually URLs).
    """

    def __init__(
        self,
        filterset: Optional[dict] = None,
        query_filters: Optional[dict] = None,
    ):
        """Initialize an EcoPlotsBase instance.

        The constructor only sets up internal state. Validation of filters is
        performed lazily by calls to select()/remove() which in turn call
        _validate_filters().

        Args:
            base_url: Optional override for the API base URL. If omitted the
                module-level API_BASE_URL is used.
            filterset: Optional mapping of facet -> list of human/canonical values
                to pre-populate the instance.
            query_filters: Optional mapping of facet -> list of API-ready values
                (eg. URLs) to pre-populate the instance.
        """
        self._base_url = API_BASE_URL
        self._filters = filterset or {}
        self._query_filters = query_filters or {}

    def __str__(self) -> str:
        """Return a user-friendly string representation of the instance.

        The string includes a professional header with the class name and version,
        followed by a clean, organized summary of the current filter configuration.
        Uses Unicode box-drawing characters for a polished, institutional look.

        Returns:
            A formatted string summarizing the instance and its filter state.

        Examples:
            >>> ec = EcoPlots()
            >>> ec.select(site_id="TCFTNS0002")
            >>> print(ec)
            ╔══════════════════════════════════════════════════════════════════════════════╗
            ║ EcoPlots                                                                     ║
            ║ Version: 0.0.3-beta                                                          ║
            ╠══════════════════════════════════════════════════════════════════════════════╣
            ║ Active Filters:                                                              ║
            ║   • site_id: TCFTNS0002                                                      ║
            ╚══════════════════════════════════════════════════════════════════════════════╝
        """
        # Box drawing constants
        BOX_WIDTH = 78
        
        # Header with decorative separator
        header = f"╔{'═' * BOX_WIDTH}╗"
        title = f"║ {self.__class__.__name__:<{BOX_WIDTH - 2}} ║"
        version_line = f"║ Version: {VERSION:<{BOX_WIDTH - 11}} ║"
        separator = f"╠{'═' * BOX_WIDTH}╣"
        footer = f"╚{'═' * BOX_WIDTH}╝"

        # Filter summary
        filter_count = len(self._filters)
        query_filter_count = len(self._query_filters)

        lines = [header, title, version_line, separator]

        # Filter section
        if filter_count > 0:
            lines.append(f"║ {'Active Filters:':<{BOX_WIDTH - 2}} ║")
            for key, value in self._filters.items():
                # Truncate long values for readability
                value_str = str(value)
                max_value_len = BOX_WIDTH - 10 - len(key)  # Account for "║   • key: "
                if len(value_str) > max_value_len:
                    value_str = value_str[:max_value_len - 3] + "..."
                
                content = f"  • {key}: {value_str}"
                lines.append(f"║ {content:<{BOX_WIDTH - 2}} ║")
        else:
            lines.append(f"║ {'No filters applied':<{BOX_WIDTH - 2}} ║")

        # Query filter section (internal)
        if query_filter_count > 0:
            lines.append(f"║ {'':<{BOX_WIDTH - 2}} ║")
            query_info = f"Resolved Query Filters: {query_filter_count}"
            lines.append(f"║ {query_info:<{BOX_WIDTH - 2}} ║")

        lines.append(footer)

        return "\n".join(lines)

    def __repr__(self) -> str:
        """Return a string representation that can reconstruct the instance.

        The representation is a valid Python expression that could be used to
        recreate an equivalent instance with the same filter configuration.

        Returns:
            A string representation suitable for debugging and reconstruction.
        """
        # Format filters for readability
        filters_repr = repr(self._filters) if self._filters else "{}"
        query_filters_repr = repr(self._query_filters) if self._query_filters else "{}"

        return (
            f"{self.__class__.__name__}("
            f"filterset={filters_repr}, "
            f"query_filters={query_filters_repr})"
        )

    def __eq__(self, other) -> bool:
        """Compare two instances for structural equality.

        Two instances are considered equal when they are of the exact same
        runtime type and both their internal _filters and _query_filters dicts
        compare equal.

        Args:
            other: Another object to compare against.

        Returns:
            True if both type and internal filter state match, False otherwise.

        Examples:
            Basic usage:

            .. code-block:: python

                from terndata.ecoplots import EcoPlots

                ec1 = EcoPlots()
                ec1.select(site_id="TCFTNS0002")

                ec2 = EcoPlots()
                ec2.select(site_id="TCFTNS0002")

                ec1 == ec2  # True
        """
        if type(self) is not type(other):  # noqa: PIE789
            return False
        return self._filters == other._filters and self._query_filters == other._query_filters

    def __bool__(self) -> bool:
        """Truthiness of the instance.

        The instance is considered truthy only when it has both human-facing
        _filters and resolved/_query_filters populated. This reflects that a
        fully-formed selection requires both sides.

        Returns:
            True if both _filters and _query_filters are non-empty, False otherwise.

        Examples:
            Basic usage:

            .. code-block:: python

                from terndata.ecoplots import EcoPlots

                ec = EcoPlots()
                bool(ec)  # False

                ec.select(site_id="TCFTNS0002")
                _ = ec.preview()           # or: _ = ec.get_data(); resolves query-side filters
                bool(ec)  # True
        """
        return bool(self._filters) and bool(self._query_filters)

    def __len__(self) -> int:
        """Return the count of selected filter values.

        The length is computed as the total number of individual values across
        all facets in self._filters (i.e. counts values, not facets).

        Returns:
            Total number of selected filter values (int).

        Examples:
            Basic usage:

            .. code-block:: python

                from terndata.ecoplots import EcoPlots

                ec = EcoPlots()
                ec.select(site_id=["TCFTNS0002"], dataset="TERN Surveillance")
                len(ec)  # 2
        """
        return sum(len(v) for v in self._filters.values())

    def __copy__(self):
        """Create a shallow copy of the instance.

        The returned instance is of the same concrete type and receives shallow
        copies of the _filters and _query_filters mappings. This is useful for
        small modifications without affecting the original instance.

        Returns:
            A new instance (same type) with shallow-copied internal state.

        Examples:
            Basic usage:

            .. code-block:: python

                import copy
                from terndata.ecoplots import EcoPlots

                ec1 = EcoPlots().select(site_id="TCFTNS0002")
                ec2 = copy.copy(ec1)
                ec1 is ec2        # False
                ec1 == ec2        # True (same filter state)
        """
        return type(self)(
            filterset=copy.copy(self._filters),
            query_filters=copy.copy(self._query_filters),
        )

    def __deepcopy__(self, memo):
        """Create a deep copy of the instance.

        Uses copy.deepcopy on the internal mappings to ensure no shared
        references remain between the new and original instance.

        Args:
            memo: Memoization dictionary passed by the copy protocol.

        Returns:
            A new instance (same type) with deeply-copied internal state.

        Examples:
            Basic usage:

            .. code-block:: python

                import copy
                from terndata.ecoplots import EcoPlots

                ec1 = EcoPlots().select(site_id=["TCFTNS0002", "TCFTNS0003"])
                ec2 = copy.deepcopy(ec1)
                ec1 is ec2        # False
                ec1 == ec2        # True
        """
        return type(self)(
            filterset=copy.deepcopy(self._filters, memo),
            query_filters=copy.deepcopy(self._query_filters, memo),
        )

    def __contains__(self, item: str) -> bool:
        """Check if a facet is currently applied.

        This method first validates the facet name against QUERY_FACETS. For
        internal consistency it returns True only if the facet exists in both
        the human-visible _filters and the resolved _query_filters. Raises a
        KeyError for unknown facets.

        Args:
            item: Facet name to check.

        Returns:
            True if the facet is present in both _filters and _query_filters.

        Raises:
            KeyError: If the provided facet name is not known.

        Examples:
            Basic usage:

            .. code-block:: python

                ec = EcoPlots().select(site_id="TCFTNS0002")
                _ = ec.preview()        # ensure resolution

                "site_id" in ec  # True

                # The following raises KeyError (unknown facet):
                # "not_a_facet" in ec
        """
        if item not in QUERY_FACETS:
            raise KeyError(f"Invalid key `{item}`. Allowed: " + ", ".join(QUERY_FACETS))
        return item in self._filters and item in self._query_filters

    def __getitem__(self, item: str) -> list:
        """Retrieve the human-visible values for a given facet.

        Validates the facet name and ensures it is present in the current
        instance. This is an internal accessor that returns the list of values
        stored in _filters for the facet.

        Args:
            item: Facet name to retrieve.

        Returns:
            List of values for the facet.

        Raises:
            KeyError: If facet is invalid or not present in current filters.

        Examples:
            Basic usage:

            .. code-block:: python

                from terndata.ecoplots import EcoPlots

                ec = EcoPlots().select(site_id=["TCFTNS0002", "TCFTNS0003"])
                ec["site_id"]  # ["TCFTNS0002", "TCFTNS0003"]
        """
        if item not in QUERY_FACETS:
            raise KeyError(f"Invalid key `{item}`. Allowed: " + ", ".join(QUERY_FACETS))
        if item not in self._filters:
            raise KeyError(f"Key `{item}` not present in current instance.")
        return self._filters.get(item, [])

    def __setitem__(self, facet: str, values: Union[str, list[str]]):
        """Assign values to a facet (delegates to select).

        This operator-style API is a convenience wrapper that validates the
        facet is allowed and then forwards the work to select(), which handles
        normalization and validation.

        Args:
            facet: Facet name to set (must be listed in QUERY_FACETS).
            values: Single value or iterable of values to apply to the facet.

        Raises:
            KeyError: If the facet is not allowed.

        Examples:
            Basic usage:

            .. code-block:: python

                from terndata.ecoplots import EcoPlots

                ec = EcoPlots()
                ec["site_id"] = "TCFTNS0002"
                ec["site_id"] = ["TCFTNS0002", "TCFTNS0003"]
                ec["dataset"] = "TERN Surveillance"
        """
        if facet not in QUERY_FACETS:
            raise KeyError(f"Invalid key `{facet}`. Allowed: " + ", ".join(QUERY_FACETS))
        self.select(filters={facet: values})

    def __delitem__(self, key) -> None:
        """Delete a facet or specific values from a facet (delegates to remove).

        The method validates provided facet names and delegates to remove(),
        raising appropriate errors for unknown facets or malformed keys.

        Args:
            key: Either a single facet name (str) or a tuple (facet, values).

        Raises:
            KeyError: If the facet is unknown or tuple form is malformed.

        Examples:
            Basic usage:

            .. code-block:: python

                from terndata.ecoplots import EcoPlots

                ec = EcoPlots()
                del ec['site_id']                       # remove entire facet
                del ec['site_id', 'TCFTNS0002']         # remove a single value
                del ec['site_id', ['A','B','C']]        # remove multiple values
        """
        if isinstance(key, tuple):
            if len(key) != 2:
                raise KeyError("Expected ('facet', values) for value deletion.")
            facet, values = key
            # Allow human/canonical names here by resolving to canonical
            if facet not in QUERY_FACETS:
                raise KeyError(f"Unknown facet {facet!r}. Allowed: {', '.join(QUERY_FACETS)}")
            # Delegate; remove() expects canonical keys, same as select()
            self.remove(filters={facet: values})
        else:
            if key not in QUERY_FACETS:
                raise KeyError(f"Unknown facet {key!r}. Allowed: {', '.join(QUERY_FACETS)}")
            self.remove(filters={key: None})

    def select(self: SelfType, filters: Optional[dict] = None, **kwargs) -> SelfType:
        """Add/merge filters and validate them.

        Accepts either a dict or keyword arguments.

        Args:
            filters: Mapping like ``{"site_id": [...], "dataset": [...]}``.
            **kwargs: Alternative way to pass filters, e.g. ``site_id="ABC"``.

        Raises:
            ValueError: Unknown filter keys.
            ValueError: ``region`` provided without current or new ``region_type``.

        Returns:
            self for chaining.
        """
        # print(f"Current filters: {self._filters}")  # Debugging output
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
                self._filters["spatial"] = v
                continue
            if not isinstance(v, (list, tuple)):
                v = [v]
            if k in self._filters:
                self._filters[k].extend(list(v))
            else:
                self._filters[k] = list(v)

        # 4. Validate filters
        self._validate_filters()

        # print(f"Filters updated: {self._filters}")  # Debugging output

        return self

    def remove(self: SelfType, filters: Optional[dict] = None, **kwargs) -> SelfType:
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
            self (chainable)
        """
        # Merge inputs (dict + kwargs), exactly like select()
        input_filters = {}
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
                raise ValueError(
                    "Cannot remove specific values from 'spatial' filter; "
                    "use None to clear entire facet."
                )

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

    def clear(self: SelfType) -> SelfType:
        """Clear all filters from the instance.

        The method mutates the instance and returns it to allow fluent/chained calls.

        Returns:
            self (chainable)
        """
        self._filters = {}
        self._query_filters = {}
        return self

    def get_filter(self, facet: Optional[str] = None) -> Union[list, dict, None]:
        """Return the current filter values for a specific facet or all applied filters.

        Args:
            facet: The facet to retrieve the filter for. Defaults to All.

        Raises:
            ValueError: If an invalid facet name is provided.

        Returns:
            The current filter values for the specified facet as list.
            Returns a JSON string of all filters if facet is None.
        """
        if facet:
            facet_val = resolve_facet(facet, QUERY_FACETS)
            if facet_val:
                return self._filters.get(facet_val)
            raise ValueError(
                f"Invalid facet name `{facet}`. Allowed facets: " + ", ".join(QUERY_FACETS)
            )

        return self._filters

    def get_api_query_filters(self, facet: Optional[str] = None) -> Union[list, dict, None]:
        """Return the current query filters for ecoplots API for a specified facet or all facet.

        Args:
            facet: The facet to retrieve the query filters for. Defaults to None.

        Raises:
            ValueError: If an invalid facet name is provided.

        Returns:
            A dictionary of the current query filters.
            Returns a JSON string of all query filters if facet is None.
        """
        if facet:
            facet_val = resolve_facet(facet, QUERY_FACETS)
            if facet_val:
                return self._query_filters.get(facet_val)
            raise ValueError(
                f"Invalid facet name `{facet}`. Allowed facets: " + ", ".join(QUERY_FACETS)
            )

        return self._query_filters

    def discover(
        self,
        discovery_facet: str,
        region_type: Optional[str] = None,
    ) -> dict:
        """Resolve and call the discovery endpoint for a facet.

        Args:
            discovery_facet: Facet to discover (must resolve via configured discovery facets).
            region_type: Optional region type used when discovering regions.

        Returns:
            Parsed JSON payload returned by the discovery endpoint.

        Raises:
            ValueError: If the facet cannot be resolved.

        Notes:
            - Internal use only
            - A 10-second request timeout is enforced.
        """

        facet_param = resolve_facet(discovery_facet, DISCOVERY_FACETS)

        if not facet_param:
            raise ValueError(f"Invalid discovery facet: {discovery_facet}")

        if facet_param == "region" and region_type:
            region_type_val = resolve_region_type(region_type)
            url = f"{self._base_url}/api/v1.0/discovery/{facet_param}?region_type={region_type_val}"
        else:
            url = f"{self._base_url}/api/v1.0/discovery/{facet_param}"

        payload = {"query": copy.deepcopy(self._query_filters)}

        resp = requests.post(url, json=payload, timeout=10)
        resp.raise_for_status()
        return orjson.loads(resp.content)

    async def fetch_data(
        self,
        page_number: Optional[int] = None,
        page_size: Optional[int] = None,
        dformat: str = "geojson",
        **extras,
    ) -> dict:
        """Fetch data for the current query, optionally paginated.

        Posts the current query filters to `EcoPlots API` data endpoint.

        Args:
            page_number: Page index to request. Must be provided together with ``page_size``.
            page_size: Number of items per page. Must be provided together with ``page_number``.
            dformat: Output format, either "geojson" (default) or "csv".
            **extras: Additional query filters to merge into the current query

        Returns:
            Parsed JSON payload (GeoJSON) returned by the data endpoint.

        Raises:
            ValueError: If an invalid dformat is provided.

        Notes:
            - Timeout is 60s when pagination is used; otherwise 3000s.
            - Intended for internal use only.
        """
        if dformat not in ("geojson", "csv"):
            raise ValueError("dformat must be one of 'geojson' or 'csv'")

        payload = {
            "query": copy.deepcopy(self._query_filters),
            "page_number": page_number,
            "page_size": page_size,
        }

        if extras and isinstance(payload["query"], dict):
            payload["query"].update(extras)

        if page_number and page_size:
            payload.update({"page_number": page_number, "page_size": page_size})
            timeout = aiohttp.ClientTimeout(total=60)
        else:
            del payload["page_number"]
            del payload["page_size"]
            timeout = aiohttp.ClientTimeout(total=3000)

        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with session.post(
                f"{self._base_url}/api/v1.0/data?dformat={dformat}", json=payload
            ) as resp:
                resp.raise_for_status()
                data = await resp.read()
                return data if dformat == "csv" else orjson.loads(data)

    def discover_attributes(
        self,
        discovery_attribute: str,
    ) -> dict:
        """Discovers attribute URIs for a given entity type.

        Args:
            discovery_attribute: Attribute namespace to discover
                (must resolve via configured discovery attributes).

        Returns:
            Parsed JSON payload containing attribute identifiers.

        Raises:
            ValueError: If the attribute cannot be resolved.

        Notes:
            - A 30-second request timeout is enforced.
            - Intended for internal use only.
        """
        facet_param = resolve_facet(discovery_attribute, DISCOVERY_ATTRIBUTES)

        if not facet_param:
            raise ValueError(f"Invalid discovery facet: {discovery_attribute}")

        url = f"{self._base_url}/api/v1.0/discovery/attributes"

        payload = {"query": copy.deepcopy(self._query_filters)}

        params = [("type", facet_param)]

        resp = requests.post(url, params=params, json=payload, timeout=30)
        resp.raise_for_status()
        return orjson.loads(resp.content)

    def summarise_data(self, query_filters: Optional[dict] = None) -> dict:
        """Request a lightweight summary for the given or current query filters.

        Args:
            query_filters: Canonical filters to use for the summary. When omitted, the
                instance's current canonical query filters are used.

        Returns:
            Parsed JSON payload containing counts and related summary fields.

        Notes:
            - A 30-second request timeout is enforced.
            - Intended for internal use only.
        """
        payload = {
            "query": (
                copy.deepcopy(query_filters)
                if (query_filters is not None)
                else copy.deepcopy(self._query_filters)
            ),
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

    def save(self, path: Optional[Union[str, Path]] = None) -> str:
        """Save project state to a single `.ecoproj` file (atomic, checksummed).

        Writes the current `filters` and `query_filters` into a compact binary file
        with a small header and a JSON (orjson) payload. The filename resolution is:

        - If `path` is `None`: save as `./ecoplots_<UTCSTAMP>.ecoproj`.
        - If `path` has no `.ecoproj` suffix and no parent directory: save as
            `./<name>.ecoproj` in the current working directory.
        - If `path` ends with `.ecoproj`: save exactly to that location.

        Args:
            path: Optional target path or bare name. If omitted, a timestamped
                filename is created in the current working directory.

        Returns:
            Absolute path to the saved `.ecoproj` file.

        Raises:
            Exception: Any unexpected error during the write; temporary files are
                cleaned up best-effort before re-raising.
        """

        target = _ensure_ecoproj_path(path).resolve()
        target.parent.mkdir(parents=True, exist_ok=True)

        payload = {"filters": self._filters, "query_filters": self._query_filters}
        body = orjson.dumps(payload, option=orjson.OPT_SORT_KEYS)
        sha = hashlib.sha256(body).digest()

        # Header: MAGIC (4) | VERSION (1) | SHA256 (32) | LEN (8, big-endian)
        tmp = tempfile.NamedTemporaryFile(delete=False, suffix=".ecoproj")
        tmp_path = Path(tmp.name)

        try:
            with open(tmp_path, "wb") as f:
                f.write(MAGIC)
                f.write(struct.pack(">B", VERSION))
                f.write(sha)
                f.write(struct.pack(">Q", len(body)))
                f.write(body)
            # Atomic replace
            target.replace(target) if target.exists() else None
            tmp_path.replace(target)
        except Exception as e:
            tmp_path.unlink(missing_ok=True)
            raise e
        finally:
            if tmp_path.exists():
                tmp_path.unlink(missing_ok=True)

        return str(target)

    @classmethod
    def load(cls: type[SelfType], path: Union[str, Path]) -> SelfType:
        """Load a `.ecoproj` file, validate integrity, and return a new instance.

        Args:
            path: Path to a `.ecoproj` file previously created by :meth:`save`.

        Returns:
            A new instance of `EcoPlots` with `filters` and `query_filters` restored.

        Raises:
            FileNotFoundError: If the file does not exist.
            ValueError: If the file does not have a `.ecoproj` suffix, the magic
                header or version is invalid, the file is truncated, or the checksum
                does not match the payload.
        """
        p = Path(path).resolve()
        if not p.exists():
            raise FileNotFoundError(str(p))
        if p.suffix != ".ecoproj":
            raise ValueError(f"Expected a '.ecoproj' file, got: {p.name}")

        with open(p, "rb") as f:
            if f.read(4) != MAGIC:
                raise ValueError("Invalid project file (bad magic).")
            ver = struct.unpack(">B", f.read(1))[0]
            if ver != VERSION:
                raise ValueError(f"Incompatible project version: {ver} (expected {VERSION}).")
            sha = f.read(32)
            n = struct.unpack(">Q", f.read(8))[0]
            body = f.read(n)
            if len(body) != n:
                raise ValueError("Truncated project file.")
            if hashlib.sha256(body).digest() != sha:
                raise ValueError("Project integrity check failed (checksum mismatch).")

        data = orjson.loads(body)

        return cls(filterset=data.get("filters", {}), query_filters=data.get("query_filters", {}))

    def _validate_filters(self) -> bool:
        """Validate filters in parallel for all facets. Not for direct user use.

        Returns:
            `True` if filters validate and the summary indicates one or more
            matching records; `False` if validation succeeded but the selection
            yields zero records (a warning is issued).

        Raises:
            ValueError: If any facet contains values that cannot be matched.

        Notes:
            - Intended for internal use only.
        """

        query_filters = copy.deepcopy(self._query_filters)
        all_unmatched: dict = {}
        all_matched = copy.deepcopy(self._filters)

        if "spatial" in all_matched:
            query_filters["spatial"] = all_matched["spatial"]

        to_validate = {k: v for k, v in self._filters.items() if k != "spatial"}

        with ThreadPoolExecutor() as executor:
            futures = {
                # NOTE: `validate_facet` uses rapidfuzz under the hood,
                # rapidfuzz itself releases the GIL (written in C++),
                # so we can leverage "true" parallelism here with ThreadPoolExecutor
                # for CPU bound fuzzy matching and is much faster than asyncio.gather.
                executor.submit(validate_facet, facet, value): facet
                for facet, value in to_validate.items()
            }

            for future in as_completed(futures):
                facet, urls, matched, unmatched, corrected = future.result()

                # Convert to set for updating
                existing = set(query_filters.get(facet, []))
                existing.update(urls)
                query_filters[facet] = list(existing)

                all_matched.setdefault(facet, [])
                # ensure corrected values are excluded
                all_matched[facet] = [x for x in matched if x not in corrected]
                # for val in filtered_matched:
                #     if val not in all_matched[facet]:
                #         all_matched[facet].append(val)

                if unmatched:
                    all_unmatched.setdefault(facet, [])
                    all_unmatched[facet].extend(unmatched)

            # convert sets to lists
            # query_filters = {facet: list(urls) for facet, urls in query_filters.items()}

        if all_unmatched:
            msg = "The following filter values could not be matched:\n" + "\n".join(
                f"Facet '{facet}': {unmatched}" for facet, unmatched in all_unmatched.items()
            )
            raise ValueError(msg)

        data = self.summarise_data(query_filters=query_filters)
        if data.get("total_doc", 0) == 0:
            warnings.warn(
                "The applied filters result in zero matching records. "
                "Please adjust your filters. Skipping current selection...",
                UserWarning,
                stacklevel=3,
            )
            return False

        self._query_filters = query_filters
        self._filters = all_matched

        return True
