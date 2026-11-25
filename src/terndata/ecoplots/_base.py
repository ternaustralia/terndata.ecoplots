import asyncio
import codecs
import copy
import hashlib
import struct
import sys
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

import orjson
import requests
from diskcache import Cache
from rapidfuzz import fuzz, process

from ._config import (
    API_BASE_URL,
    CACHE_DIR,
    DISCOVERY_ATTRIBUTES,
    DISCOVERY_FACETS,
    EQUIPMENT_FACETS,
    EQUIPMENT_GROUPS,
    FACET_ALIASES,
    FACET_API_NAMES,
    MAGIC,
    MATERIAL_SAMPLE_TYPE_MAP,
    QUERY_FACETS,
    SAMPLE_QUERY_FACETS,
    VERSION,
)
from ._exceptions import EcoPlotsError
from ._jobs import _aiohttp, _csv_bytes, async_submit_jobs, submit_jobs
from ._nlp_utils import (
    resolve_facet,
    resolve_region_type,
    validate_facet,
)
from ._utils import _ensure_ecoproj_path, _parse_date, _validate_spatial_input
from ._vocab_cache import wait_for_vocabularies

SelfType = TypeVar("SelfType", bound="EcoPlotsBase")
PROJECT_FILE_VERSION = 1
ALLOWED_MODES = ("observations", "samples")
MODE_ALIASES = {
    "observation": "observations",
    "observations": "observations",
    "obs": "observations",
    "sample": "samples",
    "samples": "samples",
}


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
        mode: str = "observations",
    ):
        """Initialize an EcoPlotsBase instance.

        The constructor only sets up internal state. Validation of filters is
        performed lazily by calls to select()/remove() which in turn call
        _validate_filters().

        Args:
            filterset: Optional mapping of facet -> list of human/canonical values
                to pre-populate the instance.
            query_filters: Optional mapping of facet -> list of API-ready values
                (eg. URLs) to pre-populate the instance.
            mode: Operational mode, either "observations" (default) or "samples".
        """
        self._base_url = API_BASE_URL
        self._mode = self._resolve_mode(mode)
        self._filters = self._normalize_filter_keys(filterset or {})
        self._query_filters = query_filters or {}
        # In samples mode we enforce a persistent dataset selection which
        # cannot be removed by the user. Ensure both human-facing and
        # query-side filters contain the required values.
        if self._mode == "samples":
            persistent_label = "TERN Ecosystem Surveillance"
            persistent_uri = "http://linked.data.gov.au/dataset/ausplots"

            if "dataset" not in self._filters:
                self._filters["dataset"] = [persistent_label]
            else:
                if persistent_label not in self._filters["dataset"]:
                    self._filters["dataset"].insert(0, persistent_label)

            if "dataset" not in self._query_filters:
                self._query_filters["dataset"] = [persistent_uri]
            else:
                if persistent_uri not in self._query_filters["dataset"]:
                    self._query_filters["dataset"].insert(0, persistent_uri)

        self._display_init_message()

    @staticmethod
    def _normalize_filter_keys(filters: dict) -> dict:
        """Expose grouped equipment selections as ordinary public facets."""
        normalized: dict = {}

        def merge(facet, value):
            if facet in normalized and normalized[facet] is not None and value is not None:
                existing = normalized[facet]
                existing = list(existing) if isinstance(existing, (list, tuple)) else [existing]
                values = list(value) if isinstance(value, (list, tuple)) else [value]
                normalized[facet] = existing + [item for item in values if item not in existing]
            else:
                normalized[facet] = value

        for key, value in filters.items():
            facet = FACET_ALIASES.get(key, key)
            if facet in EQUIPMENT_GROUPS and isinstance(value, dict):
                for child, values in value.items():
                    public_facet = EQUIPMENT_GROUPS[facet].get(child)
                    if public_facet:
                        merge(public_facet, values)
            else:
                merge(facet, value)
        return normalized

    def _api_filter_values(self, facet):
        """Read a public facet from either grouped or legacy API state."""
        if facet in EQUIPMENT_FACETS:
            group, child, _ = EQUIPMENT_FACETS[facet]
            return self._query_filters.get(group, {}).get(child)
        if facet in EQUIPMENT_GROUPS and facet not in self._filters:
            return self._query_filters.get(facet)
        return self._query_filters.get(FACET_API_NAMES.get(facet, facet))

    @staticmethod
    def _resolve_mode(mode: str) -> str:
        """Return a canonical mode or raise for unknown modes."""
        if not isinstance(mode, str):
            raise EcoPlotsError("Invalid mode. Allowed modes are: " + ", ".join(ALLOWED_MODES))

        normalized = mode.strip().lower().replace("-", " ").replace("_", " ")
        if normalized.endswith(" mode"):
            normalized = normalized[: -len(" mode")].strip()
        normalized = normalized.replace(" ", "_")

        alias = MODE_ALIASES.get(normalized)
        if alias:
            return alias

        match = process.extractOne(normalized, ALLOWED_MODES, scorer=fuzz.WRatio)
        if match and match[1] >= 80:
            return match[0]

        suggestion = f" Did you mean '{match[0]}'?" if match and match[1] >= 70 else ""
        raise EcoPlotsError(
            f"Invalid mode {mode!r}.{suggestion} Allowed modes are: " + ", ".join(ALLOWED_MODES)
        )

    def _display_init_message(self) -> None:
        """Print a concise mode-specific initialization message."""
        if self._mode == "samples":
            print(  # noqa: T201
                f"{self.__class__.__name__} initialized in samples mode. "
                "The TERN Ecosystem Surveillance dataset filter is applied automatically."
            )
        else:
            print(f"{self.__class__.__name__} initialized in observations mode.")  # noqa: T201

    @staticmethod
    def _display_warning(message: str) -> None:
        """Display a clean, formatted warning message in Jupyter/IPython environments.

        This method provides a cleaner alternative to Python's default warnings.warn()
        which includes verbose file paths and line numbers. In Jupyter notebooks,
        it prints a styled warning message directly.

        Args:
            message: The warning message to display.
        """
        # Check if we're in IPython/Jupyter
        try:
            get_ipython  # type: ignore  # noqa: F821
            # In Jupyter/IPython - use clean print with styling
            print(f"\n⚠️  Warning: {message}\n", file=sys.stderr)  # noqa: T201
        except NameError:
            # Not in IPython - use standard warnings
            warnings.warn(message, UserWarning, stacklevel=4)

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
            ║ EcoPlots Observations                                                        ║
            ║ Version: 1.0.0                                                               ║
            ╠══════════════════════════════════════════════════════════════════════════════╣
            ║ Active Filters:                                                              ║
            ║   • site_id: TCFTNS0002                                                      ║
            ╚══════════════════════════════════════════════════════════════════════════════╝
        """
        # Box drawing constants
        BOX_WIDTH = 78
        INDENT = "    "  # 4 spaces for continuation lines

        # Header with decorative separator
        header = f"╔{'═' * BOX_WIDTH}╗"
        mode_width = BOX_WIDTH - 2 - len(self.__class__.__name__) - 1
        title = f"║ {self.__class__.__name__} {self._mode.capitalize():<{mode_width}} ║"
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
                value_str = str(value)

                # First line: "  • key: "
                prefix = f"  • {key}: "
                max_first_line = BOX_WIDTH - 2 - len(prefix)

                if len(value_str) <= max_first_line:
                    # Fits on one line
                    content = f"{prefix}{value_str}"
                    lines.append(f"║ {content:<{BOX_WIDTH - 2}} ║")
                else:
                    # Needs wrapping
                    # First line
                    first_chunk = value_str[:max_first_line]
                    content = f"{prefix}{first_chunk}"
                    lines.append(f"║ {content:<{BOX_WIDTH - 2}} ║")

                    # Continuation lines
                    remaining = value_str[max_first_line:]
                    max_cont_line = BOX_WIDTH - 2 - len(INDENT)

                    while remaining:
                        chunk = remaining[:max_cont_line]
                        remaining = remaining[max_cont_line:]
                        content = f"{INDENT}{chunk}"
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
            f"{self.__class__.__name__} {repr(self._mode)}("
            f"filterset={filters_repr}, "
            f"query_filters={query_filters_repr}, "
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
        item = FACET_ALIASES.get(item, item)
        if item not in QUERY_FACETS:
            raise KeyError(f"Invalid key `{item}`. Allowed: " + ", ".join(QUERY_FACETS))
        return self.get_filter(item) is not None and self._api_filter_values(item) is not None

    def __getitem__(self, item: str) -> Union[list, dict]:
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
        item = FACET_ALIASES.get(item, item)
        if item not in QUERY_FACETS:
            raise KeyError(f"Invalid key `{item}`. Allowed: " + ", ".join(QUERY_FACETS))
        values = self.get_filter(item)
        if values is None:
            raise KeyError(f"Key `{item}` not present in current instance.")
        return values

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
        facet = FACET_ALIASES.get(facet, facet)
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
        allowed_facets = SAMPLE_QUERY_FACETS if self._mode == "samples" else QUERY_FACETS

        if isinstance(key, tuple):
            if len(key) != 2:
                raise KeyError("Expected ('facet', values) for value deletion.")
            facet, values = key
            facet = FACET_ALIASES.get(facet, facet)
            # Allow human/canonical names here by resolving to canonical
            if facet not in allowed_facets:
                raise KeyError(f"Unknown facet {facet!r}. Allowed: {', '.join(allowed_facets)}")
            # Delegate; remove() expects canonical keys, same as select()
            self.remove(filters={facet: values})
        else:
            key = FACET_ALIASES.get(key, key)
            if key not in allowed_facets:
                raise KeyError(f"Unknown facet {key!r}. Allowed: {', '.join(allowed_facets)}")
            self.remove(filters={key: None})

    def select(self: SelfType, filters: Optional[dict] = None, **kwargs) -> SelfType:
        """Add/merge filters and validate them.

        Accepts either a dict or keyword arguments.

        Args:
            filters: Mapping like ``{"site_id": [...], "dataset": [...]}``.
            **kwargs: Alternative way to pass filters, e.g. ``site_id="ABC"``.
                Special keyword filters (handled separately from facet resolution):

                - ``spatial``: WKT string or GeoJSON geometry ``dict`` to
                  spatially restrict results to a custom region.
                - ``has_image`` (``bool``, *samples* mode only): Limit to
                  samples that have attached images.
                - ``soil_subsite_id`` (``int`` or ``list[int]``, *samples* mode
                  only): Restrict to specific soil sub-site identifiers.
                - ``soil_depth_range`` (``[min, max]`` or
                  ``{"min": x, "max": y}``, *samples* mode only): Filter
                  samples by soil depth in metres.
                - ``date_from`` (``str``): Earliest date (inclusive) in any
                  recognisable format — ``"DD/MM/YYYY"``, ``"21 May 2020"``,
                  ``"21st May 2020"``, ``"YYYY-MM-DD"`` etc. Normalised to
                  ``"YYYY-MM-DD"`` internally. Day-first is assumed for
                  all-numeric inputs (``MM-DD-YYYY`` is never accepted).
                - ``date_to`` (``str``): Latest date (inclusive), same format
                  rules as ``date_from``.

        Raises:
            EcoPlotsError: Unknown filter keys.
            EcoPlotsError: ``region`` provided without current or new ``region_type``.
            Exception: Vocabulary or API errors are propagated after restoring
                the prior filter state.

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

        input_filters = self._normalize_filter_keys(input_filters)
        if self._mode == "samples":
            alias_map = {
                "soil_subsite": "soil_subsite_id",
            }
            normalized_input = {}
            for key, value in input_filters.items():
                canonical_key = alias_map.get(key, key)
                if canonical_key in normalized_input and value is not None:
                    existing = normalized_input[canonical_key]
                    if isinstance(existing, list):
                        if isinstance(value, (list, tuple)):
                            existing.extend(list(value))
                        else:
                            existing.append(value)
                        normalized_input[canonical_key] = existing
                    else:
                        normalized_input[canonical_key] = value
                else:
                    normalized_input[canonical_key] = value
            input_filters = normalized_input

        # 1. Determine allowed facets based on mode
        allowed_facets = SAMPLE_QUERY_FACETS if self._mode == "samples" else QUERY_FACETS

        # 2. Validate allowed keys
        invalid_keys = set(input_filters) - set(allowed_facets)
        if invalid_keys:
            raise EcoPlotsError(f"Invalid filter keys: {invalid_keys}. Allowed: {allowed_facets}")

        for facet, value in input_filters.items():
            if facet in EQUIPMENT_FACETS and value is not None:
                values = list(value) if isinstance(value, (list, tuple)) else [value]
                if not all(isinstance(item, str) for item in values):
                    raise EcoPlotsError(f"'{facet}' must be a string or list of strings.")

        # 3. Validate region logic
        if "region" in input_filters:
            region_type_now = "region_type" in input_filters
            region_type_before = "region_type" in self._filters
            if not (region_type_now or region_type_before):
                raise EcoPlotsError("'region_type' must be provided before or with 'region'.")

        # 4. Save current state for potential rollback
        filters_backup = copy.deepcopy(self._filters)
        query_backup = copy.deepcopy(self._query_filters)

        # 5. Merge filters (always as list)
        for k, v in input_filters.items():
            if v is None:
                continue
            if k == "has_image":
                if self._mode != "samples":
                    raise EcoPlotsError("'has_image' filter is only available in 'samples' mode.")
                if isinstance(v, (list, tuple)):
                    if len(v) != 1:
                        raise EcoPlotsError("'has_image' accepts a single boolean value.")
                    v = v[0]
                if not isinstance(v, bool):
                    raise EcoPlotsError("'has_image' must be a boolean (True/False).")
                self._filters["has_image"] = v
                self._query_filters["has_image"] = v
                continue
            if k == "spatial":
                _validate_spatial_input(v)  # validate spatial filter
                # replace any existing spatial filter
                self._filters["spatial"] = v
                continue
            if k == "soil_subsite_id":
                if self._mode != "samples":
                    raise EcoPlotsError(
                        "'soil_subsite_id' filter is only available in 'samples' mode."
                    )
                vals = list(v) if isinstance(v, (list, tuple)) else [v]
                normalized_subsite_ids = []
                for raw in vals:
                    try:
                        subsite_id = int(raw)
                    except (TypeError, ValueError) as exc:
                        raise EcoPlotsError(
                            "'soil_subsite_id' must be an integer or list of integers."
                        ) from exc
                    normalized_subsite_ids.append(subsite_id)

                existing_ids = self._filters.get("soil_subsite_id", [])
                if not isinstance(existing_ids, list):
                    existing_ids = [existing_ids]
                merged = list(existing_ids)
                for sid in normalized_subsite_ids:
                    if sid not in merged:
                        merged.append(sid)
                self._filters["soil_subsite_id"] = merged
                continue
            if k == "soil_depth_range":
                if self._mode != "samples":
                    raise EcoPlotsError(
                        "'soil_depth_range' filter is only available in 'samples' mode."
                    )

                min_depth = None
                max_depth = None

                if isinstance(v, dict):
                    min_depth = v.get("min")
                    max_depth = v.get("max")
                elif isinstance(v, (list, tuple)) and len(v) == 2:
                    min_depth, max_depth = v
                else:
                    raise EcoPlotsError(
                        "'soil_depth_range' must be [min, max], (min, max), "
                        "or {'min': x, 'max': y}."
                    )

                try:
                    min_depth = float(min_depth)
                    max_depth = float(max_depth)
                except (TypeError, ValueError) as exc:
                    raise EcoPlotsError(
                        "'soil_depth_range' min/max must be numeric values."
                    ) from exc

                if max_depth <= min_depth:
                    raise EcoPlotsError("Invalid 'soil_depth_range': max must be greater than min.")

                self._filters["soil_depth_range"] = [min_depth, max_depth]
                continue
            if k in ("date_from", "date_to"):
                parsed = _parse_date(v if isinstance(v, str) else str(v))
                self._filters[k] = parsed
                continue
            if not isinstance(v, (list, tuple)):
                v = [v]
            if k in self._filters:
                self._filters[k].extend(list(v))
            else:
                self._filters[k] = list(v)

        # 6. Validate filters - if validation fails (returns False), rollback
        try:
            validation_passed = self._validate_filters()
        except Exception:
            self._filters = filters_backup
            self._query_filters = query_backup
            raise

        if not validation_passed:
            self._filters = filters_backup
            self._query_filters = query_backup

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
            EcoPlotsError: Unknown filter keys (not in ``QUERY_FACETS``).
            EcoPlotsError: If ``dataset`` is targeted while in ``samples`` mode
                (the ``TERN Ecosystem Surveillance`` dataset is protected).
            KeyError: Facet not present in current filters.
            EcoPlotsError: Specific values requested but not found for that facet.

        Returns:
            self (chainable)
        """
        # Merge inputs (dict + kwargs), exactly like select()
        input_filters = {}
        if filters:
            input_filters.update(filters)
        if kwargs:
            input_filters.update(kwargs)
        expanded_filters: dict = {}
        for key, values in input_filters.items():
            group = FACET_ALIASES.get(key, key)
            children = EQUIPMENT_GROUPS.get(group, {})
            applied = [name for name in children.values() if name in self._filters]
            if values is None and applied:
                expanded_filters.update(dict.fromkeys(applied))
                if group in self._filters:
                    expanded_filters[group] = None
            else:
                expanded_filters[key] = values
        input_filters = self._normalize_filter_keys(expanded_filters)
        # 1. Determine allowed facets based on mode
        allowed_facets = SAMPLE_QUERY_FACETS if self._mode == "samples" else QUERY_FACETS

        # 2. Validate allowed keys
        invalid_keys = set(input_filters) - set(allowed_facets)
        if invalid_keys:
            raise EcoPlotsError(f"Invalid filter keys: {invalid_keys}. Allowed: {allowed_facets}")

        # Protect persistent dataset in samples mode
        if self._mode == "samples" and "dataset" in input_filters:
            raise EcoPlotsError(
                "Cannot remove 'dataset' when in 'samples' mode. "
                "The dataset 'TERN Surveillance' is required and protected."
            )

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
                raise EcoPlotsError(
                    "Cannot remove specific values from 'spatial' filter; "
                    "use None to clear entire facet."
                )

            # normalize to a list
            if isinstance(vals, (str, bytes)) or not isinstance(vals, Iterable):
                vals = [vals]
            else:
                vals = list(vals)

            if facet in EQUIPMENT_FACETS:
                _, _, matched, unmatched, _ = validate_facet(facet, vals)
                if unmatched:
                    raise EcoPlotsError(f"Values not found in facet {facet!r}: {unmatched}")
                vals = matched
            existing = self._filters.get(facet, [])
            missing = [v for v in vals if v not in existing]
            if missing:
                raise EcoPlotsError(f"Values not found in facet {facet!r}: {missing}")

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

        Notes:
            In ``samples`` mode the ``TERN Ecosystem Surveillance`` dataset
            filter is preserved; only user-added filters are cleared.
        """
        # Preserve persistent dataset when in samples mode
        if self._mode == "samples":
            self._filters = {"dataset": ["TERN Ecosystem Surveillance"]}
            self._query_filters = {"dataset": ["http://linked.data.gov.au/dataset/ausplots"]}
        else:
            self._filters = {}
            self._query_filters = {}
        return self

    def from_date(self: SelfType, date: str) -> SelfType:
        """Set an earliest-date filter (inclusive).

        Chainable with :meth:`till`. The date string is parsed tolerantly —
        ``"DD/MM/YYYY"``, ``"21 May 2020"``, ``"21st May 2020"``,
        ``"YYYY-MM-DD"`` etc. are all accepted. For all-numeric inputs the
        day-first convention (``DD-MM-YYYY``) is always used.

        Equivalent to ``select(date_from=date)``.

        Args:
            date: Start date in any recognisable human format.

        Returns:
            self (chainable)

        Examples:
            .. code-block:: python

                ec.select(site_id="ABC").from_date("01/01/2020").to_date("31/12/2022")
        """
        return self.select(date_from=date)

    def to_date(self: SelfType, date: str) -> SelfType:
        """Set a latest-date filter (inclusive).

        Chainable with :meth:`from_date`. Accepts the same flexible date
        formats — ``"DD/MM/YYYY"``, ``"31 Dec 2022"``, ``"YYYY-MM-DD"``, etc.

        Equivalent to ``select(date_to=date)``.

        Args:
            date: End date in any recognisable human format.

        Returns:
            self (chainable)

        Examples:
            .. code-block:: python

                ec.select(site_id="ABC").from_date("01/01/2020").to_date("31/12/2022")
        """
        return self.select(date_to=date)

    def get_filter(self, facet: Optional[str] = None) -> Union[list, dict, None]:
        """Return the current filter values for a specific facet or all applied filters.

        Args:
            facet: The facet to retrieve the filter for. Defaults to All.

        Raises:
            EcoPlotsError: If an invalid facet name is provided.

        Returns:
            A list of values for the specified facet, or ``None`` if the facet
            is not currently applied. If *facet* is ``None``, returns a ``dict``
            mapping each applied facet to its list of values.
        """
        if facet:
            allowed_facets = SAMPLE_QUERY_FACETS if self._mode == "samples" else QUERY_FACETS
            facet_val = resolve_facet(facet, allowed_facets)
            if facet_val:
                if facet_val in EQUIPMENT_GROUPS and facet_val not in self._filters:
                    children = {
                        child: self._filters[name]
                        for child, name in EQUIPMENT_GROUPS[facet_val].items()
                        if name in self._filters
                    }
                    return children or None
                return self._filters.get(facet_val)
            raise EcoPlotsError(
                f"Invalid facet name `{facet}`. Allowed facets: " + ", ".join(allowed_facets)
            )

        return self._filters

    def get_api_query_filters(self, facet: Optional[str] = None) -> Union[list, dict, None]:
        """Return the current query filters for ecoplots API for a specified facet or all facet.

        Args:
            facet: The facet to retrieve the query filters for. Defaults to None.

        Raises:
            EcoPlotsError: If an invalid facet name is provided.

        Returns:
            A list of resolved API values for the specified facet, or ``None``
            if the facet is not currently applied. If *facet* is ``None``,
            returns a ``dict`` of all resolved query filters.
        """
        if facet:
            allowed_facets = SAMPLE_QUERY_FACETS if self._mode == "samples" else QUERY_FACETS
            facet_val = resolve_facet(facet, allowed_facets)
            if facet_val:
                return self._api_filter_values(facet_val)
            raise EcoPlotsError(
                f"Invalid facet name `{facet}`. Allowed facets: " + ", ".join(allowed_facets)
            )

        return self._query_filters

    def discover(
        self,
        discovery_facet: str,
        region_type: Optional[str] = None,
        include_region: bool = False,
        query_filters: Optional[dict] = None,
    ) -> dict:
        """Resolve and call the discovery endpoint for a facet.

        Args:
            discovery_facet: Facet to discover (must resolve via configured discovery facets).
            region_type: Optional region type used when discovering regions.
            include_region: If True while discovering sites, request site region
                metadata from the API using ``include-regions=True``.
            query_filters: Optional query filters to use for this request instead
                of the instance's current query filters.

        Returns:
            Parsed JSON payload returned by the discovery endpoint.

        Raises:
            EcoPlotsError: If the facet cannot be resolved.

        Notes:
            - Internal use only
            - A 60-second request timeout is enforced.
        """

        facet_param = resolve_facet(discovery_facet, DISCOVERY_FACETS)

        if not facet_param:
            raise EcoPlotsError(f"Invalid discovery facet: {discovery_facet}")

        if self._mode == "samples" and facet_param in {*FACET_API_NAMES, *EQUIPMENT_FACETS}:
            raise EcoPlotsError(
                f"'{facet_param}' discovery is available in observations mode only."
            )

        params = []
        if facet_param == "region" and region_type:
            region_type_val = resolve_region_type(region_type)
            if region_type_val:
                api_region_type = region_type_val.removeprefix("IBRA7 ").lower().replace(" ", "-")
                params.append(("region_type", api_region_type))
        if facet_param == "site_id" and include_region:
            params.append(("include-regions", "True"))

        api_facet = (
            "platform_instance"
            if facet_param == "platform"
            else FACET_API_NAMES.get(facet_param, facet_param)
        )
        url = f"{self._base_url}/api/v1.0/discovery/{api_facet}"

        payload = {
            "query": (
                copy.deepcopy(query_filters)
                if query_filters is not None
                else copy.deepcopy(self._query_filters)
            )
        }

        resp = requests.post(url, params=params if params else None, json=payload, timeout=60)
        resp.raise_for_status()
        return orjson.loads(resp.content)

    def discover_samples(
        self,
        discovery_facet: str,
        region_type: Optional[str] = None,
    ) -> dict:
        """Resolve and call the discovery endpoint for samples.

        Args:
            discovery_facet: Facet to discover (must resolve via SAMPLE_DISCOVERY_FACETS).
            region_type: Optional region type used when discovering regions.
                Required when discovery_facet is "region".

        Returns:
            Parsed JSON payload returned by the discovery endpoint.

        Raises:
            EcoPlotsError: If the facet cannot be resolved or region_type is missing
                when discovering regions.

        Notes:
            - Internal use only
            - A 60-second request timeout is enforced.
        """
        from ._config import SAMPLE_DISCOVERY_FACETS

        facet_param = resolve_facet(discovery_facet, SAMPLE_DISCOVERY_FACETS)

        if not facet_param:
            raise EcoPlotsError(f"Invalid discovery facet: {discovery_facet}")

        if discovery_facet == "dataset":
            # hardcoded, doesn't change
            return [
                {
                    "key": "TERN Ecosystem Surveillance",
                    "uri": "http://linked.data.gov.au/dataset/ausplots",
                }
            ]

        url = f"{self._base_url}/api/v1.0/ui/facet/samples"

        # Build query with only the facets defined in SAMPLE_DISCOVERY_FACETS
        query = {}
        for facet in SAMPLE_DISCOVERY_FACETS:
            # Skip region_type if we're discovering regions; will be resolved from args
            if discovery_facet == "region" and facet == "region_type":
                continue
            if facet in self._query_filters:
                query[facet] = self._query_filters[facet]

        # Additional query-side sample filters used by dedicated sample discovery endpoints.
        for facet in ("soil_subsite_id", "soil_depth_range", "date_from", "date_to"):
            if facet in self._query_filters:
                query[facet] = self._query_filters[facet]

        # When discovering regions, resolve region_type from args and add to query
        if discovery_facet == "region":
            if not region_type:
                raise EcoPlotsError("region_type is required when discovering regions")
            facet, urls, matched, unmatched, corrected = validate_facet(
                "region_type", [region_type]
            )
            if urls:
                query["region_type"] = urls
            else:
                raise EcoPlotsError(f"Could not resolve region_type: {region_type}")

        payload = {"query": query, "has_image": self._query_filters.get("has_image", False)}

        params = []
        if discovery_facet == "sample_name":
            params.append(("facet", facet_param))

        resp = requests.post(
            url,
            params=params if params else None,
            json=payload,
            timeout=60,
        )
        resp.raise_for_status()

        # Parse JSON from response text
        parsed = orjson.loads(resp.text)

        if discovery_facet == "region_type":
            # Ensure we have a list
            parsed = parsed["aggregations"]["region_type"]["buckets"]
            if not isinstance(parsed, list):
                return []

            # Remove doc_count and add uri from cache
            wait_for_vocabularies()
            with Cache(CACHE_DIR) as cache:
                region_type_map = cache.get("region_type", {})
                for res in parsed:
                    if isinstance(res, dict):
                        res.pop("doc_count", None)
                        key = res.get("key")
                        res["uri"] = key
                        res["key"] = region_type_map.get(key, "N/A")

        elif discovery_facet == "region":
            # Ensure we have a list
            parsed = parsed["aggregations"]["region"]["buckets"]
            if not isinstance(parsed, list):
                return []

            # Remove doc_count and add uri from cache
            wait_for_vocabularies()
            with Cache(CACHE_DIR) as cache:
                region_map = cache.get("region", {})
                for res in parsed:
                    if isinstance(res, dict):
                        res.pop("doc_count", None)
                        key = res.get("key")
                        res["uri"] = key
                        res["key"] = region_map.get(key, "N/A")

        elif discovery_facet == "material_sample_type":
            parsed = parsed["aggregations"][facet_param]["value"]["buckets"]
            if not isinstance(parsed, list):
                return []

            for res in parsed:
                if isinstance(res, dict):
                    res.pop("doc_count", None)
                    key = res.get("key")
                    res["uri"] = key
                    res["key"] = MATERIAL_SAMPLE_TYPE_MAP.get(key, "N/A")

        elif discovery_facet in ("site_id", "used_procedure"):
            parsed = parsed["aggregations"][facet_param]["value"]["buckets"]
            if not isinstance(parsed, list):
                return []

            wait_for_vocabularies()
            with Cache(CACHE_DIR) as cache:
                facet_map = cache.get(facet_param, {})
                for res in parsed:
                    if isinstance(res, dict):
                        res.pop("doc_count", None)
                        key = res.get("key")
                        res["uri"] = key
                        res["key"] = facet_map.get(key, "N/A")

        elif discovery_facet == "sample_name":
            parsed = parsed["aggregations"][facet_param]["value"]["buckets"]
            if not isinstance(parsed, list):
                return []

            for res in parsed:
                if isinstance(res, dict):
                    res.pop("doc_count", None)

        return parsed

    def discover_soil_depth_range(self):
        """Discover soil depth range aggregates for the current query.

        Sends the current samples query to the ``/samples/soildepth`` endpoint and
        returns a single-row GeoDataFrame with descriptive depth summary columns.

        Returns:
            geopandas.GeoDataFrame: One-row table with soil depth aggregate values.

        Raises:
            EcoPlotsError: If called outside ``samples`` mode.
        """
        if self._mode != "samples":
            raise EcoPlotsError("Soil depth range discovery is only available in 'samples' mode.")

        try:
            import geopandas
        except ImportError as exc:
            raise EcoPlotsError(
                "geopandas is required for discover_soil_depth_range(). "
                "Install it with: pip install geopandas"
            ) from exc

        payload = {"query": copy.deepcopy(self._query_filters)}
        # Keep payload compatible with discovery-style samples endpoints.

        resp = requests.post(
            f"{self._base_url}/api/v1.0/samples/soildepth",
            json=payload,
            timeout=30,
        )
        resp.raise_for_status()

        parsed = orjson.loads(resp.content)
        aggs = parsed.get("aggregations", {}) if isinstance(parsed, dict) else {}

        def _value(key: str):
            node = aggs.get(key, {})
            if isinstance(node, dict):
                return node.get("value")
            return None

        def _range_text(min_key: str, max_key: str):
            low = _value(min_key)
            high = _value(max_key)
            if low is None or high is None:
                return None
            return f"{low}-{high} m"

        row = {
            "overall_depth_min_meter": _value("min_soil_depth_min"),
            "overall_depth_max_meter": _value("max_soil_depth_max"),
            "min_depth_range": _range_text("min_soil_depth_min", "min_soil_depth_max"),
            "max_depth_range": _range_text("max_soil_depth_min", "max_soil_depth_max"),
        }

        return geopandas.GeoDataFrame([row])

    def discover_soilpit(self):
        """Discover soil pit distribution for the current samples query.

        Sends the current query to the ``/samples/soilpit`` endpoint and returns
        a two-column table with the soil pit identifier and its document count.

        Returns:
            pandas.DataFrame: Columns are ``soilpit`` and ``counts``.

        Raises:
            EcoPlotsError: If called outside ``samples`` mode.
        """
        if self._mode != "samples":
            raise EcoPlotsError("Soil pit discovery is only available in 'samples' mode.")

        try:
            import pandas as pd
        except ImportError as exc:
            raise EcoPlotsError(
                "pandas is required for discover_soilpit(). Install it with: pip install pandas"
            ) from exc

        payload = {"query": copy.deepcopy(self._query_filters)}

        resp = requests.post(
            f"{self._base_url}/api/v1.0/samples/soilpit",
            json=payload,
            timeout=30,
        )
        resp.raise_for_status()

        parsed = orjson.loads(resp.content)
        buckets = (
            (((parsed or {}).get("aggregations", {}) or {}).get("soil_subsite_id", {}) or {})
            .get("value", {})
            .get("buckets", [])
        )

        rows = []
        if isinstance(buckets, list):
            for bucket in buckets:
                if not isinstance(bucket, dict):
                    continue
                rows.append(
                    {
                        "soilpit": bucket.get("key"),
                        "counts": bucket.get("doc_count"),
                    }
                )

        return pd.DataFrame(rows, columns=["soilpit", "counts"])

    def discover_species(self):
        """Discover species names and counts using the current mode's UI facet.

        Preserves current filters, including image filtering in samples mode.
        Returns the API's species facet buckets as speciesname/count rows.
        """
        import pandas as pd

        query = copy.deepcopy(self._query_filters)
        payload = {"query": query}
        endpoint = "ui/facet"
        if self._mode == "samples":
            endpoint += "/samples"
            payload["has_image"] = query.pop("has_image", False)
        response = requests.post(
            f"{self._base_url}/api/v1.0/{endpoint}",
            params={"facet": "speciesname"},
            json=payload,
            timeout=60,
        )
        response.raise_for_status()
        buckets = (
            response.json()
            .get("aggregations", {})
            .get("speciesname", {})
            .get("value", {})
            .get("buckets", [])
        )
        return pd.DataFrame(
            [
                {"speciesname": bucket["key"], "count": bucket.get("doc_count", 0)}
                for bucket in buckets
                if isinstance(bucket, dict) and "key" in bucket
            ],
            columns=["speciesname", "count"],
        )

    def _data_payload(self, page_number=None, page_size=None, **extras):
        payload = {"query": copy.deepcopy(self._query_filters)}
        payload["query"].update(extras)
        if (page_number is None) != (page_size is None):
            raise EcoPlotsError("page_number and page_size must be provided together.")
        if page_number is not None:
            payload.update(page_number=page_number, page_size=page_size)
        return payload

    @staticmethod
    def _validate_data_format(dformat):
        if dformat not in ("geojson", "csv"):
            raise EcoPlotsError("dformat must be one of 'geojson' or 'csv'")

    async def fetch_data(
        self,
        page_number: Optional[int] = None,
        page_size: Optional[int] = None,
        dformat: str = "geojson",
        **extras,
    ):
        """Submit a single data job and asynchronously retrieve its complete output."""
        self._validate_data_format(dformat)
        payload = self._data_payload(page_number, page_size, **extras)
        job = await async_submit_jobs(self._base_url, dformat, [payload])
        result = await job._result(preserve_strings=True)
        if dformat == "csv":
            return await asyncio.to_thread(_csv_bytes, result)
        return await asyncio.to_thread(orjson.loads, result)

    def fetch_data_sync(
        self,
        page_number: Optional[int] = None,
        page_size: Optional[int] = None,
        dformat: str = "geojson",
        **extras,
    ):
        """Submit a single data job and retrieve its complete output."""
        self._validate_data_format(dformat)
        payload = self._data_payload(page_number, page_size, **extras)
        job = submit_jobs(self._base_url, dformat, [payload])
        result = job._result(preserve_strings=True)
        return _csv_bytes(result) if dformat == "csv" else orjson.loads(result)

    def iter_fetch_data_lines_sync(
        self,
        page_number: Optional[int] = None,
        page_size: Optional[int] = None,
        dformat: str = "geojson",
        **extras,
    ):
        """Yield decoded lines from a submitted job's stream."""
        self._validate_data_format(dformat)
        payload = self._data_payload(page_number, page_size, **extras)
        job = submit_jobs(self._base_url, dformat, [payload])
        deadline = job._deadline(3000, 1)
        decoder = codecs.getincrementaldecoder("utf-8")()
        pending = ""
        chunks = job._iter_content(job._jobs[0], deadline, 1, True)
        try:
            for chunk in chunks:
                pending += decoder.decode(chunk)
                while "\n" in pending:
                    line, pending = pending.split("\n", 1)
                    yield line.rstrip("\r")
        finally:
            chunks.close()
        pending += decoder.decode(b"", final=True)
        if pending:
            yield pending.rstrip("\r")

    def _sample_export_payload(self):
        """Build a sample export request requiring exactly one sample type."""
        selected = self._query_filters.get("material_sample_type", [])
        if len(selected) != 1:
            raise EcoPlotsError(
                "Exactly one material_sample_type must be selected to fetch samples."
            )
        payload = {"query": copy.deepcopy(self._query_filters)}
        has_image = payload["query"].pop("has_image", False)
        if has_image:
            payload["has_image"] = True
        return payload

    @staticmethod
    def _sample_export_frame(content):
        """Parse flat CSV samples, preserving array fields and geographic coordinates."""
        import io

        import pandas as pd

        from ._utils import _to_geopandas

        frame = pd.read_csv(io.BytesIO(content)) if content.strip() else pd.DataFrame()
        for column in (
            "parent_igsn_uris",
            "sibling_igsn_uris",
            "child_igsn_uris",
            "sample_image_urls",
        ):
            if column in frame:
                frame[column] = frame[column].map(
                    lambda value: orjson.loads(value) if isinstance(value, str) else []
                )
        return _to_geopandas(frame)

    def fetch_samples_data_sync(self):
        """Retrieve all matching samples as CSV for the selected sample type."""
        payload = self._sample_export_payload()
        response = requests.post(
            f"{self._base_url}/api/v1.0/data/samples",
            params={"dformat": "csv"},
            json=payload,
            timeout=300,
        )
        response.raise_for_status()
        return self._sample_export_frame(response.content)

    async def iter_fetch_data_lines(
        self,
        page_number: Optional[int] = None,
        page_size: Optional[int] = None,
        dformat: str = "geojson",
        **extras,
    ):
        """Asynchronously yield decoded lines from a submitted job's stream."""
        self._validate_data_format(dformat)
        payload = self._data_payload(page_number, page_size, **extras)
        job = await async_submit_jobs(self._base_url, dformat, [payload])
        deadline = job._deadline(3000, 1)
        decoder = codecs.getincrementaldecoder("utf-8")()
        pending = ""
        async with _aiohttp().ClientSession() as session:
            chunks = job._iter_content(session, job._jobs[0], deadline, 1, True)
            try:
                async for chunk in chunks:
                    pending += decoder.decode(chunk)
                    while "\n" in pending:
                        line, pending = pending.split("\n", 1)
                        yield line.rstrip("\r")
                pending += decoder.decode(b"", final=True)
                if pending:
                    yield pending.rstrip("\r")
            finally:
                await chunks.aclose()

    async def fetch_samples_data(self):
        """Retrieve all matching sample CSV asynchronously for the selected sample type."""
        payload = self._sample_export_payload()
        aiohttp = _aiohttp()
        timeout = aiohttp.ClientTimeout(total=300, sock_read=300, sock_connect=30)
        async with aiohttp.ClientSession() as session:
            async with session.post(
                f"{self._base_url}/api/v1.0/data/samples",
                params={"dformat": "csv"},
                json=payload,
                timeout=timeout,
            ) as response:
                response.raise_for_status()
                content = await response.read()
        return await asyncio.to_thread(self._sample_export_frame, content)

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
            EcoPlotsError: If the attribute cannot be resolved.

        Notes:
            - A 30-second request timeout is enforced.
            - Intended for internal use only.
        """
        facet_param = resolve_facet(discovery_attribute, DISCOVERY_ATTRIBUTES)

        if not facet_param:
            raise EcoPlotsError(f"Invalid discovery facet: {discovery_attribute}")

        url = f"{self._base_url}/api/v1.0/discovery/attributes"

        payload = {"query": copy.deepcopy(self._query_filters)}

        params = [("type", facet_param)]

        resp = requests.post(url, params=params, json=payload, timeout=30)
        resp.raise_for_status()
        return orjson.loads(resp.content)

    def fetch_attributes_data(self, attribute_type: str) -> bytes:
        """Fetch site or site-visit attribute data as CSV bytes.

        Args:
            attribute_type: Attribute data entity to fetch. Supported values are
                ``"site"`` and ``"site_visit"``/``"site-visit"``.

        Returns:
            Raw CSV bytes returned by the API.

        Raises:
            EcoPlotsError: If an unsupported attribute type is requested.
        """
        attribute_paths = {
            "site": "site",
            "site_visit": "site-visit",
            "site-visit": "site-visit",
        }
        endpoint_attribute = attribute_paths.get(attribute_type)
        if not endpoint_attribute:
            raise EcoPlotsError(
                "Invalid attribute data type. Allowed values are: site, site_visit."
            )

        payload = {"query": copy.deepcopy(self._query_filters)}
        resp = requests.post(
            f"{self._base_url}/api/v1.0/data/attributes/{endpoint_attribute}",
            params=[("dformat", "csv")],
            json=payload,
            timeout=300,
        )
        resp.raise_for_status()
        return resp.content

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

        if self._mode == "samples":
            payload["context"] = "samples"
            if "has_image" in payload["query"]:
                payload["has_image"] = payload["query"].pop("has_image")

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
        tmp.close()

        try:
            with open(tmp_path, "wb") as f:
                f.write(MAGIC)
                f.write(struct.pack(">B", PROJECT_FILE_VERSION))
                f.write(sha)
                f.write(struct.pack(">Q", len(body)))
                f.write(body)
            # Atomic replace
            target.replace(target) if target.exists() else None
            tmp_path.replace(target)
        except Exception:
            tmp_path.unlink(missing_ok=True)
            raise
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
            A new instance of the calling class with ``filters`` and
            ``query_filters`` restored from the file.

        Raises:
            FileNotFoundError: If the file does not exist.
            EcoPlotsError: If the file does not have a `.ecoproj` suffix, the magic
                header or version is invalid, the file is truncated, or the checksum
                does not match the payload.
        """
        p = Path(path).resolve()
        if not p.exists():
            raise FileNotFoundError(str(p))
        if p.suffix != ".ecoproj":
            raise EcoPlotsError(f"Expected a '.ecoproj' file, got: {p.name}")

        with open(p, "rb") as f:
            if f.read(4) != MAGIC:
                raise EcoPlotsError("Invalid project file (bad magic).")
            ver = struct.unpack(">B", f.read(1))[0]
            if ver != PROJECT_FILE_VERSION:
                raise EcoPlotsError(
                    f"Incompatible project version: {ver} (expected {PROJECT_FILE_VERSION})."
                )
            sha = f.read(32)
            n = struct.unpack(">Q", f.read(8))[0]
            body = f.read(n)
            if len(body) != n:
                raise EcoPlotsError("Truncated project file.")
            if hashlib.sha256(body).digest() != sha:
                raise EcoPlotsError("Project integrity check failed (checksum mismatch).")

        data = orjson.loads(body)

        return cls(filterset=data.get("filters", {}), query_filters=data.get("query_filters", {}))

    def _validate_filters(self) -> bool:
        """Validate filters in parallel for all facets. Not for direct user use.

        Returns:
            `True` if filters validate and the summary indicates one or more
            matching records; `False` if validation succeeded but the selection
            yields zero records (a warning is issued).

        Raises:
            EcoPlotsError: If any facet contains values that cannot be matched.

        Notes:
            - Intended for internal use only.
        """

        query_filters = copy.deepcopy(self._query_filters)
        all_unmatched: dict = {}
        all_matched = copy.deepcopy(self._filters)

        if "spatial" in all_matched:
            query_filters["spatial"] = all_matched["spatial"]

        if "soil_subsite_id" in all_matched:
            query_filters["soil_subsite_id"] = all_matched["soil_subsite_id"]

        if "soil_depth_range" in all_matched:
            query_filters["soil_depth_range"] = all_matched["soil_depth_range"]

        if "date_from" in all_matched:
            query_filters["date_from"] = all_matched["date_from"]

        if "date_to" in all_matched:
            query_filters["date_to"] = all_matched["date_to"]

        if "speciesname" in all_matched:
            user_species_values = all_matched.get("speciesname", [])
            if not isinstance(user_species_values, (list, tuple)):
                user_species_values = [user_species_values]

            species_df = self.discover_species()
            if "speciesname" in species_df.columns:
                available_species = [
                    str(v).strip()
                    for v in species_df["speciesname"].dropna().tolist()
                    if str(v).strip()
                ]
            else:
                available_species = []

            if not available_species:
                raise EcoPlotsError(
                    "Unable to validate 'speciesname': discovery returned no species values "
                    "for the current filters."
                )

            matched_species = []
            unmatched_species = []

            for raw_value in user_species_values:
                candidate = str(raw_value).strip()
                if not candidate:
                    unmatched_species.append(raw_value)
                    continue

                # First pass: case-insensitive exact match.
                exact_match = next(
                    (name for name in available_species if name.casefold() == candidate.casefold()),
                    None,
                )
                if exact_match is not None:
                    matched_species.append(exact_match)
                    continue

                # Fuzzy fallback to tolerate minor spelling/casing/missing tokens.
                fuzzy = process.extractOne(candidate, available_species, scorer=fuzz.WRatio)
                if fuzzy is None:
                    unmatched_species.append(raw_value)
                    continue

                best_name, score, _ = fuzzy
                if score >= 80:
                    if best_name.casefold() != candidate.casefold():
                        self._display_warning(
                            f"Value '{candidate}' for facet 'speciesname' corrected to "
                            f"'{best_name}'."
                        )
                    matched_species.append(best_name)
                else:
                    unmatched_species.append(raw_value)

            if unmatched_species:
                all_unmatched.setdefault("speciesname", [])
                all_unmatched["speciesname"].extend(unmatched_species)
            else:
                # Deduplicate while preserving order.
                deduped_species = []
                seen_species = set()
                for item in matched_species:
                    if item not in seen_species:
                        seen_species.add(item)
                        deduped_species.append(item)
                all_matched["speciesname"] = deduped_species
                query_filters["speciesname"] = deduped_species

        to_validate = {
            k: v
            for k, v in self._filters.items()
            if k
            not in {
                "spatial",
                "has_image",
                "soil_subsite_id",
                "soil_depth_range",
                "speciesname",
                "date_from",
                "date_to",
            }
        }

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
                if facet in EQUIPMENT_FACETS:
                    group, child, _ = EQUIPMENT_FACETS[facet]
                    children = query_filters.setdefault(group, {})
                    existing = set(children.get(child, []))
                    existing.update(urls)
                    children[child] = sorted(existing)
                else:
                    api_facet = FACET_API_NAMES.get(facet, facet)
                    existing = set(query_filters.get(api_facet, []))
                    existing.update(urls)
                    query_filters[api_facet] = list(existing)

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
            raise EcoPlotsError(msg)

        summary_query_filters = copy.deepcopy(query_filters)
        summary_query_filters.pop("has_image", None)

        data = self.summarise_data(query_filters=summary_query_filters)
        if data.get("total_doc", 0) == 0:
            self._display_warning(
                "The applied filters result in zero matching records. "
                "Please adjust your filters. Skipping current selection..."
            )
            return False

        self._query_filters = query_filters
        self._filters = all_matched

        return True

    def _fetch_clusters(self, geojson: Optional[dict] = None) -> dict:
        """Fetch clustered data points for map visualization.

        Args:
            geojson: Optional GeoJSON polygon to define the area of interest.

        Returns:
            Parsed JSON payload containing clustered data points.

        Notes:
            - Intended for internal use only.
        """
        payload = {
            "query": copy.deepcopy(self._query_filters),
            "clustering_precision": 3,
            "geojson": geojson
            or {
                "type": "Polygon",
                "coordinates": [
                    [
                        [107.68366383276675, -9.83285528397626],
                        [159.86061589572708, -9.83285528397626],
                        [159.86061589572708, -44.49207177551449],
                        [107.68366383276675, -44.49207177551449],
                        [107.68366383276675, -9.83285528397626],
                    ]
                ],
            },
        }

        if self._mode == "samples":
            payload["context"] = "samples"
            has_image = payload["query"].pop("has_image", None)
            if has_image is True:
                payload["has_image"] = True

        resp = requests.post(
            f"{self._base_url}/api/v1.0/ui/map/clusters",
            json=payload,
            timeout=30,
        )

        resp.raise_for_status()
        return orjson.loads(resp.content)

    def _ensure_required_material_sample_types(
        self, required_labels: list[str], context: str
    ) -> None:
        """Ensure at least one required material sample type is selected.

        Args:
            required_labels: Human-readable material sample type labels where at
                least one must be present in current ``material_sample_type``.
            context: Short workflow name used in error messages.

        Raises:
            EcoPlotsError: If none of the required sample types are selected.
        """
        label_to_uri = {label: uri for uri, label in MATERIAL_SAMPLE_TYPE_MAP.items()}
        selected = self._query_filters.get("material_sample_type", [])
        if isinstance(selected, str):
            selected_uris = {selected}
        else:
            selected_uris = set(selected)

        required_uris = {label_to_uri[label] for label in required_labels if label in label_to_uri}

        has_any_required = bool(selected_uris.intersection(required_uris))

        if not has_any_required:
            selected_labels = [
                MATERIAL_SAMPLE_TYPE_MAP.get(uri, uri) for uri in sorted(selected_uris)
            ]
            selected_display = ", ".join(selected_labels) if selected_labels else "none"
            raise EcoPlotsError(
                f"{context} requires material_sample_type to include at least one of: "
                f"{', '.join(required_labels)}. Currently selected: {selected_display}."
            )
