from typing import Any, Optional


def _flatten_mapping(prefix: str, obj: dict[str, Any], out: dict[str, Any]) -> None:
    """Flatten a nested mapping into dotted keys.

    Special-cases common `{value, unit, attribute}` shapes:
      - `prefix.key` → value
      - `prefix.key.unit` → unit (if present)
      - `prefix.key.attribute` → attribute (if present)

    Args:
        prefix: Dotted path accumulated so far.
        obj: Mapping to flatten.
        out: Destination mapping to update in place.

    Notes:
        Intended for internal use only; mutates ``out``.
    """
    if not isinstance(obj, dict):
        return
    for k, v in obj.items():
        key = f"{prefix}.{k}" if prefix else k
        if isinstance(v, dict):
            if "value" in v:
                out[key] = v.get("value")
                if "unit" in v:
                    out[f"{key}.unit"] = v.get("unit")
                if "attribute" in v:
                    out[f"{key}.attribute"] = v.get("attribute")
            else:
                _flatten_mapping(key, v, out)
        else:
            out[key] = v


def _base_from_feature(feature: dict[str, Any]) -> tuple[dict[str, Any], Optional[dict[str, Any]]]:
    """Extract feature-level context and geometry.

    Pulls dataset and site context into a flat dict and returns it with the
    raw GeoJSON geometry.

    Args:
        feature: GeoJSON Feature mapping.

    Returns:
        tuple: ``(base_context, geometry_geojson)`` where:
            - ``base_context`` is a flattened mapping of dataset/site fields.
            - ``geometry_geojson`` is the raw geometry mapping or ``None``.

    Notes:
        Intended for internal use only.
    """
    props = feature.get("properties", {}) or {}
    geom = feature.get("geometry")

    base: dict[str, Any] = {}
    ds = props.get("dataset", {}) or {}
    base["dataset.title"] = ds.get("dataset.title")
    base["dataset.link"] = ds.get("dataset.link")
    _flatten_mapping("dataset.attributes", ds.get("dataset.attributes", {}) or {}, base)

    site = props.get("site", {}) or {}
    base["site.name"] = site.get("site.name")
    base["site.link"] = site.get("site.link")
    _flatten_mapping("site.attributes", site.get("site.attributes", {}) or {}, base)

    return base, geom


def _rows_from_sitevisit_task(
    task: tuple[dict[str, Any], dict[str, Any], Optional[dict[str, Any]]],
) -> list[dict[str, Any]]:
    """Expand a single site-visit task into observation rows.

    Builds one or more row dictionaries by combining base context, a site visit,
    features of interest, and observations. Geometry (if present) is attached
    to each row.

    Args:
        task: A tuple ``(base_context, site_visit, geometry_geojson)`` produced
            by ``_iter_sitevisit_tasks_from``.

    Returns:
        Row dictionaries ready for tabular assembly (each row describes
        one observation or, if none exist, a context-only record).

    Notes:
        Intended for internal use only.
    """
    base, sv, geom = task
    rows: list[dict[str, Any]] = []

    sv_base = dict(base)
    if sv:
        sv_base["siteVisit.id"] = sv.get("siteVisit.id")
        sv_base["siteVisit.name"] = sv.get("siteVisit.name")
        sv_base["siteVisit.date"] = sv.get("siteVisit.date")
        sv_base["siteVisit.link"] = sv.get("siteVisit.link")
        _flatten_mapping("siteVisit.attributes", sv.get("siteVisit.attributes", {}) or {}, sv_base)

    fois = (sv.get("featureOfInterest") if sv else None) or []
    if not fois:
        row = dict(sv_base)
        if geom:
            row["geometry"] = geom
        rows.append(row)
        return rows

    for foi in fois:
        foi_base = dict(sv_base)
        foi_base["foi.id"] = foi.get("foi.id")
        foi_base["foi.type"] = foi.get("foi.type")
        foi_base["foi.link"] = foi.get("foi.link")
        if "foi.scientificName" in foi:
            foi_base["foi.scientificName"] = foi.get("foi.scientificName")
        _flatten_mapping("foi.attributes", foi.get("foi.attributes", {}) or {}, foi_base)

        obs_dict = foi.get("foi.observations") or {}
        if not obs_dict:
            row = dict(foi_base)
            if geom:
                row["geometry"] = geom
            rows.append(row)
            continue

        for group, obs_list in obs_dict.items():
            for obs in obs_list or []:
                row = dict(foi_base)
                row["obs.group"] = group
                row["obs.property"] = obs.get("observableProperty")
                row["obs.value"] = obs.get("value")
                row["obs.unit"] = obs.get("unit")
                row["obs.resultTime"] = obs.get("resultTime")
                row["obs.procedure"] = obs.get("usedProcedure")
                _flatten_mapping("obs.system", obs.get("system", {}) or {}, row)
                if geom:
                    row["geometry"] = geom
                rows.append(row)

    return rows
