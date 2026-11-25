"""Revalidate persistent vocabularies before their first use in each process."""

import logging
import sqlite3
import threading
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone

import diskcache
import requests

from ._config import API_BASE_URL, CACHE_DIR, CACHE_EXPIRE_SECONDS, VOCAB_FACETS

_ready = threading.Event()
_start_lock = threading.Lock()
_refresh_lock = threading.Lock()
_started = False
_METADATA = "__vocabulary_metadata__"


def _version():
    """Read the current origin vocabulary version without a cached HTTP response."""
    response = requests.get(
        f"{API_BASE_URL}/api/v1.0/ui/cache/version",
        headers={"Cache-Control": "no-cache"},
        timeout=5,
    )
    response.raise_for_status()
    version = response.headers.get("X-Index-Version") or response.text.strip()
    if not version:
        raise ValueError("Vocabulary API returned an empty version.")
    return version


def _fetch_facet(facet, version, previous, labels):
    """Fetch or revalidate one facet, checking its response version before use."""
    headers = {"Cache-Control": "no-cache"}
    if labels is not None and previous.get("etag"):
        headers["If-None-Match"] = previous["etag"]
    response = requests.get(
        f"{API_BASE_URL}/api/v1.0/data/label/{facet}",
        params={"v": version},
        headers=headers,
        timeout=15,
    )
    response.raise_for_status()
    if response.headers.get("X-Index-Version") != version:
        raise ValueError("Vocabulary response version changed during refresh.")
    if response.status_code == 304:
        if labels is None or previous.get("version") != version:
            raise ValueError("Vocabulary API returned 304 without a matching cached payload.")
    else:
        labels = response.json()[facet]
        if not isinstance(labels, dict):
            raise ValueError("Vocabulary payload must be a mapping.")
    return labels, {"version": version, "etag": response.headers.get("ETag")}


def refresh_vocabularies():
    """Stage needed vocabularies and publish one verified cache transaction."""
    with _refresh_lock, diskcache.Cache(CACHE_DIR) as cache:
        version = _version()
        metadata = cache.get(_METADATA, {})
        if not isinstance(metadata, dict):
            metadata = {}
        previous = metadata.get("facets", {})
        labels = {facet: cache.get(facet) for facet in VOCAB_FACETS}
        labels = {
            facet: value if isinstance(value, dict) else None for facet, value in labels.items()
        }
        needed = [
            facet
            for facet in VOCAB_FACETS
            if labels[facet] is None or previous.get(facet, {}).get("version") != version
        ]
        updated = dict(previous)
        if needed:
            with ThreadPoolExecutor(max_workers=6) as pool:
                pending = {
                    facet: pool.submit(
                        _fetch_facet, facet, version, previous.get(facet, {}), labels[facet]
                    )
                    for facet in needed
                }
                for facet, future in pending.items():
                    labels[facet], updated[facet] = future.result()
            if _version() != version:
                raise ValueError("Vocabulary index changed while downloading labels.")
        with cache.transact():
            for facet in VOCAB_FACETS:
                cache.set(facet, labels[facet], expire=CACHE_EXPIRE_SECONDS)
            cache.set(
                _METADATA,
                {
                    "version": version,
                    "api_url": API_BASE_URL,
                    "facets": updated,
                    "checked_at": datetime.now(timezone.utc).isoformat(),
                },
            )
        return labels


def _load():
    """Keep cached labels available when startup revalidation cannot complete."""
    try:
        refresh_vocabularies()
    except (
        requests.RequestException,
        ValueError,
        KeyError,
        TypeError,
        OSError,
        sqlite3.Error,
    ) as exc:
        logging.getLogger(__name__).warning(
            "Vocabulary freshness could not be verified; using available cached labels: %s", exc
        )
    finally:
        _ready.set()


def start_vocab_loader():
    """Launch exactly one background vocabulary check per process."""
    global _started
    with _start_lock:
        if not _started:
            _started = True
            threading.Thread(target=_load, name="ecoplots-vocab", daemon=True).start()


def wait_for_vocabularies():
    """Wait for startup validation before consuming persistent vocabulary labels."""
    start_vocab_loader()
    _ready.wait()
