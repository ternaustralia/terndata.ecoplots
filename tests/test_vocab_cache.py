"""Versioned vocabulary refresh, atomic publication and startup coordination."""

import threading
from types import SimpleNamespace

import pytest
import requests
from diskcache import Cache

from terndata.ecoplots import _vocab_cache as vocab


@pytest.fixture
def store(tmp_path, monkeypatch):
    vocab.wait_for_vocabularies()
    monkeypatch.setattr(vocab, "CACHE_DIR", str(tmp_path / "labels"))
    monkeypatch.setattr(vocab, "VOCAB_FACETS", ["dataset", "platform_type"])
    monkeypatch.setattr(vocab, "_version", lambda: "v2")
    with Cache(vocab.CACHE_DIR) as cache:
        yield cache


def seed(store, version="v1"):
    for facet in vocab.VOCAB_FACETS:
        store[facet] = {"old-uri": "Old"}
    store[vocab._METADATA] = {
        "version": version,
        "facets": {
            facet: {"version": version, "etag": '"' + version + '"'} for facet in vocab.VOCAB_FACETS
        },
    }


def response(version="v2", status=200, facet="dataset"):
    return SimpleNamespace(
        status_code=status,
        headers={"X-Index-Version": version, "ETag": '"' + version + '"'},
        raise_for_status=lambda: None,
        json=lambda: {facet: {"new-uri": "New"}},
    )


def test_new_version_replaces_all_facets_and_next_check_reuses_them(store, monkeypatch):
    seed(store)
    calls = []

    def get(url, **kwargs):
        calls.append(kwargs)
        return response(facet=url.rsplit("/", 1)[-1])

    monkeypatch.setattr(vocab.requests, "get", get)
    vocab.refresh_vocabularies()
    assert store["dataset"] == {"new-uri": "New"}
    assert store[vocab._METADATA]["version"] == "v2"
    assert store[vocab._METADATA]["checked_at"]
    assert calls[0]["headers"]["If-None-Match"] == '"v1"'
    assert calls[0]["params"] == {"v": "v2"}
    vocab.refresh_vocabularies()
    assert len(calls) == 2


def test_missing_facet_is_fetched_even_if_version_matches(store, monkeypatch):
    seed(store, "v2")
    del store["platform_type"]
    calls = []

    def get(url, **kwargs):
        calls.append(kwargs)
        return response(facet="platform_type")

    monkeypatch.setattr(vocab.requests, "get", get)
    vocab.refresh_vocabularies()
    assert len(calls) == 1
    assert "If-None-Match" not in calls[0]["headers"]
    assert store["dataset"] == {"old-uri": "Old"}


@pytest.mark.parametrize("failure", ["network", "version", "rotation", "malformed"])
def test_failed_refresh_keeps_old_generation(store, monkeypatch, failure):
    seed(store)
    before = store[vocab._METADATA]

    def get(url, **kwargs):
        if failure == "network":
            raise requests.ConnectionError("offline")
        result = response(
            version="wrong" if failure == "version" else "v2", facet=url.rsplit("/", 1)[-1]
        )
        if failure == "malformed":
            result.json = lambda: {url.rsplit("/", 1)[-1]: []}
        return result

    if failure == "rotation":
        versions = iter(["v2", "v3"])
        monkeypatch.setattr(vocab, "_version", lambda: next(versions))
    monkeypatch.setattr(vocab.requests, "get", get)
    with pytest.raises((ValueError, requests.ConnectionError)):
        vocab.refresh_vocabularies()
    assert store["dataset"] == {"old-uri": "Old"}
    assert store["platform_type"] == {"old-uri": "Old"}
    assert store[vocab._METADATA] == before


def test_304_reuses_only_matching_payload(monkeypatch):
    monkeypatch.setattr(vocab.requests, "get", lambda *a, **k: response(status=304))
    labels = {"uri": "Label"}
    result, metadata = vocab._fetch_facet("dataset", "v2", {"version": "v2"}, labels)
    assert result == labels
    assert metadata["etag"] == '"v2"'
    with pytest.raises(ValueError, match="matching cached payload"):
        vocab._fetch_facet("dataset", "v2", {"version": "v1"}, labels)


def test_offline_startup_logs_once_and_releases_waiters(store, monkeypatch, caplog):
    seed(store)
    ready = threading.Event()
    monkeypatch.setattr(vocab, "_ready", ready)

    def fail():
        raise requests.ConnectionError("offline")

    monkeypatch.setattr(vocab, "refresh_vocabularies", fail)
    vocab._load()
    assert ready.is_set()
    assert "freshness could not be verified" in caplog.text
    assert store["dataset"] == {"old-uri": "Old"}


def test_consumers_wait_for_loader_completion(monkeypatch):
    ready = threading.Event()
    consumed = threading.Event()
    entered = threading.Event()
    monkeypatch.setattr(vocab, "_ready", ready)
    monkeypatch.setattr(vocab, "start_vocab_loader", lambda: entered.set())

    def consume():
        vocab.wait_for_vocabularies()
        consumed.set()

    thread = threading.Thread(target=consume)
    thread.start()
    assert entered.wait(1)
    try:
        assert not consumed.is_set()
    finally:
        ready.set()
        thread.join(1)
    assert consumed.is_set()


def test_unversioned_cache_is_downloaded_again(store, monkeypatch):
    for facet in vocab.VOCAB_FACETS:
        store[facet] = {"legacy-uri": "Legacy"}
    monkeypatch.setattr(
        vocab.requests, "get", lambda url, **k: response(facet=url.rsplit("/", 1)[-1])
    )
    vocab.refresh_vocabularies()
    assert store["dataset"] == {"new-uri": "New"}
    assert store[vocab._METADATA]["version"] == "v2"


def test_version_probe_requests_origin_revalidation(monkeypatch):
    calls = []

    def get(url, **kwargs):
        calls.append((url, kwargs))
        return SimpleNamespace(raise_for_status=lambda: None, headers={}, text="v2\n")

    monkeypatch.setattr(vocab.requests, "get", get)
    assert vocab._version() == "v2"
    assert calls[0][0].endswith("/ui/cache/version")
    assert calls[0][1]["headers"] == {"Cache-Control": "no-cache"}
    assert calls[0][1]["timeout"] == 5
