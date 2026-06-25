from importlib import resources
from pathlib import Path

import pytest

from terndata.ecoplots import EcoPlots, EcoPlotsError
from terndata.ecoplots._config import (
    API_BASE_URL_ENV_VAR,
    DEV_API_BASE_URL,
    PRODUCTION_API_BASE_URL,
    _resolve_api_base_url,
)


def test_save_and_load_round_trip_project_file(tmp_path):
    ec = EcoPlots(
        filterset={"site_id": ["TCFTNS0002"]},
        query_filters={"site_id": ["https://example.test/site/TCFTNS0002"]},
    )

    path = ec.save(tmp_path / "query-state")
    loaded = EcoPlots.load(path)

    assert path.endswith(".ecoproj")
    assert loaded == ec
    assert loaded is not ec


def test_load_rejects_wrong_suffix(tmp_path):
    path = tmp_path / "query-state.txt"
    path.write_bytes(b"not an ecoproj")

    with pytest.raises(EcoPlotsError, match=".ecoproj"):
        EcoPlots.load(path)


def test_load_rejects_bad_magic(tmp_path):
    path = tmp_path / "query-state.ecoproj"
    path.write_bytes(b"NOPE")

    with pytest.raises(EcoPlotsError, match="bad magic"):
        EcoPlots.load(path)


def test_package_declares_typed_marker():
    marker = resources.files("terndata.ecoplots").joinpath("py.typed")

    assert marker.is_file()


def test_repository_declares_citation_metadata():
    citation = Path(__file__).resolve().parents[1] / "CITATION.cff"

    assert citation.is_file()
    text = citation.read_text(encoding="utf-8")
    assert "cff-version: 1.2.0" in text
    assert "terndata.ecoplots" in text


def test_release_versions_use_production_api(monkeypatch):
    monkeypatch.delenv(API_BASE_URL_ENV_VAR, raising=False)

    assert _resolve_api_base_url("1.2.3") == PRODUCTION_API_BASE_URL


def test_dev_versions_use_test_api(monkeypatch):
    monkeypatch.delenv(API_BASE_URL_ENV_VAR, raising=False)

    assert _resolve_api_base_url("1.2.4.dev123") == DEV_API_BASE_URL


def test_api_base_url_env_override(monkeypatch):
    monkeypatch.setenv(API_BASE_URL_ENV_VAR, "http://localhost:8000/")

    assert _resolve_api_base_url("1.2.3") == "http://localhost:8000"
