import copy
from importlib import resources
from pathlib import Path

import pytest
import rispy
import yaml

from terndata.ecoplots import AsyncEcoPlots, EcoPlots, EcoPlotsError, _citations
from terndata.ecoplots import ecoplots as clients


@pytest.fixture
def references():
    return {
        "citations": [
            {
                "citation": (
                    "Queensland Herbarium (2022). Queensland CORVEG Database. "
                    "https://example.test/corveg"
                ),
                "title": "Queensland CORVEG Database",
                "authors": [{"name": "Queensland Herbarium"}],
                "year": "2022",
                "url": "https://example.test/corveg",
            },
            {
                "citation": "García, José (2020). Site observations. https://doi.org/10.1234/example",
                "title": "Site observations",
                "authors": [{"family-names": "García", "given-names": "José"}],
                "year": "2020",
                "doi": "10.1234/example",
                "url": "https://doi.org/10.1234/example",
            },
        ]
    }


@pytest.fixture(params=[EcoPlots, AsyncEcoPlots], ids=["sync", "async"])
def citation_client(request, references, monkeypatch):
    ec = request.param(query_filters={"site_id": ["site-uri"], "feature_type": ["feature-uri"]})
    calls = []

    def fetch(base_url, query, mode):
        calls.append((base_url, copy.deepcopy(query), mode))
        return references

    monkeypatch.setattr(clients, "fetch_citations", fetch)
    monkeypatch.setattr(_citations, "VERSION", "1.2.0")
    return ec, calls


def test_cite_returns_data_references_then_versioned_software(citation_client):
    ec, calls = citation_client

    text = ec.cite()

    assert isinstance(text, str)
    assert "Queensland Herbarium (2022)" in text
    assert "García, José (2020)" in text
    assert "Chandra, Avinash" in text
    assert "Version 1.2.0" in text
    assert text.index("Queensland") < text.index("García") < text.index("Chandra")
    assert calls == [(ec._base_url, ec._query_filters, "observations")]


def test_cite_ris_is_readable_by_independent_parser(citation_client):
    ec, _ = citation_client

    entries = rispy.loads(ec.cite(dformat="ris"))

    assert len(entries) == 3
    assert entries[0]["type_of_reference"] == "DATA"
    assert entries[0]["authors"] == ["Queensland Herbarium,"]
    assert entries[0]["year"] == "2022"
    assert entries[1]["authors"] == ["García, José"]
    assert entries[1]["doi"] == "10.1234/example"
    assert entries[-1]["type_of_reference"] == "COMP"
    assert entries[-1]["authors"] == ["Chandra, Avinash"]
    assert entries[-1]["edition"] == "1.2.0"
    assert "License: Apache-2.0" in entries[-1]["notes"]
    assert "year" not in entries[-1]


@pytest.mark.parametrize("suffix", [".ris", ".txt", ".RIS"])
def test_cite_saves_format_inferred_from_path(citation_client, tmp_path, suffix):
    ec, _ = citation_client
    path = tmp_path / "nested" / f"references{suffix}"

    assert ec.cite(path=path) == str(path.resolve())
    content = path.read_text(encoding="utf-8")
    if suffix.lower() == ".ris":
        assert len(rispy.loads(content)) == 3
    else:
        assert "Chandra, Avinash" in content
        assert "García, José" in content


def test_cite_removes_duplicates_without_reordering(citation_client, references):
    ec, _ = citation_client
    duplicate = copy.deepcopy(references["citations"][0])
    duplicate["citation"] = "  " + duplicate["citation"].replace(" ", "  ")
    references["citations"].append(duplicate)
    same_doi = copy.deepcopy(references["citations"][1])
    same_doi["citation"] = "An alternative rendering of the same publication."
    references["citations"].append(same_doi)

    entries = rispy.loads(ec.cite(dformat="ris"))

    assert len(entries) == 3
    assert entries[0]["title"] == "Queensland CORVEG Database"
    assert entries[1]["title"] == "Site observations"


def test_cite_preserves_unstructured_references(citation_client, references):
    ec, _ = citation_client
    text = "A published citation whose structured author and date metadata are unavailable."
    references["citations"] = [{"citation": text}]

    entry = rispy.loads(ec.cite(dformat="ris"))[0]

    assert entry["notes"] == [text]
    assert entry["title"] == text
    assert all(field not in entry for field in ("authors", "year", "doi", "publisher"))


def test_cite_empty_match_still_includes_software(citation_client, references):
    ec, _ = citation_client
    references["citations"] = []

    entries = rispy.loads(ec.cite(dformat="ris"))

    assert len(entries) == 1
    assert entries[0]["type_of_reference"] == "COMP"


@pytest.mark.parametrize(
    "kwargs",
    [
        {"dformat": "bibtex"},
        {"dformat": ""},
        {"path": "references.json"},
        {"dformat": "text", "path": "references.ris"},
        {"dformat": "ris", "path": "references.txt"},
    ],
)
def test_cite_validates_output_before_requesting(citation_client, kwargs):
    ec, calls = citation_client

    with pytest.raises(EcoPlotsError):
        ec.cite(**kwargs)

    assert calls == []


def test_cite_keeps_samples_context_and_filters(citation_client):
    ec, calls = citation_client
    ec._mode = "samples"
    ec._query_filters.update(material_sample_type=["sample-type"], has_image=True)

    ec.cite()

    assert calls[0][2] == "samples"
    assert calls[0][1]["material_sample_type"] == ["sample-type"]
    assert calls[0][1]["has_image"] is True


def test_cite_invalid_metadata_does_not_replace_file(citation_client, references, tmp_path):
    ec, _ = citation_client
    references["citations"] = [{"citation": ""}]
    path = tmp_path / "references.ris"
    path.write_text("existing references", encoding="utf-8")

    with pytest.raises(EcoPlotsError, match="empty citation"):
        ec.cite(path=path)

    assert path.read_text() == "existing references"


def test_ris_values_cannot_inject_records(citation_client, references):
    ec, _ = citation_client
    references["citations"][0]["title"] = "Title\nER  - \nTY  - JOUR"

    entries = rispy.loads(ec.cite(dformat="ris"))

    assert len(entries) == 3
    assert entries[0]["title"] == "Title ER - TY - JOUR"


def test_ris_preserves_corporate_authors_with_commas(citation_client, references):
    ec, _ = citation_client
    references["citations"][0]["authors"] = [{"name": "Department of Environment, Queensland"}]

    entry = rispy.loads(ec.cite(dformat="ris"))[0]

    assert entry["authors"] == ["Department of Environment,, Queensland"]


def test_packaged_software_metadata_matches_repository():
    packaged = resources.files("terndata.ecoplots").joinpath("CITATION.cff").read_text()
    canonical = (Path(__file__).resolve().parents[1] / "CITATION.cff").read_text()

    assert yaml.safe_load(packaged) == yaml.safe_load(canonical)


@pytest.mark.parametrize("client_class", [EcoPlots, AsyncEcoPlots])
@pytest.mark.parametrize("mode", ["observations", "samples"])
def test_cite_uses_read_only_metadata_endpoint(client_class, mode, data_api, references):
    data_api.citations = references
    ec = client_class(mode=mode, query_filters={"site_id": ["site-uri"]})

    text = ec.cite()

    assert isinstance(text, str)
    assert "Queensland Herbarium (2022)" in text
    assert len(data_api.requests) == 1
    method, url, options = data_api.requests[0]
    assert method == "POST"
    assert url == f"{ec._base_url}/api/v1.0/data/citations"
    assert options["json"] == {"query": ec._query_filters, "context": mode}
    assert all(response.closed for response in data_api.responses)


def test_unavailable_endpoint_is_reported_without_software_only_fallback(monkeypatch):
    class Response:
        status_code = 404

        def __enter__(self):
            return self

        def __exit__(self, *args):
            pass

    monkeypatch.setattr(_citations.requests, "post", lambda *args, **kwargs: Response())

    with pytest.raises(EcoPlotsError, match="/api/v1.0/data/citations"):
        EcoPlots(query_filters={"site_id": ["site-uri"]}).cite()


def test_cite_snapshots_filters(citation_client, references, monkeypatch):
    ec, _ = citation_client
    queries = []

    def fetch(base_url, query, mode):
        ec._query_filters["site_id"].append("another-site")
        queries.append(query)
        return references

    monkeypatch.setattr(clients, "fetch_citations", fetch)

    ec.cite()

    assert queries == [{"site_id": ["site-uri"], "feature_type": ["feature-uri"]}]
