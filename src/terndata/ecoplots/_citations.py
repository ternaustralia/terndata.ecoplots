"""Retrieve and format query-specific data and software citations."""

import copy
import tempfile
from dataclasses import dataclass
from importlib import resources
from pathlib import Path
from typing import Optional, Union

import requests
import yaml

from ._config import VERSION
from ._exceptions import EcoPlotsError


class _CitationText(str):
    """Display citation paragraphs directly in notebooks."""

    def _repr_pretty_(self, printer, cycle):
        printer.text(str(self))


@dataclass(frozen=True)
class _Author:
    name: str
    organisation: bool = False

    def ris_name(self):
        if not self.organisation:
            return self.name
        name = self.name.rstrip(",")
        return name.replace(",", ",,", 1) if "," in name else name + ","


@dataclass(frozen=True)
class _Reference:
    text: str
    title: str
    authors: tuple[_Author, ...] = ()
    kind: str = "DATA"
    year: str = ""
    doi: str = ""
    url: str = ""
    version: str = ""
    publisher: str = ""
    license: str = ""


def _line(value) -> str:
    return " ".join(str(value).split()) if value is not None else ""


def _author(value) -> _Author:
    if not isinstance(value, dict):
        raise EcoPlotsError("Citation authors must contain structured names.")
    if value.get("family-names"):
        parts = [value.get("name-particle"), value["family-names"]]
        family = " ".join(part for part in parts if part)
        name = ", ".join(part for part in [family, value.get("given-names")] if part)
        if value.get("name-suffix"):
            name += ", " + value["name-suffix"]
        return _Author(_line(name))
    if value.get("name"):
        return _Author(_line(value["name"]), organisation=True)
    if value.get("literal"):
        return _Author(_line(value["literal"]))
    raise EcoPlotsError("Citation author is missing a name.")


def _software_reference() -> _Reference:
    metadata = yaml.safe_load(
        resources.files("terndata.ecoplots").joinpath("CITATION.cff").read_text(encoding="utf-8")
    )
    authors = tuple(_author(author) for author in metadata["authors"])
    title = metadata["title"]
    url = metadata.get("repository-code") or metadata.get("url", "")
    year = str(metadata.get("date-released", ""))[:4]
    names = "; ".join(author.name for author in authors)
    text = f"{names}. " + (f"({year}). " if year else "")
    text += f"{title} (Version {VERSION}) [Computer software]. {url}"
    return _Reference(
        text=text,
        title=title,
        authors=authors,
        kind="COMP",
        year=year,
        doi=metadata.get("doi", ""),
        url=url,
        version=VERSION,
        license=metadata.get("license", ""),
    )


def _data_references(response) -> list[_Reference]:
    if not isinstance(response, dict) or not isinstance(response.get("citations"), list):
        raise EcoPlotsError("Invalid citation API response: expected a citations list.")
    references = []
    seen = set()
    for item in response["citations"]:
        if not isinstance(item, dict) or not isinstance(item.get("citation"), str):
            raise EcoPlotsError("Invalid citation API record: missing citation text.")
        text = _line(item["citation"])
        if not text:
            raise EcoPlotsError("Invalid citation API record: empty citation text.")
        doi = _line(item.get("doi"))
        key = ("doi", doi.casefold()) if doi else ("text", text.casefold())
        if key in seen:
            continue
        seen.add(key)
        references.append(
            _Reference(
                text=text,
                title=_line(item.get("title")) or text,
                authors=tuple(_author(value) for value in item.get("authors", [])),
                year=_line(item.get("year")),
                doi=doi,
                url=_line(item.get("url")),
                version=_line(item.get("version")),
                publisher=_line(item.get("publisher")),
                license=_line(item.get("license")),
            )
        )
    return references


def _ris(references: list[_Reference]) -> str:
    entries = []
    for reference in references:
        fields = [("TY", reference.kind), ("TI", reference.title)]
        fields.extend(("AU", author.ris_name()) for author in reference.authors)
        fields.extend(
            [
                ("PY", reference.year),
                ("DO", reference.doi),
                ("UR", reference.url),
                ("ET", reference.version),
                ("PB", reference.publisher),
                ("N1", reference.text),
            ]
        )
        if reference.license:
            fields.append(("N1", "License: " + reference.license))
        lines = [f"{tag}  - {_line(value)}" for tag, value in fields if value]
        lines.append("ER  - ")
        entries.append("\r\n".join(lines))
    return "\r\n\r\n".join(entries) + "\r\n"


def _citation_format(dformat: Optional[str], path: Optional[Union[str, Path]]) -> str:
    suffixes = {".ris": "ris", ".txt": "text"}
    inferred = None
    if path is not None:
        inferred = suffixes.get(Path(path).suffix.lower())
        if inferred is None:
            raise EcoPlotsError("Citation files must use a .ris or .txt extension.")
    selected = dformat if dformat is not None else inferred or "text"
    if selected not in {"text", "ris"}:
        raise EcoPlotsError("Citation dformat must be 'text' or 'ris'.")
    if inferred is not None and inferred != selected:
        raise EcoPlotsError(f"Citation file extension does not match dformat={selected!r}.")
    return selected


def _render_citations(response, dformat, path):
    references = [*_data_references(response), _software_reference()]
    content = (
        _ris(references)
        if dformat == "ris"
        else _CitationText("\n\n".join(reference.text for reference in references))
    )
    if path is None:
        return content
    target = Path(path).expanduser()
    target.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(dir=target.parent) as directory:
        temporary = Path(directory) / target.name
        temporary.write_bytes(content.encode("utf-8"))
        temporary.replace(target)
    return str(target.resolve())


def _endpoint_error(status):
    if status in {404, 405, 501}:
        raise EcoPlotsError(
            "Citation generation requires the /api/v1.0/data/citations endpoint "
            "on the configured EcoPlots API."
        )


def fetch_citations(base_url: str, query: dict, mode: str):
    """Retrieve citation records for a snapshot of the current filters."""
    with requests.post(
        f"{base_url}/api/v1.0/data/citations",
        json={"query": copy.deepcopy(query), "context": mode},
        timeout=60,
    ) as response:
        _endpoint_error(response.status_code)
        response.raise_for_status()
        return response.json()
