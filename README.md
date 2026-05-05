<p align="center">
  <img src="https://raw.githubusercontent.com/ternaustralia/terndata.ecoplots/main/docs/_static/img/ecoplots-logo-light.svg" height="70" alt="EcoPlots logo">
</p>

<h1 align="center"><span style="color:#F5A26C">EcoPlots</span> Python Library</h1>

<p align="center">
  <a href="https://pypi.org/project/terndata.ecoplots/"><img src="https://img.shields.io/pypi/v/terndata.ecoplots.svg?logo=pypi&logoColor=white" alt="PyPI"></a>
  <a href="https://pypi.org/project/terndata.ecoplots/"><img src="https://img.shields.io/pypi/pyversions/terndata.ecoplots.svg?logo=python&logoColor=white" alt="Python versions"></a>
  <a href="https://terndata-ecoplots.readthedocs.io/en/latest/"><img src="https://img.shields.io/readthedocs/terndata-ecoplots.svg?logo=readthedocs" alt="Docs"></a>
  <a href="https://github.com/ternaustralia/terndata.ecoplots/blob/main/LICENSE"><img src="https://img.shields.io/github/license/ternaustralia/terndata.ecoplots.svg" alt="License"></a>
  <a href="https://github.com/ternaustralia/terndata.ecoplots"><img src="https://img.shields.io/badge/GitHub-Repo-181717?logo=github&logoColor=white" alt="GitHub"></a>
  <a href="https://ecoplots.tern.org.au"><img src="https://img.shields.io/badge/EcoPlots-Portal-6EB3A6?labelColor=043E4F&" alt="EcoPlots Portal"></a>
  <a href="https://terndata-ecoplots.readthedocs.io/en/latest/changelog.html"><img src="https://img.shields.io/badge/Changelog-1.0.0-informational?logo=git&logoColor=white" alt="Changelog"></a>
</p>

---
Python client for discovering, filtering, and retrieving ecological field data from the **[TERN EcoPlots Portal](https://ecoplots.tern.org.au)**. Pagination, streaming, and response normalisation are handled automatically — you get back a clean `pandas.DataFrame`, `geopandas.GeoDataFrame`, or `GeoJSON` (for observations) with no post-processing required.

---

## Features

**Data access**
- 🔬 **Observations & Samples** — Two purpose-built workflows covering ecological plot observations and physical material samples (soil, plant vouchers, tissue) from TERN's national monitoring network.
- 🌐 **Full API abstraction** — Pagination, streaming, and response normalisation are handled automatically; you work with clean Python objects, not raw HTTP.
- 📦 **Analysis-ready outputs** — Observations return a `geopandas.GeoDataFrame` by default; also available as `pandas.DataFrame` or `GeoJSON`. Samples return a `geopandas.GeoDataFrame` by default with option to fetch as `pandas.DataFrame`. No post-processing needed.

**Discovery & filtering**
- 🔎 **Validated filters with fuzzy resolution** — Mistyped or partial filter values are caught and corrected before any request is sent, with a ranked list of suggestions.
- ⚡ **Preview before you download** — Inspect the first page of results instantly to confirm your filters before committing to a full retrieval.
- 🗺️ **Interactive spatial selector** — Draw a bounding polygon on a live map widget directly in Jupyter; the geometry is applied as a filter automatically.

**Developer experience**
- 🧭 **Sync and async clients** — `EcoPlots` for scripts and notebooks; `AsyncEcoPlots` for async/ASGI services and concurrent I/O pipelines.
- 💾 **Reproducible projects** — Save and reload the full discovery state as a `.ecoproj` file for shareable, version-controllable workflows.
- 🖼️ **Notebook widgets** — Built-in IGSN viewer and sample image browser for interactive exploration without leaving the notebook.

📖 **Full documentation:** https://terndata-ecoplots.readthedocs.io/en/latest/

---

## Installation

```sh
pip install terndata.ecoplots
```

Supported Python: **3.10, 3.11, 3.12, 3.13**  
Key dependencies: `aiohttp`, `geopandas`, `ipyleaflet`, `ipywidgets`, `rapidfuzz`, `orjson`

---

## Quick Start

Zero to data in three lines:

```python
from terndata.ecoplots import EcoPlots

ec = EcoPlots()
ec.select(site_id="TCFTNS0002")
gdf = ec.get_data()          # → GeoDataFrame, ready for analysis
```

See [Modes](#modes) for full workflow examples, or jump to the [demo notebooks](#demo-notebooks).

---

## Modes

### Observations (default)

Retrieve ecological observation data — site visits, feature types, and measured
properties — across Australia's TERN monitoring network.

```python
from terndata.ecoplots import EcoPlots

ec = EcoPlots()                           # mode="observations" by default
ec.select(dataset="TERN Surveillance",
          site_id="TCFTNS0002")
ec.preview()                              # quick look (first page)
gdf = ec.get_data()                       # default → GeoDataFrame
df  = ec.get_data(dformat="pd")           # → pandas DataFrame
gjson = ec.get_data(dformat="geojson")      # → GeoJSON (observations only)
```

### Samples

Retrieve physical specimens — soil pit samples, plant voucher specimens, plant
tissue samples, and more — with access to IGSN identifiers and sample images.

```python
from terndata.ecoplots import EcoPlots

ec = EcoPlots(mode="samples")
ec.select(material_sample_type="Plant Voucher Specimen",
          has_images=True)
gdf = ec.get_data()                        # default → GeoDataFrame
```

Samples mode includes two dedicated notebook widgets:

| Widget | Method | Description |
|---|---|---|
| IGSN viewer | `ec.view_sample_igsn()` | Browse International Geo Sample Numbers linked to retrieved specimens |
| Sample image viewer | `ec.view_sample_images()` | Preview photos associated with sample records inline in Jupyter |

> **Note:** In samples mode the *TERN Ecosystem Surveillance* dataset is applied
> automatically and cannot be removed.

### Async client (`AsyncEcoPlots`)

For async/ASGI services or concurrent I/O pipelines, use `AsyncEcoPlots` — it has the same interface as `EcoPlots` with `await`-able retrieval methods. Both modes, all filters, and all widgets are fully supported.

```python
from terndata.ecoplots import AsyncEcoPlots

ec = AsyncEcoPlots()
ec.select(site_id="TCFTNS0002")
gdf = await ec.get_data()        # non-blocking fetch
```

---

## Interactive Widgets

Both modes provide notebook widgets for interactive data exploration:

| Widget | Mode | Method |
|---|---|---|
| Spatial selector | Both | `ec.select_spatial()` |
| IGSN viewer | Samples | `ec.view_sample_igsn()` |
| Sample image viewer | Samples | `ec.view_sample_images()` |

---

## Demo Notebooks

| Mode | Notebook |
|---|---|
| Observations | [examples/demo.ipynb](https://github.com/ternaustralia/terndata.ecoplots/blob/main/examples/demo.ipynb) |
| Samples | [examples/demo_samples.ipynb](https://github.com/ternaustralia/terndata.ecoplots/blob/main/examples/demo_samples.ipynb) |

---

## Links

- 📚 Docs: https://terndata-ecoplots.readthedocs.io/en/latest/
- 🗺️ Workflow Reference: https://terndata-ecoplots.readthedocs.io/en/latest/workflows.html
- 🧭 EcoPlots Portal: https://ecoplots.tern.org.au
- 🧑‍💻 Source: https://github.com/ternaustralia/terndata.ecoplots
- 📦 PyPI: https://pypi.org/project/terndata.ecoplots/

---

## Contributing

Contributions are welcome! Please **open an issue first** to discuss substantial changes before submitting a PR. For small bug fixes, a direct PR is fine.

**Target branch:** `main`

**Development commands:**

| Task | Command |
|---|---|
| Run tests | `make test` |
| Lint | `make check-lint` |
| Type check | `make check-types` |
| Lint + types | `make check` |
| Build docs | `make doc` |
| Build wheel | `make build` |

All checks are also available via `tox` — see `tox.ini` for environment definitions.

---

## Support

- 🐛 **Bug reports & feature requests:** [Open a GitHub issue](https://github.com/ternaustralia/terndata.ecoplots/issues)
- 📧 **Direct enquiries:** [esupport@tern.org.au](mailto:esupport@tern.org.au)
- 📚 **Documentation:** [terndata-ecoplots.readthedocs.io](https://terndata-ecoplots.readthedocs.io/en/latest/)

---

## Citation

Terrestrial Ecosystem Research Network (2026). *terndata.ecoplots: A Python package for accessing TERN EcoPlots data*. https://pypi.org/project/terndata.ecoplots/

---

## License

Licensed under the terms in [LICENSE](https://github.com/ternaustralia/terndata.ecoplots/blob/main/LICENSE).  
Copyright © 2025-2026 [TDSA (TERN Data Services and Analytics)](https://github.com/ternaustralia).  
Author: [Avinash Chandra](https://github.com/avi2413)
