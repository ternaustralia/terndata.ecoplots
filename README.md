<p align="center">
  <picture>
    <source media="(prefers-color-scheme: dark)"  srcset="docs/_static/img/ecoplots-logo-dark.svg">
    <source media="(prefers-color-scheme: light)" srcset="docs/_static/img/ecoplots-logo-light.svg">
    <img src="docs/_static/img/ecoplots-logo-light.svg" height="70" alt="EcoPlots logo">
  </picture>
</p>

<h1 align="center"><span style="color:#F5A26C">EcoPlots</span> Python Library</h1>

<p align="center">
  <a href="https://pypi.org/project/terndata.ecoplots/"><img src="https://img.shields.io/pypi/v/terndata.ecoplots.svg?logo=pypi&logoColor=white" alt="PyPI"></a>
  <a href="https://pypi.org/project/terndata.ecoplots/"><img src="https://img.shields.io/pypi/pyversions/terndata.ecoplots.svg?logo=python&logoColor=white" alt="Python versions"></a>
  <a href="https://terndata-ecoplots.readthedocs.io/en/latest/"><img src="https://img.shields.io/readthedocs/terndata-ecoplots.svg?logo=readthedocs" alt="Docs"></a>
  <a href="LICENSE"><img src="https://img.shields.io/github/license/ternaustralia/terndata.ecoplots.svg" alt="License"></a>
  <a href="https://github.com/ternaustralia/terndata.ecoplots"><img src="https://img.shields.io/badge/GitHub-Repo-181717?logo=github&logoColor=white" alt="GitHub"></a>
  <a href="https://ecoplots.tern.org.au"><img src="https://img.shields.io/badge/EcoPlots-Portal-6EB3A6?labelColor=043E4F" alt="EcoPlots Portal"></a>
</p>

---

High-level Python clients for discovering, filtering, previewing, and retrieving
ecological field data from the **[TERN EcoPlots Portal](https://ecoplots.tern.org.au)**.
Supports two operational modes — **observations** and **samples** — and returns
tidy structures ready for analysis (`geojson`, `pandas.DataFrame`, `geopandas.GeoDataFrame`).

---

## Features

- 🔬 **Two modes**: observations (ecological plots) and samples (physical specimens)
- 🔎 Validated, human-friendly filters with fuzzy name resolution
- ⚡ Preview result pages before committing to full downloads
- 🗺️ Interactive spatial selection widget (draw a polygon on a map)
- 🧭 Two clients: synchronous `EcoPlots` and asynchronous `AsyncEcoPlots`
- 💾 Save / load projects via `.ecoproj` files for reproducible workflows

**Documentation:** https://terndata-ecoplots.readthedocs.io

---

## Installation

    pip install terndata.ecoplots

Supported Python: 3.10+

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
gdf = ec.get_data()                       # full pull as GeoDataFrame
```

### Samples

Retrieve physical specimens — soil pit samples, plant voucher specimens, plant
tissue samples, and more — with access to IGSN identifiers and sample images.

```python
from terndata.ecoplots import EcoPlots

ec = EcoPlots(mode="samples")
ec.select(material_sample_type="Plant Voucher Specimen",
          has_images=True)
df = ec.get_data(dformat="pd")
```

> **Note:** In samples mode the *TERN Ecosystem Surveillance* dataset is applied
> automatically and cannot be removed.

---

## Interactive Widgets

Both modes provide notebook widgets for interactive data exploration:

| Widget | Mode | Method |
|---|---|---|
| Spatial selector | Both | `ec.select_spatial()` |
| IGSN viewer | Samples | `ec.view_sample_igsn()` |
| Sample image viewer | Samples | `ec.view_sample_images()` |

---

## Async client

```python
from terndata.ecoplots import AsyncEcoPlots

ec = AsyncEcoPlots()
ec.select(site_id="TCFTNS0002")
gdf = await ec.get_data()        # non-blocking fetch
```

---

## Demo

For example usage, see the [demo notebook](examples/demo.ipynb).

---

## Links

- 📚 Docs: https://terndata-ecoplots.readthedocs.io/en/latest/
- 🧭 EcoPlots Portal: https://ecoplots.tern.org.au
- 🧑‍💻 Source: https://github.com/ternaustralia/terndata.ecoplots
- 📦 PyPI: https://pypi.org/project/terndata.ecoplots/

---

## Contributing

Issues and pull requests are welcome — please open an issue to discuss substantial changes.

Build docs locally:

    pip install -r docs/requirements.txt
    make -C docs html

Run tests:

    make test

Build wheels locally:

    make build

---

## Support

For questions or issues, email **esupport@tern.org.au**.

---

## Citation

Terrestrial Ecosystem Research Network (2026). *terndata.ecoplots: A Python package for accessing TERN EcoPlots data*. https://pypi.org/project/terndata.ecoplots/

---

## License

Licensed under the terms in [LICENSE](LICENSE).  
Copyright © 2026 **TDSA (TERN Data Services and Analytics)**.  
Author: Avinash Chandra.
