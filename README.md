# EcoPlots Python Library

[![PyPI](https://img.shields.io/pypi/v/terndata.ecoplots.svg?logo=pypi&logoColor=white)](https://pypi.org/project/terndata.ecoplots/)
[![Python versions](https://img.shields.io/pypi/pyversions/terndata.ecoplots.svg?logo=python&logoColor=white)](https://pypi.org/project/terndata.ecoplots/)
[![Docs](https://img.shields.io/readthedocs/terndata-ecoplots.svg?logo=readthedocs)](https://terndata-ecoplots.readthedocs.io/en/latest/)
[![License](https://img.shields.io/github/license/ternaustralia/terndata.ecoplots.svg)](LICENSE)
[![GitHub](https://img.shields.io/badge/GitHub-Repo-181717?logo=github&logoColor=white)](https://github.com/ternaustralia/terndata.ecoplots)
[![EcoPlots Portal](https://img.shields.io/badge/EcoPlots-Portal-6EB3A6?labelColor=043E4F)](https://ecoplots.tern.org.au)

High-level Python clients for discovering, filtering, previewing, and retrieving
ecological plot data from the **[TERN EcoPlots Portal](https://ecoplots.tern.org.au)**.
Returns tidy structures for analysis (`pandas.DataFrame`, `geopandas.GeoDataFrame`)
or raw `GeoJSON`.

---

## Features

- 🔎 Discover datasets with validated, human-friendly filters  
- ⚡ Preview result pages before full downloads  
- 🗺️ Spatial selection (GeoJSON/WKT)  
- 🧭 Two clients: synchronous `EcoPlots` and asynchronous `AsyncEcoPlots`  
- 💾 Save / load projects via `.ecoproj` for reproducible workflows

**Documentation:** https://terndata-ecoplots.readthedocs.io

---

## Installation

    pip install terndata.ecoplots

Supported Python: 3.10+

---

## Quick start

    from terndata.ecoplots import EcoPlots

    ec = EcoPlots()
    ec.select(site_id="TCFTNS0002")    # add validated filters
    preview = ec.preview().head()      # quick look (first page)
    gdf = ec.get_data()                # full pull (GeoDataFrame)

### Async

    import asyncio
    from terndata.ecoplots import AsyncEcoPlots

    async def main():
        ec = AsyncEcoPlots()
        ec.select(site_id="TCFTNS0002")
        gdf = await ec.get_data()
        return gdf

    # asyncio.run(main())

---

## Links

- 📚 Docs (latest): https://terndata-ecoplots.readthedocs.io/en/latest/
- 🧭 EcoPlots Portal: https://ecoplots.tern.org.au
- 🧑‍💻 Source: https://github.com/ternaustralia/terndata.ecoplots
- 📦 PyPI: https://pypi.org/project/terndata.ecoplots/

---

## Contributing

Issues and pull requests are welcome—please open an issue to discuss substantial changes.

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

Terrestrial Ecosystem Research Network (2025). *terndata.ecoplots: A Python package for accessing TERN EcoPlots data*. https://pypi.org/project/terndata.ecoplots/

---

## License

Licensed under the terms in [LICENSE](LICENSE).  
Copyright © 2025 **TDSA (TERN Data Services and Analytics)**.
Author: Avinash Chandra.
