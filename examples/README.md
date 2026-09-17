# EcoPlots examples

This directory contains practical Python and R examples for discovering, filtering,
retrieving, and visualising TERN EcoPlots data.

## Python examples

- [Observations demo](demo.ipynb) — covers discovery, filters, summaries, previews,
  data retrieval, GeoJSON output, and spatial selection.
- [Samples demo](demo_samples.ipynb) — covers sample discovery and filtering, IGSN
  identifiers, soil and species analysis, image viewing, and spatial selection.

The Python examples are Jupyter notebooks. Install the GUI extra to use their
interactive widgets:

```bash
pip install "terndata.ecoplots[gui]"
```

## R examples

The R examples use
[`reticulate`](https://rstudio.github.io/reticulate/) to call the EcoPlots Python
package from R:

- [R setup and usage guide](<R examples/ReadMe.md>)
- [Queensland soil colour](<R examples/terndata.ecoplots_QueenslandSoilColour.R>) —
  downloads QBEIS soil-colour observations and plots their site locations and colours.
- [Trends in species cover](<R examples/terndata.ecoplots_TrendsInSpeciesCover.R>) —
  retrieves observations for a site and visualises changes in species cover between
  visits.
