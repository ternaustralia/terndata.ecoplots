# EcoPlots Library — Workflow Reference

A quick-reference guide for all supported workflows in both **observations** and **samples** modes.

---

## Clients

Install modules:

```bash
pip install terndata.ecoplots          # sync client, observations/samples, Parquet
pip install "terndata.ecoplots[async]" # AsyncEcoPlots + async streaming transport
pip install "terndata.ecoplots[gui]"   # Jupyter widgets
```

| | Sync | Async |
|---|---|---|
| **Class** | `EcoPlots` | `AsyncEcoPlots` |
| **`get_data()`** | Blocking | `await`-able |
| **`get_data_stream()`** | — | async iterator |
| **Everything else** | Sync | Sync (inherited) |

```python
from terndata.ecoplots import EcoPlots, AsyncEcoPlots

# Observations (default)
ec = EcoPlots()
aec = AsyncEcoPlots()

# Samples
ec = EcoPlots("samples")
aec = AsyncEcoPlots("samples")
```

---

## Filter Management

All filter methods return `self` — they are fully chainable.

### `select()`

```python
# Keyword arguments
ec.select(site_id="TCFTNS0002")
ec.select(region_type="States and Territories")

# Dict style
ec.select({"site_id": "TCFTNS0002", "dataset": "TERN Ecosystem Surveillance"})

# Chained
ec.select(site_id="TCFTNS0002").select(feature_type="soil profile")
```

Values are fuzzy-resolved against the API vocabulary. If the resulting selection yields zero records the filter is **automatically rolled back** with a warning.

### `remove()`

```python
ec.remove(site_id="TCFTNS0002")          # remove one value
ec.remove(site_id=["A", "B"])            # remove multiple values
ec.remove(site_id=None)                  # remove entire facet
ec.remove({"site_id": None, "dataset": None})
```

### `clear()`

```python
ec.clear()   # reset all filters
             # in samples mode the mandatory dataset filter is preserved
```

### `get_filter()` / `get_api_query_filters()`

```python
ec.get_filter()                   # all human-readable filters
ec.get_filter("site_id")          # one facet
ec.get_api_query_filters()        # all resolved API (URI) filters
ec.get_api_query_filters("dataset")
```

---

## Date Range Filtering

Available in **both** modes. Inputs are parsed tolerantly — day-first is assumed for all-numeric formats (`DD/MM/YYYY`). `MM-DD-YYYY` is never accepted.

```python
# Chainable helpers
ec.from_date("01/01/2020").till("31/12/2022")

# Equivalent using select()
ec.select(date_from="21st May 2020", date_to="May 31 2022")

# Mix of formats — all normalised to YYYY-MM-DD internally
ec.from_date("1st Jan 2021").till("2022-06-30")
```

Accepted formats (examples): `"21/05/2020"`, `"21-05-2020"`, `"21st May 2020"`, `"May 20 2020"`, `"2020-05-21"`, `"March 3rd 2021"`.

---

## Observations Mode

### Filter Facets

| Facet | Description |
|---|---|
| `region_type` | e.g. `"States and Territories"`, `"IBRA7 Bioregions"` |
| `region` | Must be used together with `region_type` |
| `dataset` | e.g. `"TERN Ecosystem Surveillance"` |
| `site_id` | e.g. `"TCFTNS0002"` |
| `site_visit_id` | Site visit identifier |
| `feature_type` | e.g. `"soil profile"`, `"plant occurrence"` |
| `observed_property` | e.g. `"soil pH"` |
| `spatial` | WKT string or GeoJSON geometry dict |
| `project` | Project identifier |
| `date_from` / `date_to` | Date range (any recognisable format) |

### Discovery

```python
ec.get_datasources()            # available datasets
ec.get_sites()                  # available sites
ec.get_sites(include_region=True)  # sites with region columns
ec.get_region_types()           # available region types
ec.get_regions("States and Territories")   # regions for a type
ec.get_feature_types()          # available feature types
ec.get_observed_properties()    # available observed properties
ec.get_used_procedures()        # available used procedures

# Attribute lookups (returns metadata for what's selected)
ec.get_datasources_attributes()
ec.get_sites_attributes()
ec.get_site_visit_attributes()
ec.get_observation_attributes()

# Attribute data values from current filters
ec.get_site_attributes_data()
ec.get_site_visit_attributes_data()
```

### Preview & Summary

```python
ec.summary()                    # record counts as DataFrame
ec.summary(dformat="json")      # raw API dict

ec.preview()                    # first ~10 rows as GeoDataFrame
ec.preview(dformat="pandas")    # as DataFrame
ec.preview(dformat="geojson")   # raw GeoJSON dict
```

### Retrieve Data

```python
gdf = ec.get_data()                          # GeoDataFrame (default)
df  = ec.get_data(dformat="pandas")         # DataFrame
pq  = ec.get_data(dformat="parquet")        # Parquet bytes
gj  = ec.get_data(dformat="geojson")        # GeoJSON object
ec.export_data("outputs/observations.parquet")  # retrieve and save directly
ec.export_data("outputs/observations.csv")
ec.export_data("outputs/observations.geojson")

# Allow full download with no filters (use with caution)
gdf = ec.get_data(allow_full_download=True)

# Async
gdf = await aec.get_data()
gdf = await aec.get_data(dformat="pandas")

async for gdf_chunk in aec.get_data_stream(dformat="gpd"):
    ...

async for parquet_chunk in aec.get_data_stream(dformat="pq"):
    ...
```

### Spatial Widget

```python
widget = ec.select_spatial()    # interactive map — run in a notebook cell
# After drawing a shape, call ec.get_filter("spatial") to retrieve the WKT
```

### Typical Observations Workflow

```python
from terndata.ecoplots import EcoPlots

ec = EcoPlots()

# 1. Discover
ec.get_datasources()
ec.get_feature_types()

# 2. Filter
ec.select(dataset="TERN Ecosystem Surveillance") \
  .select(feature_type="soil profile") \
  .select(observed_property="soil pH") \
  .from_date("01/01/2019").till("31/12/2023")

# 3. Inspect
ec.summary()
ec.preview()

# 4. Retrieve
gdf = ec.get_data()

# 5. Save project
ec.save("my_soil_query.ecoproj")
```

---

## Samples Mode

The mandatory `TERN Ecosystem Surveillance` dataset is always set — it cannot be removed.

### Filter Facets

| Facet | Description |
|---|---|
| `region_type` | Same as observations |
| `region` | Same as observations |
| `dataset` | Fixed — `"TERN Ecosystem Surveillance"` |
| `site_id` | e.g. `"SAAKAN0002"` |
| `soil_subsite_id` | Integer or list of integers |
| `soil_depth_range` | `[min, max]` or `{"min": x, "max": y}` (metres) |
| `speciesname` | e.g. `"Acacia aneura"` — fuzzy-matched |
| `material_sample_type` | One of the 5 types below |
| `used_procedure` | Procedure URI or label |
| `has_image` | `True` / `False` |
| `spatial` | WKT string or GeoJSON geometry dict |
| `date_from` / `date_to` | Date range (any recognisable format) |

#### Material Sample Types

| Label | Notes |
|---|---|
| `"Plant Tissue Sample"` | |
| `"Plant Voucher Specimen"` | Required for `view_sample_igsn()` and `get_speciesname()` |
| `"Soil Metagenomic Sample"` | Required for `get_soilpit()` |
| `"Soil Pit Sample"` | Required for `get_soil_depth_range()` |
| `"Soil Subsite Sample"` | Required for `get_soil_depth_range()` and `get_soilpit()` |

### Discovery

```python
ec = EcoPlots("samples")

ec.get_datasources()             # always returns TERN Ecosystem Surveillance
ec.get_sites()
ec.get_sites(include_region=True)  # sample sites with region columns
ec.get_region_types()
ec.get_regions("IBRA7 Bioregions")
ec.get_material_sample_types()   # 5 material sample types
ec.get_used_procedures()         # available procedures
```

### Preview & Summary

```python
ec.summary()
ec.preview()                     # returns GeoDataFrame (first 10 rows)
ec.preview(dformat="pandas")     # as DataFrame
# Note: "geojson"/"json" not supported in samples mode
```

### Retrieve Data

```python
# Select exactly one material_sample_type before calling get_data()
ec.select(material_sample_type="Plant Voucher Specimen")

gdf = ec.get_data()                     # GeoDataFrame
df  = ec.get_data(dformat="pandas")    # DataFrame
pq  = ec.get_data(dformat="pq")        # Parquet bytes
ec.export_data("outputs/samples.parquet")
ec.export_data("outputs/samples.csv")

# Async
gdf = await aec.get_data()

async for gdf_chunk in aec.get_data_stream(dformat="gpd"):
    ...
```

### IGSN Viewer

```python
ec.select(material_sample_type="Plant Voucher Specimen")

df_igsn = ec.get_sample_igsn()           # DataFrame: sample_name, igsn

# Interactive DOI viewer widget (notebook)
ec.view_sample_igsn()                    # dropdown of all IGSNs
ec.view_sample_igsn("10.60792/qda030489")          # specific IGSN
ec.view_sample_igsn("https://doi.org/10.60792/...")  # full DOI URL
```

### Soil Analysis

```python
# Depth range — requires Soil Pit Sample or Soil Subsite Sample
ec.select(material_sample_type="Soil Pit Sample")
ec.get_soil_depth_range()        # GeoDataFrame with depth stats

# Soil pit distribution — requires Soil Metagenomic or Soil Subsite Sample
ec.select(material_sample_type="Soil Subsite Sample")
ec.get_soilpit()                 # DataFrame: soilpit, counts

# Depth range filter
ec.select(soil_depth_range=[0, 0.1])
ec.select(soil_depth_range={"min": 0, "max": 0.3})
```

### Species Distribution

```python
# Requires Plant Tissue Sample or Plant Voucher Specimen
ec.select(material_sample_type="Plant Voucher Specimen")

ec.get_speciesname()             # DataFrame: speciesname, count

# Filter to a specific species (fuzzy-matched)
ec.select(speciesname="Acacia aneura")
```

### Sample Image Viewer

```python
ec.select(material_sample_type="Plant Voucher Specimen")
ec.select(has_image=True)

# Lazy — fetches data automatically
viewer = ec.view_sample_images()
viewer   # display in notebook

# Pre-fetched data
df = ec.get_data(dformat="pandas")
viewer = ec.view_sample_images(data=df)
```

### Spatial Widget

```python
widget = ec.select_spatial()   # same as observations mode
```

### Typical Samples Workflow

```python
from terndata.ecoplots import AsyncEcoPlots

ec = AsyncEcoPlots("samples")

# 1. Discover
ec.get_material_sample_types()
ec.get_sites()

# 2. Filter
ec.select(material_sample_type="Plant Voucher Specimen") \
  .select(region_type="States and Territories") \
  .select(region="Queensland") \
  .select(has_image=True) \
  .from_date("2020-01-01").till("2023-12-31")

# 3. Inspect
ec.summary()
ec.preview()

# 4. Retrieve
gdf = await ec.get_data()

# 5. Explore
viewer = ec.view_sample_images(data=gdf.pipe(lambda d: d))
ec.view_sample_igsn()

# 6. Save
ec.save("plant_vouchers_qld.ecoproj")
```

---

## Save & Load Projects

```python
# Save current filter state to a .ecoproj file
path = ec.save()                        # timestamped filename
path = ec.save("my_project.ecoproj")    # custom filename

# Reload
ec2 = EcoPlots.load("my_project.ecoproj")
ec2.get_filter()                        # filters restored

# Works for samples mode too
ec2 = AsyncEcoPlots.load("plant_vouchers_qld.ecoproj")
```

---

## Quick Reference Table

| Method | Obs | Samples | Notes |
|---|:---:|:---:|---|
| `select()` | ✓ | ✓ | Chainable; validates & fuzzy-resolves |
| `remove()` | ✓ | ✓ | Chainable |
| `clear()` | ✓ | ✓ | Samples keeps mandatory dataset |
| `from_date()` / `till()` | ✓ | ✓ | Chainable; smart date parsing |
| `get_filter()` | ✓ | ✓ | |
| `get_api_query_filters()` | ✓ | ✓ | |
| `summary()` | ✓ | ✓ | |
| `preview()` | ✓ | ✓ | Samples: no `geojson`/`json` format |
| `get_data()` | ✓ | ✓ | Async in `AsyncEcoPlots` |
| `export_data()` | ✓ | ✓ | Fetches and saves `.parquet`, `.csv`, `.geojson`, `.gpkg`, `.shp`, `.fgb` |
| `get_data_stream()` | ✓ | ✓ | Async only; `gpd`, `pq`, `geojson` for observations; `gpd`, `pq` for samples |
| `get_datasources()` | ✓ | ✓ | |
| `get_sites()` | ✓ | ✓ | |
| `get_region_types()` | ✓ | ✓ | |
| `get_regions()` | ✓ | ✓ | |
| `get_feature_types()` | ✓ | — | |
| `get_observed_properties()` | ✓ | — | |
| `get_used_procedures()` | ✓ | ✓ | |
| `get_observation_attributes()` | ✓ | — | |
| `get_datasources_attributes()` | ✓ | — | |
| `get_sites_attributes()` | ✓ | — | |
| `get_site_visit_attributes()` | ✓ | — | |
| `get_site_attributes_data()` | ✓ | ✓ | CSV endpoint as DataFrame |
| `get_site_visit_attributes_data()` | ✓ | ✓ | CSV endpoint as DataFrame |
| `get_material_sample_types()` | — | ✓ | |
| `get_sample_igsn()` | — | ✓ | Requires Plant Voucher Specimen |
| `view_sample_igsn()` | — | ✓ | Requires Plant Voucher Specimen |
| `get_soil_depth_range()` | — | ✓ | Requires Soil Pit/Subsite Sample |
| `get_soilpit()` | — | ✓ | Requires Soil Metagenomic/Subsite |
| `get_speciesname()` | — | ✓ | Requires Plant Tissue/Voucher |
| `view_sample_images()` | — | ✓ | |
| `select_spatial()` | ✓ | ✓ | Notebook widget |
| `save()` / `load()` | ✓ | ✓ | `.ecoproj` binary format |
