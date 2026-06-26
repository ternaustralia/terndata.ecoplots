Change Log
==========

Changes over time for the EcoPlots Python library.

v1.1.1 (2026-06-26)
-------------------

- Added Parquet output to the standard installation and kept optional extras focused on async transport and GUI widgets.
- Added ``EcoPlots("samples")`` constructor shorthand, mode validation, fuzzy mode resolution, and mode-specific init messages.
- Added ``EcoPlots.discover(facet)`` as an intuitive dispatcher while keeping the existing ``get_*`` discovery methods.
- Added ``get_sites(include_region=True)`` for observations and samples, flattening returned region types into dataframe columns.
- Added site and site-visit attribute retrieval via ``get_site_attributes_data()`` and ``get_site_visit_attributes_data()``.
- Added synchronous data fetching paths for the synchronous client to avoid unnecessary async runtime overhead.
- Added async ``get_data_stream()`` for streaming selected outputs from ``AsyncEcoPlots``.
- Added ``export_data()`` for direct file export with extension validation for CSV, Parquet, GeoJSON, and vector geospatial formats.
- Added ``py.typed`` typing marker and citation metadata via ``CITATION.cff``.
- Updated documentation and examples for observations, samples, async, GUI, Parquet, attributes, region discovery, and file export.
- Updated deployment workflows so pushes to ``main`` publish PyPI development prereleases and GitHub releases publish production distributions from the same workflow file.
- Development package versions now resolve to the EcoPlots test API, while production releases resolve to the production EcoPlots API.

v1.0.0 (2026-05-05)
--------------------

- First stable production release of ``terndata.ecoplots``.
- Versioning updated across package and documentation to ``1.0.0``.

Beta v7
-------

- Updated Documentations.
- Minor bug fixes and imprrovements to the samples workflow.
- Added *Samples Workflow* demo notebook.

Beta v6
-------

- Introduced the **has_image** field in sample discovery responses to indicate the presence of associated images.
- Added the IGSN based sample discovery path to the samples-mode discovery workflow, enabling retrieval of sample metadata based on IGSN values.
- Added the **samples image viewer** and **samples IGSN viewer**.

Beta v5
-------

- Added the **samples workflow**.
- Introduced samples-mode discovery and retrieval paths for material sample use cases.
- Added samples-mode validation constraints, including single ``material_sample_type`` selection for retrieval.

Beta v4
-------

- Updated asynchronous data fetch workflows for improved async retrieval behavior.

Beta v3
-------

- Added the interactive map workflow for spatial selection.
