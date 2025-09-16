.. raw:: html

   <div class="eco-hero eco-left">
     <div class="eco-hero__brand">
       <img class="only-light eco-hero__logo" src="_static/img/ecoplots-logo-light.svg" alt="EcoPlots logo">
       <img class="only-dark  eco-hero__logo" src="_static/img/ecoplots-logo-dark.svg"  alt="EcoPlots logo">
       <span class="eco-hero__title">EcoPlots Python Library</span>
     </div>

     <p class="eco-hero__tagline">
       Discover, filter, preview, and retrieve data from the TERN EcoPlots Portal.
     </p>

     <p class="eco-hero__cta">
       <a class="eco-btn eco-btn--primary" href="api/ecoplots_class.html">EcoPlots Client</a>
       <a class="eco-btn eco-btn--outline" href="api/overview.html">Overview</a>
     </p>

     <div class="eco-badges">
       <a class="eco-badge" href="https://github.com/ternaustralia/terndata.ecoplots" target="_blank" rel="noopener">
         <img src="_static/img/github-mark.svg" alt="GitHub"> <span>GitHub</span>
       </a>
       <a class="eco-badge" href="https://pypi.org/project/terndata.ecoplots/" target="_blank" rel="noopener">
         <img src="https://img.shields.io/pypi/v/terndata.ecoplots.svg?logo=pypi&logoColor=white" alt="PyPI">
         <span>PyPI</span>
      </a>
       <a class="eco-badge" href="https://ecoplots.tern.org.au" target="_blank" rel="noopener">
         <img class="only-light" src="_static/img/ecoplots-logo-light.svg" alt="EcoPlots">
         <img class="only-dark"  src="_static/img/ecoplots-logo-dark.svg"  alt="EcoPlots">
         <span>EcoPlots Portal</span>
       </a>
     </div>
   </div>

What is EcoPlots?
==================
TERN EcoPlots is a platform designed for searching, discovering, and accessing ecological observations—both from systematic site-based surveys and opportunistic surveys—as well as specimen samples collected during field surveys from various data sources. With TERN EcoPlots, users can search for observation data and specimen samples from systematic surveys across Australia. The platform allows users to integrate data from multiple sources and access it as a comprehensive, ready-to-use data package. Additionally, users can search for specimen samples and request access to these samples for further research.

TERN EcoPlots is developed based on a semantic data integration approach. Datasets are generally received from custodians in various forms, including PostgreSQL databases and CSV file formats. In the data ingestion process, each source dataset is mapped to a `TERN Plot ontology <https://linkeddata.tern.org.au/information-models/tern-ontology>`_ including the identification and mapping of domain feature types, parameters, and categorical values to controlled vocabularies, as well as the performance of data validation routines and the resolution of taxonomic names. All data are organised in Resource Description Framework (RDF) and stored in a triple store.

See also: `EcoPlots Portal <https://ecoplots.tern.org.au>`_.

What is the EcoPlots Python library?
====================================
The EcoPlots Python library provides a small, Pythonic client for the EcoPlots REST API, so you can programmatically discover datasets, apply validated filters, preview results, and retrieve analysis-ready data.

Key capabilities:

- **Discovery & filtering**: human-friendly filters that are validated against the portal’s controlled vocabularies.
- **Preview first**: inspect results quickly (page-wise) before downloading full datasets.
- **Spatial selection**: filter using polygons, rectangles, or WKT/GeoJSON.
- **Tidy outputs**: return formats include ``pandas.DataFrame``, ``geopandas.GeoDataFrame`` (optional), or raw GeoJSON.
- **Two runtimes**:
  - :class:`~terndata.ecoplots.ecoplots.EcoPlots` (synchronous) for scripts and notebooks.
  - :class:`~terndata.ecoplots.ecoplots.AsyncEcoPlots` (asynchronous) for web backends and concurrent I/O.
- **Reproducibility**: save and reload selections/projects via ``.ecoproj`` files.

Installation
============
.. code-block:: bash

   pip install terndata.ecoplots

Next steps
==========
- :doc:`Package overview <api/overview>`
- :doc:`EcoPlots Client <api/ecoplots_class>`
- :doc:`Async EcoPlots client <api/async_ecoplots_class>`

.. toctree::
   :hidden:
   :maxdepth: 2

   api

.. toctree::
   :hidden:
   :maxdepth: 1

   internals
