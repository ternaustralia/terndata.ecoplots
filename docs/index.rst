.. raw:: html

   <div class="eco-hero eco-left">
     <div class="eco-hero__brand">
       <img class="only-light eco-hero__logo" src="_static/img/ecoplots-logo-light.svg" alt="EcoPlots logo">
       <img class="only-dark  eco-hero__logo" src="_static/img/ecoplots-logo-dark.svg"  alt="EcoPlots logo">
       <span class="eco-hero__title">EcoPlots Python Library</span>
     </div>

     <p class="eco-hero__tagline">
       Discover, filter, preview, and retrieve ecological observations and physical
       samples from the TERN EcoPlots Portal.
     </p>

     <p class="eco-hero__cta">
       <a class="eco-btn eco-btn--primary" href="api/ecoplots_observations.html">Observations Workflow</a>
       <a class="eco-btn eco-btn--primary" href="api/ecoplots_samples.html">Samples Workflow</a>
       <a class="eco-btn eco-btn--outline" href="api/overview.html">API Overview</a>
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

About EcoPlots
==============

`TERN EcoPlots <https://ecoplots.tern.org.au>`_ is a platform
for searching, discovering, and accessing Australian ecological data — from systematic
site-based surveys and opportunistic observations, to physical specimen samples
collected during field surveys.

The platform integrates data from multiple custodians into a single, analysis-ready
package underpinned by a semantic data model. Source datasets are mapped to the
`TERN Plot Ontology <https://linkeddata.tern.org.au/information-models/tern-ontology>`_,
where feature types, parameters, and categorical values are linked to controlled
vocabularies and stored as RDF in a triple store.

EcoPlots Python Library
=======================

The ``terndata.ecoplots`` Python library provides a lightweight, Pythonic client
for the EcoPlots REST API. It supports two operational modes:

.. list-table::
   :header-rows: 1
   :widths: 18 82

   * - Mode
     - What you can access
   * - **observations**
     - Ecological observation data — site visits, feature types, measured
       properties — returned as ``pandas``/``geopandas`` tables.
   * - **samples**
     - Physical specimens (soil, plant tissue, plant voucher) with IGSN
       identifiers, sample images, and associated metadata.

Key capabilities:

- Human-friendly, validated filters with fuzzy name resolution
- Preview results page-by-page before committing to a full download
- Interactive widgets: spatial selector, IGSN viewer, sample image viewer
- Two runtimes: :class:`~terndata.ecoplots.ecoplots.EcoPlots` (sync) and
  :class:`~terndata.ecoplots.ecoplots.AsyncEcoPlots` (async)
- Save and reload your filter selection via ``.ecoproj`` project files

Installation
============

.. code-block:: bash

   pip install terndata.ecoplots

Supported Python: 3.10+

Quick start
===========

**Observations**

.. code-block:: python

   from terndata.ecoplots import EcoPlots

   ec = EcoPlots()                          # mode="observations" by default
   ec.select(dataset="TERN Surveillance")
   gdf = ec.get_data()

**Samples**

.. code-block:: python

   from terndata.ecoplots import EcoPlots

   ec = EcoPlots(mode="samples")
   ec.select(material_sample_type="Plant Voucher Specimen", has_images=True)
   df = ec.get_data(dformat="pd")

Next steps
==========

- :doc:`Observations Workflow <api/ecoplots_observations>` — all methods available in observations mode
- :doc:`Samples Workflow <api/ecoplots_samples>` — all methods available in samples mode
- :doc:`Workflow reference <workflows>` — end-to-end user guide for sync and async workflows
- :doc:`Package overview <api/overview>` — constructor parameters, client comparison
- :doc:`EcoPlots Client <api/ecoplots_class>` — full API reference
- :doc:`Async EcoPlots client <api/async_ecoplots_class>`
- `Observations Demo Notebook <https://github.com/ternaustralia/terndata.ecoplots/blob/main/examples/demo.ipynb>`_
- `Samples Demo Notebook <https://github.com/ternaustralia/terndata.ecoplots/blob/main/examples/demo_samples.ipynb>`_

.. toctree::
   :hidden:
   :maxdepth: 2

   api

.. toctree::
   :hidden:
   :maxdepth: 1

  workflows
   api/ecoplots_observations
   api/ecoplots_samples
   internals
   changelog
