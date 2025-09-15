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
         <img src="https://img.shields.io/pypi/v/terndata.ecoplots.svg" alt="PyPI">
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
=================
EcoPlots provides a small, Pythonic client for the `EcoPlots Portal <https://ecoplots.tern.org.au>`_,
abstracting dataset discovery, filter validation, quick previews, and data retrieval
into tidy structures (``pandas.DataFrame`` / ``geopandas.GeoDataFrame``) or raw GeoJSON.

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
