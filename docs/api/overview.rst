Package Overview
================

``terndata.ecoplots`` is a Python client for the TERN EcoPlots platform.
It helps you discover, filter, preview, and download ecological plot data in a
workflow that feels familiar to ecologists.

The package is designed so you can:

- Start with simple filters (for example site, dataset, region)
- Preview results before full download
- Retrieve data as ``pandas``/``geopandas`` tables for analysis
- Save your current setup and reuse it later

If you are new to Python, the most important thing to know is:
you usually only need to set filters with methods like ``select()``.
Most internal variables are managed by the library.

Client Types: Sync vs Async
---------------------------

The library provides two clients. They behave almost the same, except for
how ``get_data()`` is called.

.. list-table::
   :header-rows: 1
   :widths: 18 24 38 20

   * - Client
     - Best for
     - When to use
     - ``get_data()`` call
   * - ``EcoPlots``
     - Beginners, notebooks, simple scripts
     - You want the simplest workflow and do not need ``await``.
     - ``df = ec.get_data()``
   * - ``AsyncEcoPlots``
     - Larger fetches, apps/services, advanced users
     - You are already using async code and want non-blocking downloads.
     - ``df = await ec.get_data()``

Important:

- Methods such as ``select()``, ``summary()``, and ``preview()`` are used in the same way for both clients.
- The practical difference is mainly the final data fetch step (``get_data()``).

Constructor Parameters (``__init__``)
-------------------------------------

Both classes share the same constructor:

.. code-block:: python

    EcoPlots(filterset=None, query_filters=None, mode="observations")
    AsyncEcoPlots(filterset=None, query_filters=None, mode="observations")

Parameter guide
~~~~~~~~~~~~~~~

.. list-table::
   :header-rows: 1
   :widths: 18 16 34 32

   * - Parameter
     - Type
     - What it sets
     - Should you set it manually?
   * - ``filterset``
     - ``dict`` or ``None``
     - Human-readable filters loaded at startup.
     - Usually leave as ``None`` and use ``select()`` in normal use.
   * - ``query_filters``
     - ``dict`` or ``None``
     - Internal API-ready filter values (typically resolved URIs).
     - No. This is internal state and should usually not be edited.
   * - ``mode``
     - ``str``
     - Data domain: ``"observations"`` (default) or ``"samples"``.
     - Yes, if you intentionally want samples workflows.

What ``mode`` changes
~~~~~~~~~~~~~~~~~~~~~

- ``mode="observations"``:
    Standard ecological observation workflows.
- ``mode="samples"``:
    Enables sample-focused workflows and methods (for example material sample type, IGSN, soil/sample utilities).

In ``samples`` mode, the library enforces a required dataset selection
(``TERN Ecosystem Surveillance``) internally so the query stays valid.
Do not manually remove this from internal state.

Variables You Should Not Change Manually
----------------------------------------

For most users, avoid editing internal attributes directly.
In particular, do not manually modify:

- ``_query_filters`` (internal API filter representation)
- ``_filters`` (managed by helper methods such as ``select()`` and ``remove()``)
- ``_mode`` after initialization (set ``mode=...`` when creating the client)
- ``_base_url`` (managed by package configuration)

Recommended practice is:

1. Create a client.
2. Apply filters via public methods.
3. Preview/summary.
4. Call ``get_data()``.

Beginner-Friendly Example
-------------------------

.. code-block:: python

    from terndata.ecoplots import EcoPlots

    # Start simple with default mode="observations"
    ec = EcoPlots()

    # Add filters using public API (recommended)
    ec.select(site_id="TCFTNS0002", dataset="TERN Ecosystem Surveillance")

    # Check a small preview first
    preview_df = ec.preview(dformat="pd")

    # Fetch full data
    data_gdf = ec.get_data()

Magic Methods & Pythonic API
-----------------------------

Both ``EcoPlots`` and ``AsyncEcoPlots`` support intuitive Pythonic operations through magic methods:

.. code-block:: python

   from terndata.ecoplots import EcoPlots
   
   ec = EcoPlots()
   
   # Dict-like access
   ec["site_id"] = "TCFTNS0002"
   ec["dataset"] = "TERN Ecosystem Surveillance"
   
   # Check filters
   if "site_id" in ec:
       print(f"Site ID: {ec['site_id']}")
   
   # Count filters
   print(f"Active filters: {len(ec)}")
   
   # Boolean check
   if ec:  # True if any filters are set
       print("Filters are configured")
   
   # Beautiful display
   print(ec)  # Shows professional box-formatted output
   
   # Comparison
   ec2 = EcoPlots()
   ec2["site_id"] = "TCFTNS0002"
   print(ec == ec2)  # False (different datasets)
   
   # Copying
   import copy
   ec_copy = copy.deepcopy(ec)

See the :doc:`EcoPlots <ecoplots_class>` and :doc:`AsyncEcoPlots <async_ecoplots_class>` class documentation for detailed method descriptions.

.. automodule:: terndata.ecoplots
    :members:
    :undoc-members:
    :show-inheritance:
    :exclude-members: EcoPlotsError
