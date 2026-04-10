AsyncEcoPlots
=============

Overview
--------

``AsyncEcoPlots`` is the asynchronous client. It is useful when you already run
async code (for example in services/apps) and want non-blocking data download.

In practice, most methods are used the same way as the synchronous client.
The key difference is that you use ``await`` for ``get_data()``.

Constructor Parameters
----------------------

Signature:

.. code-block:: python

   AsyncEcoPlots(filterset=None, query_filters=None, mode="observations")

What each parameter means:

.. list-table::
   :header-rows: 1
   :widths: 18 16 30 36

   * - Parameter
     - Type
     - What it sets
     - Should you set it manually?
   * - ``filterset``
     - ``dict`` or ``None``
     - Starting human-readable filters.
     - Usually no. Prefer using ``select()`` after creation.
   * - ``query_filters``
     - ``dict`` or ``None``
     - Internal API-ready filter values.
     - No. Keep this internal.
   * - ``mode``
     - ``str``
     - ``"observations"`` (default) or ``"samples"``.
     - Yes, only when you intentionally need samples workflows.

Internal Variables: Do Not Edit Manually
----------------------------------------

Avoid directly changing these internal attributes:

- ``_filters``
- ``_query_filters``
- ``_mode``
- ``_base_url``

Use public methods instead (for example ``select()``, ``remove()``,
``summary()``, ``preview()`` and ``get_data()``).

Async Usage Note
----------------

- ``EcoPlots``: ``df = ec.get_data()``
- ``AsyncEcoPlots``: ``df = await ec.get_data()``

.. autoclass:: terndata.ecoplots.ecoplots.AsyncEcoPlots
   :members:
   :undoc-members:
   :inherited-members: terndata.ecoplots._base.EcoPlotsBase
   :special-members: __str__, __repr__, __eq__, __bool__, __len__, __contains__, __getitem__, __setitem__, __delitem__, __copy__, __deepcopy__
   :show-inheritance:
   :member-order: bysource

Magic Methods
-------------

The ``AsyncEcoPlots`` class inherits all magic methods from ``EcoPlotsBase`` for intuitive interactions:

**Display & Representation:**
  - ``__str__()`` - Pretty-printed box format showing filters and version
  - ``__repr__()`` - Evaluable string representation for debugging

**Comparison:**
  - ``__eq__(other)`` - Compare two instances for structural equality
  - ``__bool__()`` - Check if any filters are applied (``if ecoplots:``)

**Container Operations:**
  - ``__len__()`` - Count active filters (``len(ecoplots)``)
  - ``__contains__(key)`` - Check if filter exists (``"site_id" in ecoplots``)
  - ``__getitem__(key)`` - Get filter values (``ecoplots["site_id"]``)
  - ``__setitem__(key, value)`` - Set filter (``ecoplots["dataset"] = "TERN"``)
  - ``__delitem__(key)`` - Remove filter (``del ecoplots["site_id"]``)

**Copying:**
  - ``__copy__()`` - Shallow copy support (``copy.copy(ecoplots)``)
  - ``__deepcopy__(memo)`` - Deep copy support (``copy.deepcopy(ecoplots)``)


