AsyncEcoPlots
=============

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


