Package Overview
================

.. automodule:: terndata.ecoplots
   :members:
   :undoc-members:
   :show-inheritance:

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


