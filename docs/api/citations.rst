Citations
=========

``cite()`` returns references for data matching the current filters, followed by
the software citation with the installed version. Observations and samples are
supported. No data download or package creation is required.
The configured EcoPlots API must provide citation lookup support.

.. code-block:: python

   print(ec.cite())
   ec.cite(path="references.ris")
   ris_text = ec.cite(dformat="ris")

The default output is readable text. A ``.ris`` or ``.txt`` path selects the file
format and returns the absolute saved path. RIS files contain a separate entry
for each reference, including the software. Original citation text is retained
when structured bibliographic fields are unavailable.

.. automethod:: terndata.ecoplots.EcoPlots.cite
   :no-index:
