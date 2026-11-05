Data Jobs
=========

Observation ``get_data()`` calls submit jobs, poll status, and retrieve results
automatically. Existing calls and return formats are unchanged.

Use ``submit_data_job(dformat="csv")`` to submit the selected query and continue
other work. ``"geojson"`` is also supported. CSV feature-type requests are
managed internally.

.. code-block:: python

   job = ec.submit_data_job()
   job.refresh()
   print(job.status)
   job.download("observations.csv")

   for frame in job.stream():
       ...

With ``AsyncEcoPlots``:

.. code-block:: python

   job = aec.submit_data_job()
   job.refresh()
   await job.download("observations.csv")

   async for frame in job.stream():
       ...

``status`` is cached; ``refresh()`` updates it. Overall success is reported as
``"succeeded"``. ``wait()`` waits for completion without downloading.

``stream()`` starts when jobs are running and yields a DataFrame per CSV feature
type, or raw bytes for GeoJSON. CSV data is buffered per feature type before
parsing. ``download(path)`` waits for completion and writes one file, aligning
CSV columns across feature types.

``wait()``, ``stream()``, and ``download()`` accept ``timeout`` (default 3000
seconds) and ``poll_interval`` (default 1 second). Failed or cancelled jobs raise
``EcoPlotsError``; a local timeout raises ``TimeoutError``. The handle can be used
again after a timeout. Local cancellation does not cancel server jobs.

.. autoclass:: terndata.ecoplots.DataJob
   :members:
   :inherited-members:

.. autoclass:: terndata.ecoplots.AsyncDataJob
   :members:
   :inherited-members:
