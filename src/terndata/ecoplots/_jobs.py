"""Job submission, polling, and retrieval for observation data."""

import asyncio
import copy
import io
import math
import tempfile
import time
from pathlib import Path
from typing import Any, Union
from urllib.parse import urljoin

import orjson
import pandas as pd
import requests

from ._exceptions import EcoPlotsError
from ._utils import _align_and_concat

_SUCCESS = {"succeded", "succeeded"}
_TERMINAL = _SUCCESS | {"failed", "cancelled"}
_STATUSES = _TERMINAL | {"queued", "running"}
_POLL_INTERVAL = 1.0
_JOB_TIMEOUT = 3000.0


def _aiohttp():
    try:
        import aiohttp
    except ImportError as exc:
        raise EcoPlotsError(
            "Async data retrieval requires aiohttp. Install it with: "
            "pip install 'terndata.ecoplots[async]'"
        ) from exc
    return aiohttp


def _read_csv(payload: Union[bytes, bytearray], preserve_strings: bool = False) -> pd.DataFrame:
    if not payload.strip():
        return pd.DataFrame()
    if preserve_strings:
        return pd.read_csv(io.BytesIO(payload), dtype=str, keep_default_na=False)
    return pd.read_csv(io.BytesIO(payload))


def _csv_bytes(frame: pd.DataFrame) -> bytes:
    return frame.to_csv(index=False).encode("utf-8")


def _save_payload(path: Union[str, Path], payload: bytes) -> str:
    target = Path(path).expanduser()
    target.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(dir=target.parent) as directory:
        temporary = Path(directory) / target.name
        temporary.write_bytes(payload)
        temporary.replace(target)
    return str(target.resolve())


class _JobGroup:
    def __init__(self, base_url: str, dformat: str, jobs: list[dict[str, Any]]):
        self._base_url = base_url.rstrip("/") + "/"
        self.dformat = dformat
        self._jobs = copy.deepcopy(jobs)
        for job in self._jobs:
            self._validate(job)

    @staticmethod
    def _validate(job):
        if not isinstance(job, dict) or not job.get("job_id"):
            raise EcoPlotsError("Invalid data job response: missing job_id.")
        if job.get("status") not in _STATUSES:
            raise EcoPlotsError(
                f"Unknown status for data job {job['job_id']}: {job.get('status')!r}"
            )

    @property
    def jobs(self) -> list[dict[str, Any]]:
        """Return metadata for the underlying server jobs."""
        return copy.deepcopy(self._jobs)

    @property
    def job_ids(self) -> list[str]:
        """Return the underlying server job IDs."""
        return [job["job_id"] for job in self._jobs]

    @property
    def status(self) -> str:
        """Return cached overall status; call refresh() to update it."""
        statuses = {job["status"] for job in self._jobs}
        if "failed" in statuses:
            return "failed"
        if "cancelled" in statuses:
            return "cancelled"
        if statuses <= _SUCCESS:
            return "succeeded"
        if statuses == {"queued"}:
            return "queued"
        return "running"

    @property
    def bytes_written(self) -> int:
        """Return the total bytes reported by the server jobs."""
        return sum(job.get("bytes_written", 0) for job in self._jobs)

    @property
    def chunks_written(self) -> int:
        """Return the total chunks reported by the server jobs."""
        return sum(job.get("chunks_written", 0) for job in self._jobs)

    def _url(self, job, kind):
        value = job.get(f"{kind}_url")
        if not value:
            raise EcoPlotsError(f"Data job {job['job_id']} has no {kind}_url.")
        return urljoin(self._base_url, value)

    def _update(self, job, metadata):
        self._validate(metadata)
        if metadata["job_id"] != job["job_id"]:
            raise EcoPlotsError("Data job status response has a different job_id.")
        job.update(metadata)

    def _check(self):
        for job in self._jobs:
            if job["status"] in {"failed", "cancelled"}:
                detail = job.get("error_message") or job["status"]
                code = job.get("error_code")
                raise EcoPlotsError(
                    f"Data job {job['job_id']} {job['status']}: {detail}"
                    + (f" ({code})" if code else "")
                )

    @staticmethod
    def _deadline(timeout, poll_interval):
        if not math.isfinite(timeout) or timeout <= 0:
            raise ValueError("timeout must be a positive, finite number of seconds.")
        if not math.isfinite(poll_interval) or poll_interval < 0:
            raise ValueError("poll_interval must be a non-negative, finite number of seconds.")
        return time.monotonic() + timeout

    def _remaining(self, deadline):
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise TimeoutError(f"Timed out waiting for data jobs: {', '.join(self.job_ids)}")
        return remaining

    @staticmethod
    def _check_size(job, size):
        expected = job.get("bytes_written")
        if expected is not None and size != expected:
            raise EcoPlotsError(
                f"Incomplete output for data job {job['job_id']}: "
                f"received {size} bytes, expected {expected}."
            )

    def _refresh_one_sync(self, job, deadline):
        request_timeout = min(60, self._remaining(deadline))
        # TIM100 only recognizes literal timeouts; this value follows the deadline.
        with requests.get(self._url(job, "status"), timeout=request_timeout) as r:  # noqa: TIM100
            r.raise_for_status()
            self._update(job, r.json())

    def refresh(self):
        """Refresh server metadata and return this handle."""
        deadline = time.monotonic() + 60
        for job in self._jobs:
            self._refresh_one_sync(job, deadline)
        return self


class DataJob(_JobGroup):
    """A submitted observation query with status, streaming, and download access.

    Obtain a handle with ``EcoPlots.submit_data_job()``. CSV queries manage
    feature-type jobs internally. Status is cached until ``refresh()`` is called.
    """

    def _wait_one(self, job, deadline, poll_interval, streaming=False):
        ready = _SUCCESS | ({"running"} if streaming else set())
        while True:
            self._check()
            self._remaining(deadline)
            if job["status"] in ready:
                return
            self._refresh_one_sync(job, deadline)
            self._check()
            if job["status"] not in ready:
                time.sleep(min(poll_interval, self._remaining(deadline)))

    def wait(self, *, timeout: float = _JOB_TIMEOUT, poll_interval: float = _POLL_INTERVAL):
        """Wait for successful completion and return this handle."""
        deadline = self._deadline(timeout, poll_interval)
        for job in self._jobs:
            self._wait_one(job, deadline, poll_interval)
        return self

    def _iter_content(self, job, deadline, poll_interval, streaming):
        self._wait_one(job, deadline, poll_interval, streaming)
        kind = "stream" if streaming else "download"
        size = 0
        request_timeout = self._remaining(deadline)
        # TIM100 only recognizes literal timeouts; this value follows the deadline.
        with requests.get(  # noqa: TIM100
            self._url(job, kind), stream=True, timeout=request_timeout
        ) as r:
            r.raise_for_status()
            for chunk in r.iter_content(chunk_size=65536):
                self._remaining(deadline)
                if chunk:
                    size += len(chunk)
                    yield chunk
        self._wait_one(job, deadline, poll_interval)
        self._check_size(job, size)

    def stream(self, *, timeout: float = _JOB_TIMEOUT, poll_interval: float = _POLL_INTERVAL):
        """Yield CSV DataFrames per feature type, or raw GeoJSON bytes.

        Streaming starts when a server job is running. CSV output is buffered
        per feature type before parsing. Failures raise ``EcoPlotsError``.

        Args:
            timeout: Maximum total retrieval time in seconds.
            poll_interval: Seconds between status requests.

        Yields:
            A DataFrame per CSV job, or GeoJSON byte chunks.
        """
        deadline = self._deadline(timeout, poll_interval)
        for job in self._jobs:
            chunks = self._iter_content(job, deadline, poll_interval, True)
            if self.dformat == "csv":
                yield _read_csv(b"".join(chunks))
            else:
                yield from chunks

    def _result(
        self, *, timeout=_JOB_TIMEOUT, poll_interval=_POLL_INTERVAL, preserve_strings=False
    ):
        deadline = self._deadline(timeout, poll_interval)
        if self.dformat == "csv":
            frames = []
            for job in self._jobs:
                payload = b"".join(self._iter_content(job, deadline, poll_interval, False))
                frames.append(_read_csv(payload, preserve_strings))
            return _align_and_concat(frames)
        return b"".join(self._iter_content(self._jobs[0], deadline, poll_interval, False))

    def download(
        self,
        path: Union[str, Path],
        *,
        timeout: float = _JOB_TIMEOUT,
        poll_interval: float = _POLL_INTERVAL,
    ) -> str:
        """Wait and save complete output; CSV results share one aligned header.

        Args:
            path: Destination file. Parent directories are created.
            timeout: Maximum total retrieval time in seconds.
            poll_interval: Seconds between status requests.

        Returns:
            The absolute destination path.
        """
        result = self._result(timeout=timeout, poll_interval=poll_interval, preserve_strings=True)
        payload = _csv_bytes(result) if self.dformat == "csv" else result
        return _save_payload(path, payload)


class AsyncDataJob(_JobGroup):
    """Async job handle returned by ``AsyncEcoPlots.submit_data_job()``."""

    async def _refresh_one(self, session, job, deadline):
        timeout = _aiohttp().ClientTimeout(total=min(60, self._remaining(deadline)))
        async with session.get(self._url(job, "status"), timeout=timeout) as r:
            r.raise_for_status()
            self._update(job, await r.json(loads=orjson.loads))

    async def _wait_one(self, session, job, deadline, poll_interval, streaming=False):
        ready = _SUCCESS | ({"running"} if streaming else set())
        while True:
            self._check()
            self._remaining(deadline)
            if job["status"] in ready:
                return
            await self._refresh_one(session, job, deadline)
            self._check()
            if job["status"] not in ready:
                await asyncio.sleep(min(poll_interval, self._remaining(deadline)))

    async def wait(self, *, timeout: float = _JOB_TIMEOUT, poll_interval: float = _POLL_INTERVAL):
        """Wait for successful completion and return this handle."""
        deadline = self._deadline(timeout, poll_interval)
        async with _aiohttp().ClientSession() as session:
            for job in self._jobs:
                await self._wait_one(session, job, deadline, poll_interval)
        return self

    async def _iter_content(self, session, job, deadline, poll_interval, streaming):
        await self._wait_one(session, job, deadline, poll_interval, streaming)
        kind = "stream" if streaming else "download"
        timeout = _aiohttp().ClientTimeout(total=self._remaining(deadline))
        size = 0
        async with session.get(self._url(job, kind), timeout=timeout) as r:
            r.raise_for_status()
            async for chunk in r.content.iter_chunked(65536):
                self._remaining(deadline)
                size += len(chunk)
                yield chunk
        await self._wait_one(session, job, deadline, poll_interval)
        self._check_size(job, size)

    async def stream(self, *, timeout: float = _JOB_TIMEOUT, poll_interval: float = _POLL_INTERVAL):
        """Yield CSV DataFrames per feature type, or raw GeoJSON bytes.

        Streaming starts when a server job is running. CSV output is buffered
        per feature type before parsing. Failures raise ``EcoPlotsError``.

        Args:
            timeout: Maximum total retrieval time in seconds.
            poll_interval: Seconds between status requests.

        Yields:
            A DataFrame per CSV job, or GeoJSON byte chunks.
        """
        deadline = self._deadline(timeout, poll_interval)
        async with _aiohttp().ClientSession() as session:
            for job in self._jobs:
                chunks = self._iter_content(session, job, deadline, poll_interval, True)
                try:
                    if self.dformat == "csv":
                        buffer = bytearray()
                        async for chunk in chunks:
                            buffer.extend(chunk)
                        yield await asyncio.to_thread(_read_csv, buffer)
                    else:
                        async for chunk in chunks:
                            yield chunk
                finally:
                    await chunks.aclose()

    async def _result(
        self, *, timeout=_JOB_TIMEOUT, poll_interval=_POLL_INTERVAL, preserve_strings=False
    ):
        deadline = self._deadline(timeout, poll_interval)
        dfs = []
        async with _aiohttp().ClientSession() as session:
            for job in self._jobs:
                buffer = bytearray()
                async for chunk in self._iter_content(session, job, deadline, poll_interval, False):
                    buffer.extend(chunk)
                if self.dformat == "csv":
                    dfs.append(await asyncio.to_thread(_read_csv, buffer, preserve_strings))
                else:
                    return await asyncio.to_thread(bytes, buffer)
        return await asyncio.to_thread(_align_and_concat, dfs)

    async def download(
        self,
        path: Union[str, Path],
        *,
        timeout: float = _JOB_TIMEOUT,
        poll_interval: float = _POLL_INTERVAL,
    ) -> str:
        """Wait and save complete output; CSV results share one aligned header."""
        result = await self._result(
            timeout=timeout, poll_interval=poll_interval, preserve_strings=True
        )
        if self.dformat == "csv":
            payload = await asyncio.to_thread(_csv_bytes, result)
        else:
            payload = result
        return await asyncio.to_thread(_save_payload, path, payload)


def submit_jobs(base_url: str, dformat: str, payloads: list[dict]) -> DataJob:
    """Submit server jobs without waiting for output."""
    jobs = []
    for payload in payloads:
        with requests.post(
            f"{base_url}/api/v1.0/data",
            params={"dformat": dformat},
            json=payload,
            timeout=60,
        ) as r:
            r.raise_for_status()
            metadata = r.json()
            _JobGroup._validate(metadata)
            jobs.append(metadata)
    return DataJob(base_url, dformat, jobs)


async def async_submit_jobs(base_url: str, dformat: str, payloads: list[dict]) -> AsyncDataJob:
    """Submit server jobs asynchronously without waiting for output."""
    jobs = []
    aiohttp = _aiohttp()
    async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=60)) as session:
        for payload in payloads:
            async with session.post(
                f"{base_url}/api/v1.0/data", params={"dformat": dformat}, json=payload
            ) as r:
                r.raise_for_status()
                metadata = await r.json(loads=orjson.loads)
                _JobGroup._validate(metadata)
                jobs.append(metadata)
    return AsyncDataJob(base_url, dformat, jobs)
