import asyncio
import copy
from types import SimpleNamespace
from urllib.parse import urlsplit

import orjson
import pytest

from terndata.ecoplots._base import EcoPlotsBase


@pytest.fixture
def no_filter_validation(monkeypatch):
    """Disable API/cache-backed filter validation for local state tests."""

    monkeypatch.setattr(EcoPlotsBase, "_validate_filters", lambda self: True)


class DataAPI:
    """In-memory job API used by sync and async transport tests."""

    def __init__(self):
        self.requests = []
        self.jobs = {}
        self.statuses = ["succeded"]
        self.feature_types = ["feature-a", "feature-b"]
        self.responses = []
        self.delay = 0
        self.citations = {"citations": []}
        self.csv = {
            "feature-a": b"site_id,longitude_Degree,latitude_Degree,a\nA,153.0,-27.0,one\n",
            "feature-b": b"site_id,longitude_Degree,latitude_Degree,b\nB,154.0,-28.0,two\n",
        }
        self.geojson = b'{"type":"FeatureCollection","features":[]}'

    def request(self, method, url, **kwargs):
        self.requests.append((method, url, copy.deepcopy(kwargs)))
        path = urlsplit(url).path
        if path.endswith("/data/citations"):
            return copy.deepcopy(self.citations), b""
        if path.endswith("/discovery/feature_type"):
            return [{"uri": uri} for uri in self.feature_types], b""
        if method == "POST" and path.endswith("/data"):
            job_id = str(len(self.jobs) + 1)
            dformat = kwargs["params"]["dformat"]
            query = kwargs["json"]["query"]
            payload = self.csv[query["feature_type"][0]] if dformat == "csv" else self.geojson
            metadata = {
                "job_id": job_id,
                "status": "queued",
                "format": dformat,
                "status_url": f"/api/v1.0/data/{job_id}",
                "download_url": f"/api/v1.0/data/{job_id}/download",
                "stream_url": f"/api/v1.0/data/{job_id}/stream",
                "bytes_written": 0,
                "chunks_written": 0,
                "error_code": None,
                "error_message": None,
            }
            self.jobs[job_id] = {
                "metadata": metadata,
                "payload": payload,
                "statuses": list(self.statuses),
            }
            return copy.deepcopy(metadata), b""
        parts = path.split("/")
        job_id = parts[4]
        job = self.jobs[job_id]
        if parts[-1] in {"stream", "download"}:
            return None, job["payload"]
        statuses = job["statuses"]
        if statuses:
            job["metadata"]["status"] = statuses.pop(0)
        if job["metadata"]["status"] in {"succeded", "succeeded"}:
            job["metadata"]["bytes_written"] = len(job["payload"])
            job["metadata"]["chunks_written"] = 1
        return copy.deepcopy(job["metadata"]), b""


class SyncDataResponse:
    def __init__(self, api, method, url, kwargs):
        self.metadata, self.content = api.request(method, url, **kwargs)
        self.closed = False
        self.status_code = 200
        api.responses.append(self)

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.closed = True

    def raise_for_status(self):
        pass

    def json(self):
        return self.metadata

    def iter_content(self, chunk_size):
        for offset in range(0, len(self.content), 7):
            yield self.content[offset : offset + 7]


class AsyncDataResponse:
    def __init__(self, api, method, url, kwargs):
        self.api = api
        self.method = method
        self.url = url
        self.kwargs = kwargs
        self.closed = True
        self.status = 200
        self.content = self
        api.responses.append(self)

    async def __aenter__(self):
        await asyncio.sleep(self.api.delay)
        self.metadata, self.payload = self.api.request(self.method, self.url, **self.kwargs)
        self.closed = False
        return self

    async def __aexit__(self, *args):
        self.closed = True

    def raise_for_status(self):
        pass

    async def json(self, **kwargs):
        return self.metadata

    async def iter_chunked(self, chunk_size):
        for offset in range(0, len(self.payload), 7):
            await asyncio.sleep(self.api.delay)
            yield self.payload[offset : offset + 7]


@pytest.fixture
def data_api(monkeypatch):
    """Replace data/discovery HTTP requests with an in-memory API."""
    api = DataAPI()

    def post(url, **kwargs):
        response = SyncDataResponse(api, "POST", url, kwargs)
        if url.endswith("/discovery/feature_type"):
            response.content = orjson.dumps(response.metadata)
            response.closed = True
        return response

    monkeypatch.setattr("terndata.ecoplots._jobs.requests.post", post)
    monkeypatch.setattr(
        "terndata.ecoplots._jobs.requests.get",
        lambda url, **kwargs: SyncDataResponse(api, "GET", url, kwargs),
    )

    class Session:
        async def __aenter__(self):
            return self

        async def __aexit__(self, *args):
            pass

        def post(self, url, **kwargs):
            return AsyncDataResponse(api, "POST", url, kwargs)

        def get(self, url, **kwargs):
            return AsyncDataResponse(api, "GET", url, kwargs)

    transport = SimpleNamespace(
        ClientSession=lambda **kwargs: Session(), ClientTimeout=lambda **kwargs: kwargs
    )
    monkeypatch.setattr("terndata.ecoplots._jobs._aiohttp", lambda: transport)
    monkeypatch.setattr("terndata.ecoplots._base._aiohttp", lambda: transport)
    return api
