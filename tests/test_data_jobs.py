import asyncio
import inspect
import threading

import orjson
import pandas as pd
import pytest

from terndata.ecoplots import (
    AsyncDataJob,
    AsyncEcoPlots,
    DataJob,
    EcoPlots,
    EcoPlotsError,
)


def resolve(value):
    return asyncio.run(value) if inspect.isawaitable(value) else value


def collect(stream):
    async def consume():
        return [chunk async for chunk in stream]

    return asyncio.run(consume()) if hasattr(stream, "__aiter__") else list(stream)


@pytest.fixture(params=[EcoPlots, AsyncEcoPlots], ids=["sync", "async"])
def client(request, data_api):
    ec = request.param(query_filters={"site_id": ["site-uri"]})
    ec._base_url = "https://example.test"
    return ec


def test_submit_returns_one_handle_without_waiting(client, data_api):
    job = client.submit_data_job()

    assert isinstance(job, AsyncDataJob if isinstance(client, AsyncEcoPlots) else DataJob)
    assert not inspect.isawaitable(job)
    assert job.status == "queued"
    assert job.job_ids == ["1", "2"]
    assert all(method == "POST" for method, _, _ in data_api.requests)
    payloads = [kwargs["json"] for _, url, kwargs in data_api.requests if url.endswith("/data")]
    assert payloads == [
        {"query": {"site_id": ["site-uri"], "feature_type": ["feature-a"]}},
        {"query": {"site_id": ["site-uri"], "feature_type": ["feature-b"]}},
    ]
    client._query_filters["site_id"].append("another-site")
    assert payloads[0]["query"]["site_id"] == ["site-uri"]
    metadata = job.jobs
    metadata[0]["status"] = "failed"
    assert job.status == "queued"


def test_geojson_submits_once_without_feature_discovery(client, data_api):
    client._query_filters["feature_type"] = ["feature-a", "feature-b"]
    job = client.submit_data_job(dformat="geojson")

    assert job.job_ids == ["1"]
    assert len(data_api.requests) == 1
    assert data_api.requests[0][2]["json"]["query"]["feature_type"] == ["feature-a", "feature-b"]


def test_refresh_exposes_overall_status_and_progress(client, data_api):
    data_api.statuses = ["running", "succeded"]
    job = client.submit_data_job()

    assert job.refresh() is job
    assert job.status == "running"
    assert job.refresh() is job
    assert job.status == "succeeded"
    assert job.bytes_written == sum(len(value) for value in data_api.csv.values())
    assert job.chunks_written == 2
    assert all(response.closed for response in data_api.responses)


@pytest.mark.parametrize("status", ["succeded", "succeeded"])
def test_wait_accepts_success_statuses(client, data_api, status):
    data_api.statuses = ["queued", "running", status]
    job = client.submit_data_job()

    assert resolve(job.wait(poll_interval=0)) is job
    assert job.status == "succeeded"
    assert not any(url.endswith(("/stream", "/download")) for _, url, _ in data_api.requests)


def test_csv_download_aligns_headers_and_preserves_multiline_fields(client, data_api, tmp_path):
    data_api.csv["feature-a"] = 'site_id,a\n001,"first line\nsecond line, café"\n'.encode()
    job = client.submit_data_job()
    path = tmp_path / "nested" / "observations.csv"

    assert resolve(job.download(path, poll_interval=0)) == str(path.resolve())
    df = pd.read_csv(path, keep_default_na=False)
    assert df["site_id"].tolist() == ["001", "B"]
    assert df.loc[0, "a"] == "first line\nsecond line, café"
    assert df.loc[1, "a"] == "N/A"
    assert df.loc[0, "b"] == "N/A"
    assert len([url for _, url, _ in data_api.requests if url.endswith("/download")]) == 2
    assert not any(url.endswith("/stream") for _, url, _ in data_api.requests)


def test_geojson_download_preserves_output(client, data_api, tmp_path):
    data_api.geojson = '{"type":"FeatureCollection", "features":[], "name":"café"}\n'.encode()
    job = client.submit_data_job(dformat="geojson")
    path = tmp_path / "observations.geojson"

    resolve(job.download(path, poll_interval=0))

    assert path.read_bytes() == data_api.geojson


def test_csv_stream_starts_while_running(client, data_api):
    data_api.statuses = ["running", "succeded"]
    job = client.submit_data_job()

    chunks = collect(job.stream(poll_interval=0))

    assert all(isinstance(chunk, pd.DataFrame) for chunk in chunks)
    assert [chunk["site_id"].iloc[0] for chunk in chunks] == ["A", "B"]
    assert job.status == "succeeded"
    urls = [url for _, url, _ in data_api.requests]
    stream_index = urls.index("https://example.test/api/v1.0/data/1/stream")
    assert urls[stream_index - 1] == "https://example.test/api/v1.0/data/1"
    assert urls[stream_index + 1] == "https://example.test/api/v1.0/data/1"
    assert all(response.closed for response in data_api.responses)


def test_geojson_stream_yields_original_bytes(client, data_api):
    data_api.statuses = ["running", "succeded"]
    data_api.geojson = '{"type":"FeatureCollection","features":[],"name":"café"}'.encode()
    job = client.submit_data_job(dformat="geojson")

    chunks = collect(job.stream(poll_interval=0))

    assert len(chunks) > 1
    assert b"".join(chunks) == data_api.geojson
    assert job.status == "succeeded"


@pytest.mark.parametrize("status", ["failed", "cancelled"])
def test_failed_or_cancelled_jobs_raise_with_details(client, data_api, tmp_path, status):
    data_api.statuses = [status]
    job = client.submit_data_job()
    data_api.jobs["1"]["metadata"].update(error_message="Export stopped", error_code="EXPORT_ERROR")

    with pytest.raises(EcoPlotsError, match=f"Data job 1 {status}: Export stopped.*EXPORT_ERROR"):
        resolve(job.download(tmp_path / "observations.csv", poll_interval=0))

    assert job.status == status
    assert not (tmp_path / "observations.csv").exists()
    assert all(response.closed for response in data_api.responses)


def test_stream_failure_does_not_return_a_csv_chunk(client, data_api):
    data_api.statuses = ["running", "failed"]
    job = client.submit_data_job()

    with pytest.raises(EcoPlotsError, match="Data job 1 failed"):
        collect(job.stream(poll_interval=0))

    assert all(response.closed for response in data_api.responses)


def test_incomplete_stream_raises(client, data_api):
    job = client.submit_data_job()
    resolve(job.wait(poll_interval=0))
    data_api.jobs["1"]["metadata"]["bytes_written"] += 5
    job.refresh()
    # Keep the server's final byte count while returning truncated content.
    data_api.jobs["1"]["payload"] = b"site_id\n"

    with pytest.raises(EcoPlotsError, match="Incomplete output for data job 1"):
        collect(job.stream(poll_interval=0))


def test_timeout_allows_later_retrieval(client, data_api):
    data_api.statuses = ["queued"] * 100
    job = client.submit_data_job()

    with pytest.raises(TimeoutError, match="data jobs: 1, 2"):
        resolve(job.wait(timeout=0.005, poll_interval=0.001))

    for server_job in data_api.jobs.values():
        server_job["statuses"] = ["succeeded"]
    resolve(job.wait(poll_interval=0))
    assert job.status == "succeeded"
    assert len(data_api.jobs) == 2


@pytest.mark.parametrize("dformat", ["parquet", "pd", "json", "invalid"])
def test_submit_rejects_invalid_server_formats(client, data_api, dformat):
    with pytest.raises(EcoPlotsError, match="dformat"):
        client.submit_data_job(dformat=dformat)
    assert not data_api.requests


def test_submit_rejects_samples_mode(client, data_api):
    client._mode = "samples"
    with pytest.raises(EcoPlotsError, match="observations mode only"):
        client.submit_data_job()
    assert not data_api.requests


def test_empty_feature_types_do_not_submit_jobs(client, data_api, tmp_path):
    # The discovery response must retain its URI column for an empty result.
    data_api.feature_types = [None]
    job = client.submit_data_job()
    assert job.status == "succeeded"
    assert job.job_ids == []
    assert collect(job.stream()) == []
    resolve(job.download(tmp_path / "empty.csv"))
    assert len(data_api.requests) == 1


def test_preview_preserves_pagination(data_api):
    ec = EcoPlots(query_filters={"site_id": ["site-uri"]})
    df = ec.preview(dformat="pd")

    assert df["site_id"].tolist() == ["A", "B"]
    payloads = [kwargs["json"] for _, url, kwargs in data_api.requests if url.endswith("/data")]
    assert all(payload["page_number"] == 1 and payload["page_size"] == 5 for payload in payloads)


def test_get_data_geojson_preserves_multiple_feature_types(client, data_api):
    client._query_filters["feature_type"] = ["feature-a", "feature-b"]

    result = resolve(client.get_data(dformat="geojson"))

    assert result == orjson.loads(data_api.geojson)
    assert len(data_api.jobs) == 1
    assert not any("discovery" in url for _, url, _ in data_api.requests)


def test_geojson_lines_decode_split_utf8_characters(client, data_api):
    data_api.geojson = b'{"n":"' + "é".encode() + b'","type":"FeatureCollection",\n"features":[]}\n'
    if isinstance(client, AsyncEcoPlots):
        stream = client.get_data_stream(dformat="geojson")
    else:
        stream = client.iter_fetch_data_lines_sync(dformat="geojson")

    assert collect(stream) == data_api.geojson.decode().splitlines()
    assert all(response.closed for response in data_api.responses)


def test_async_get_data_keeps_submission_and_conversion_off_event_loop(data_api, monkeypatch):
    from terndata.ecoplots import ecoplots as module

    loop_thread = threading.get_ident()
    discovery = EcoPlots._data_job_payloads
    submission = module.submit_jobs
    conversion = module._format_table
    stages = []

    def checked_discovery(*args):
        assert threading.get_ident() != loop_thread
        stages.append("discovery")
        return discovery(*args)

    def checked_conversion(*args):
        assert threading.get_ident() != loop_thread
        stages.append("conversion")
        return conversion(*args)

    def checked_submission(*args):
        assert threading.get_ident() != loop_thread
        stages.append("submission")
        return submission(*args)

    monkeypatch.setattr(EcoPlots, "_data_job_payloads", checked_discovery)
    monkeypatch.setattr(module, "submit_jobs", checked_submission)
    monkeypatch.setattr(module, "_format_table", checked_conversion)
    data_api.delay = 0.001

    async def run():
        ec = AsyncEcoPlots(query_filters={"site_id": ["site-uri"]})
        retrieval = asyncio.create_task(ec.get_data())
        ticks = 0
        while not retrieval.done():
            ticks += 1
            await asyncio.sleep(0)
        assert ticks > 10
        return await retrieval

    assert len(asyncio.run(run())) == 2
    assert stages == ["discovery", "submission", "conversion"]


def test_async_stream_closes_response_on_early_exit(data_api):
    async def run():
        ec = AsyncEcoPlots(query_filters={"site_id": ["site-uri"]})
        job = ec.submit_data_job(dformat="geojson")
        stream = job.stream(poll_interval=0)
        await anext(stream)
        await stream.aclose()

    asyncio.run(run())
    assert all(response.closed for response in data_api.responses)


def test_async_get_data_stream_closes_response_on_early_exit(data_api):
    data_api.geojson = b'{"type":"FeatureCollection",\n"features":[]}\n'

    async def run():
        ec = AsyncEcoPlots(query_filters={"site_id": ["site-uri"]})
        stream = ec.get_data_stream(dformat="geojson")
        await anext(stream)
        await stream.aclose()

    asyncio.run(run())
    assert all(response.closed for response in data_api.responses)


def test_async_wait_propagates_cancellation(data_api):
    async def run():
        ec = AsyncEcoPlots(query_filters={"site_id": ["site-uri"]})
        job = ec.submit_data_job()
        task = asyncio.create_task(job.wait())
        await asyncio.sleep(0)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task

    asyncio.run(run())
    assert all(response.closed for response in data_api.responses)
