import asyncio

import geopandas as gpd
import orjson
import pandas as pd
import pytest

from terndata.ecoplots import AsyncEcoPlots, EcoPlots, EcoPlotsError
from terndata.ecoplots._base import EcoPlotsBase


class MockEcoPlots(EcoPlots):
    def get_feature_types(self):
        return pd.DataFrame({"uri": ["feature-a", "feature-b"]})

    def fetch_data_sync(self, *args, **kwargs):
        feature_type = kwargs.get("feature_type", ["feature-a"])[0]
        if kwargs.get("dformat") == "csv":
            if feature_type == "feature-a":
                return b"site_id,longitude_Degree,latitude_Degree,a\nA,153.0,-27.0,one\n"
            return b"site_id,longitude_Degree,latitude_Degree,b\nB,154.0,-28.0,two\n"
        return {"type": "FeatureCollection", "features": []}

    async def fetch_data(self, *args, **kwargs):
        feature_type = kwargs.get("feature_type", ["feature-a"])[0]
        if kwargs.get("dformat") == "csv":
            if feature_type == "feature-a":
                return b"site_id,longitude_Degree,latitude_Degree,a\nA,153.0,-27.0,one\n"
            return b"site_id,longitude_Degree,latitude_Degree,b\nB,154.0,-28.0,two\n"
        return {"type": "FeatureCollection", "features": []}


class MockAsyncEcoPlots(AsyncEcoPlots):
    def get_feature_types(self):
        return pd.DataFrame({"uri": ["feature-a", "feature-b"]})

    async def fetch_data(self, *args, **kwargs):
        feature_type = kwargs.get("feature_type", ["feature-a"])[0]
        if kwargs.get("dformat") == "csv":
            if feature_type == "feature-a":
                return b"site_id,longitude_Degree,latitude_Degree,a\nA,153.0,-27.0,one\n"
            return b"site_id,longitude_Degree,latitude_Degree,b\nB,154.0,-28.0,two\n"
        return {"type": "FeatureCollection", "features": []}


class MockSamplesEcoPlots(EcoPlots):
    def fetch_samples_data_sync(self):
        return gpd.GeoDataFrame(
            {"sample_id": ["S1"], "scientific_name": ["Acacia aneura"]},
            geometry=gpd.points_from_xy([133.0], [-23.0], crs="EPSG:4326"),
        )

    async def fetch_samples_data(self):
        return gpd.GeoDataFrame(
            {"sample_id": ["S1"], "scientific_name": ["Acacia aneura"]},
            geometry=gpd.points_from_xy([133.0], [-23.0], crs="EPSG:4326"),
        )


def test_get_data_merges_csv_payloads_as_dataframe():
    ec = MockEcoPlots(query_filters={"site_id": ["site-uri"]})

    df = ec.get_data(dformat="pd")

    assert isinstance(df, pd.DataFrame)
    assert df.to_dict(orient="records") == [
        {
            "site_id": "A",
            "longitude_Degree": 153.0,
            "latitude_Degree": -27.0,
            "a": "one",
            "b": "N/A",
        },
        {
            "site_id": "B",
            "longitude_Degree": 154.0,
            "latitude_Degree": -28.0,
            "a": "N/A",
            "b": "two",
        },
    ]


def test_get_data_returns_geodataframe_by_default():
    ec = MockEcoPlots(query_filters={"site_id": ["site-uri"]})

    gdf = ec.get_data()

    assert isinstance(gdf, gpd.GeoDataFrame)
    assert gdf.crs == "EPSG:4326"
    assert list(gdf.geometry.x) == [153.0, 154.0]


def test_get_data_rejects_full_observations_download_without_opt_in():
    ec = MockEcoPlots()

    with pytest.raises(RuntimeError, match="No filters specified"):
        ec.get_data()


def test_get_data_rejects_invalid_format():
    ec = MockEcoPlots(query_filters={"site_id": ["site-uri"]})

    with pytest.raises(EcoPlotsError, match="Invalid 'dformat'"):
        ec.get_data(dformat="xlsx")


def test_get_data_returns_parquet_bytes(monkeypatch):
    ec = MockEcoPlots(query_filters={"site_id": ["site-uri"]})

    def fake_to_parquet(self, path, index=False):
        assert index is False
        path.write(b"PARQUET")

    monkeypatch.setattr(pd.DataFrame, "to_parquet", fake_to_parquet)

    payload = ec.get_data(dformat="parquet")

    assert payload == b"PARQUET"


def test_export_data_writes_nested_csv(tmp_path):
    ec = MockEcoPlots(query_filters={"site_id": ["site-uri"]})
    path = tmp_path / "nested" / "ecoplots.csv"

    written = ec.export_data(path)

    assert written == str(path.resolve())
    df = pd.read_csv(path)
    assert list(df["site_id"]) == ["A", "B"]


def test_export_data_writes_csv(tmp_path):
    ec = MockEcoPlots(query_filters={"site_id": ["site-uri"]})
    path = tmp_path / "ecoplots.csv"

    written = ec.export_data(path)

    assert written == str(path.resolve())
    assert list(pd.read_csv(path)["site_id"]) == ["A", "B"]


def test_export_data_writes_parquet_bytes(tmp_path, monkeypatch):
    ec = MockEcoPlots(query_filters={"site_id": ["site-uri"]})

    def fake_to_parquet(self, path, index=False):
        path.write(b"PARQUET")

    monkeypatch.setattr(pd.DataFrame, "to_parquet", fake_to_parquet)

    path = tmp_path / "ecoplots.parquet"
    written = ec.export_data(path)

    assert written == str(path.resolve())
    assert path.read_bytes() == b"PARQUET"


def test_export_data_writes_geojson(tmp_path):
    ec = MockEcoPlots(query_filters={"site_id": ["site-uri"]})
    path = tmp_path / "ecoplots.geojson"

    written = ec.export_data(path)

    assert written == str(path.resolve())
    assert orjson.loads(path.read_bytes()) == {"type": "FeatureCollection", "features": []}


def test_export_data_rejects_unknown_extension(tmp_path):
    ec = MockEcoPlots(query_filters={"site_id": ["site-uri"]})

    with pytest.raises(EcoPlotsError, match="Could not infer output format"):
        ec.export_data(tmp_path / "ecoplots.xlsx")


def test_export_data_rejects_dformat_extension_mismatch(tmp_path):
    ec = MockEcoPlots(query_filters={"site_id": ["site-uri"]})

    with pytest.raises(EcoPlotsError, match="not compatible"):
        ec.export_data(tmp_path / "ecoplots.csv", dformat="gpd")


def test_samples_get_data_returns_dataframe_or_geodataframe():
    ec = MockSamplesEcoPlots(mode="samples")

    df = ec.get_data(dformat="pd")
    gdf = ec.get_data(dformat="gpd")

    assert isinstance(df, pd.DataFrame)
    assert not isinstance(df, gpd.GeoDataFrame)
    assert isinstance(gdf, gpd.GeoDataFrame)


def test_samples_get_data_rejects_geojson_format():
    ec = MockSamplesEcoPlots(mode="samples")

    with pytest.raises(EcoPlotsError, match="samples"):
        ec.get_data(dformat="geojson")


def test_async_get_data_merges_csv_payloads_as_dataframe():
    ec = MockAsyncEcoPlots(
        filterset={"site_id": ["TCFTNS0002"]},
        query_filters={"site_id": ["site-uri"]},
    )

    df = asyncio.run(ec.get_data(dformat="pd"))

    assert isinstance(df, pd.DataFrame)
    assert list(df["site_id"]) == ["A", "B"]


def test_async_get_data_stream_yields_geodataframe_chunks():
    ec = MockAsyncEcoPlots(
        filterset={"site_id": ["TCFTNS0002"]},
        query_filters={"site_id": ["site-uri"]},
    )

    async def collect():
        return [chunk async for chunk in ec.get_data_stream(dformat="gpd")]

    chunks = asyncio.run(collect())

    assert len(chunks) == 2
    assert all(isinstance(chunk, gpd.GeoDataFrame) for chunk in chunks)
    assert sorted(chunk["site_id"].iloc[0] for chunk in chunks) == ["A", "B"]


def test_async_get_data_stream_yields_geojson_lines(monkeypatch):
    ec = MockAsyncEcoPlots(
        filterset={"site_id": ["TCFTNS0002"]},
        query_filters={"site_id": ["site-uri"]},
    )

    async def fake_lines(*args, **kwargs):
        yield '{"type":"FeatureCollection",'
        yield '"features":[]}'

    monkeypatch.setattr(ec, "iter_fetch_data_lines", fake_lines)

    async def collect():
        return [line async for line in ec.get_data_stream(dformat="geojson")]

    assert asyncio.run(collect()) == ['{"type":"FeatureCollection",', '"features":[]}']


def test_async_export_data_writes_csv(tmp_path):
    ec = MockAsyncEcoPlots(
        filterset={"site_id": ["TCFTNS0002"]},
        query_filters={"site_id": ["site-uri"]},
    )
    path = tmp_path / "async-ecoplots.csv"

    written = asyncio.run(ec.export_data(path))

    assert written == str(path.resolve())
    df = pd.read_csv(path)
    assert sorted(df["site_id"]) == ["A", "B"]


def test_discover_dispatches_to_get_method(monkeypatch):
    ec = EcoPlots()
    expected = pd.DataFrame({"key": ["site-a"], "uri": ["site-uri"]})

    monkeypatch.setattr(ec, "get_sites", lambda include_region=False: expected)

    assert ec.discover("site_id").equals(expected)


def test_get_sites_skips_region_lookup_by_default(monkeypatch):
    calls = []

    def fake_discover(
        self, discovery_facet, region_type=None, include_region=False, query_filters=None
    ):
        calls.append(include_region)
        return [{"key": "SAABHC0001", "uri": "site-uri"}]

    monkeypatch.setattr(EcoPlotsBase, "discover", fake_discover)

    df = EcoPlots().get_sites()

    assert calls == [False]
    assert df.to_dict(orient="records") == [{"key": "SAABHC0001", "uri": "site-uri"}]


def test_get_sites_can_include_flattened_region_columns(monkeypatch):
    calls = []

    def fake_discover(
        self, discovery_facet, region_type=None, include_region=False, query_filters=None
    ):
        calls.append(
            {
                "discovery_facet": discovery_facet,
                "region_type": region_type,
                "include_region": include_region,
                "query_filters": query_filters,
            }
        )
        return [
            {
                "key": "SAABHC0001",
                "regions": {
                    "IBRA7 Bioregions": "Broken Hill Complex",
                    "States and Territories": "South Australia",
                },
                "uri": "http://linked.data.gov.au/dataset/ausplots/site-saabhc0001",
                "wkt_point": "POINT(140.46516944 -31.92288056)",
            }
        ]

    monkeypatch.setattr(EcoPlotsBase, "discover", fake_discover)

    df = EcoPlots().get_sites(include_region=True)

    assert calls == [
        {
            "discovery_facet": "site_id",
            "region_type": None,
            "include_region": True,
            "query_filters": None,
        }
    ]
    assert "regions" not in df.columns
    assert df.loc[0, "IBRA7 Bioregions"] == "Broken Hill Complex"
    assert df.loc[0, "States and Territories"] == "South Australia"


def test_samples_get_sites_can_include_flattened_region_columns(monkeypatch):
    calls = []

    def fake_discover_samples(self, discovery_facet, region_type=None):
        calls.append({"method": "discover_samples", "discovery_facet": discovery_facet})
        return [
            {
                "key": "SAABHC0001",
                "uri": "http://linked.data.gov.au/dataset/ausplots/site-saabhc0001",
            }
        ]

    def fake_discover(
        self, discovery_facet, region_type=None, include_region=False, query_filters=None
    ):
        calls.append(
            {
                "method": "discover",
                "discovery_facet": discovery_facet,
                "include_region": include_region,
                "query_filters": query_filters,
            }
        )
        return [
            {
                "key": "SAABHC0001",
                "regions": {"IBRA7 Bioregions": "Broken Hill Complex"},
                "uri": "http://linked.data.gov.au/dataset/ausplots/site-saabhc0001",
            }
        ]

    monkeypatch.setattr(EcoPlotsBase, "discover_samples", fake_discover_samples)
    monkeypatch.setattr(EcoPlotsBase, "discover", fake_discover)

    df = EcoPlots("samples").get_sites(include_region=True)

    assert calls == [
        {"method": "discover_samples", "discovery_facet": "site_id"},
        {
            "method": "discover",
            "discovery_facet": "site_id",
            "include_region": True,
            "query_filters": {
                "site_id": ["http://linked.data.gov.au/dataset/ausplots/site-saabhc0001"]
            },
        },
    ]
    assert "regions" not in df.columns
    assert df.loc[0, "IBRA7 Bioregions"] == "Broken Hill Complex"


def test_base_discover_sends_include_region_param(monkeypatch):
    request = {}

    class FakeResponse:
        content = orjson.dumps([])

        def raise_for_status(self):
            return None

    def fake_post(url, params=None, json=None, timeout=None):
        request.update({"url": url, "params": params, "json": json, "timeout": timeout})
        return FakeResponse()

    monkeypatch.setattr("terndata.ecoplots._base.requests.post", fake_post)

    EcoPlotsBase().discover("site_id", include_region=True)

    assert request["url"].endswith("/api/v1.0/discovery/site_id")
    assert request["params"] == [("include-regions", "True")]
    assert request["json"] == {"query": {}}


def test_base_discover_accepts_temporary_query_filters(monkeypatch):
    request = {}

    class FakeResponse:
        content = orjson.dumps([])

        def raise_for_status(self):
            return None

    def fake_post(url, params=None, json=None, timeout=None):
        request.update({"url": url, "params": params, "json": json, "timeout": timeout})
        return FakeResponse()

    monkeypatch.setattr("terndata.ecoplots._base.requests.post", fake_post)

    EcoPlotsBase(query_filters={"dataset": ["dataset-uri"]}).discover(
        "site_id",
        include_region=True,
        query_filters={"site_id": ["site-uri"]},
    )

    assert request["json"] == {"query": {"site_id": ["site-uri"]}}


def test_get_site_attributes_data_fetches_csv(monkeypatch):
    request = {}

    class FakeResponse:
        content = (
            b"id,label,wkt_point,bioregion,plotArea\n"
            b"http://example.test/site/1,NSABBS0001,POINT(148.9 -31.2),"
            b"Brigalow Belt South,10000.0\n"
        )

        def raise_for_status(self):
            return None

    def fake_post(url, params=None, json=None, timeout=None):
        request.update({"url": url, "params": params, "json": json, "timeout": timeout})
        return FakeResponse()

    monkeypatch.setattr("terndata.ecoplots._base.requests.post", fake_post)

    ec = EcoPlots(query_filters={"site_id": ["site-uri"]})
    df = ec.get_site_attributes_data()

    assert request["url"].endswith("/api/v1.0/data/attributes/site")
    assert request["params"] == [("dformat", "csv")]
    assert request["json"] == {"query": {"site_id": ["site-uri"]}}
    assert request["timeout"] == 300
    assert df.to_dict(orient="records") == [
        {
            "id": "http://example.test/site/1",
            "label": "NSABBS0001",
            "wkt_point": "POINT(148.9 -31.2)",
            "bioregion": "Brigalow Belt South",
            "plotArea": 10000.0,
        }
    ]


def test_get_site_visit_attributes_data_fetches_csv(monkeypatch):
    request = {}

    class FakeResponse:
        content = b"id,label,visitStartDate\nvisit-uri,Visit 1,2020-01-01\n"

        def raise_for_status(self):
            return None

    def fake_post(url, params=None, json=None, timeout=None):
        request.update({"url": url, "params": params, "json": json, "timeout": timeout})
        return FakeResponse()

    monkeypatch.setattr("terndata.ecoplots._base.requests.post", fake_post)

    df = EcoPlots(query_filters={"site_visit_id": ["visit-uri"]}).get_site_visit_attributes_data()

    assert request["url"].endswith("/api/v1.0/data/attributes/site-visit")
    assert request["params"] == [("dformat", "csv")]
    assert request["json"] == {"query": {"site_visit_id": ["visit-uri"]}}
    assert df.to_dict(orient="records") == [
        {"id": "visit-uri", "label": "Visit 1", "visitStartDate": "2020-01-01"}
    ]


def test_get_attributes_data_returns_empty_dataframe_for_empty_csv(monkeypatch):
    class FakeResponse:
        content = b""

        def raise_for_status(self):
            return None

    monkeypatch.setattr(
        "terndata.ecoplots._base.requests.post",
        lambda url, params=None, json=None, timeout=None: FakeResponse(),
    )

    df = EcoPlots().get_site_attributes_data()

    assert df.empty
