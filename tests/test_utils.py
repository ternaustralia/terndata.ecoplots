import pandas as pd
import pytest

from terndata.ecoplots import EcoPlotsError
from terndata.ecoplots._utils import (
    _align_and_concat,
    _ensure_ecoproj_path,
    _parse_date,
    _to_geopandas,
    _validate_spatial_input,
)


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        ("2020-05-21", "2020-05-21"),
        ("21/05/2020", "2020-05-21"),
        ("21-05-2020", "2020-05-21"),
        ("21st May 2020", "2020-05-21"),
        ("May 20 2020", "2020-05-20"),
    ],
)
def test_parse_date_normalizes_supported_formats(raw, expected):
    assert _parse_date(raw) == expected


def test_parse_date_rejects_invalid_input():
    with pytest.raises(EcoPlotsError, match="Cannot parse date"):
        _parse_date("not a date")


def test_validate_spatial_input_accepts_supported_shapes():
    assert _validate_spatial_input("POLYGON((0 0, 1 0, 1 1, 0 0))") == "wkt"
    assert _validate_spatial_input({"type": "Point", "coordinates": [153.0, -27.0]}) == "geojson"
    assert _validate_spatial_input([152.0, -28.0, 153.0, -27.0]) == "bbox"


def test_validate_spatial_input_rejects_malformed_shapes():
    with pytest.raises(EcoPlotsError, match="spatial"):
        _validate_spatial_input("POLYGON((0 0, 1 1)")


def test_ensure_ecoproj_path_adds_suffix(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)

    assert _ensure_ecoproj_path("analysis").name == "analysis.ecoproj"
    assert _ensure_ecoproj_path(tmp_path / "nested" / "analysis").suffix == ".ecoproj"


def test_align_and_concat_preserves_column_order_and_fills_missing_values():
    first = pd.DataFrame({"a": [1], "b": [2]})
    second = pd.DataFrame({"b": [3], "c": [4]})

    result = _align_and_concat([first, second])

    assert result.to_dict(orient="records") == [
        {"a": 1, "b": 2, "c": "N/A"},
        {"a": "N/A", "b": 3, "c": 4},
    ]


def test_to_geopandas_builds_points_from_coordinate_columns():
    df = pd.DataFrame({"longitude_Degree": [153.0], "latitude_Degree": [-27.0]})

    gdf = _to_geopandas(df)

    assert gdf.crs == "EPSG:4326"
    assert gdf.geometry.iloc[0].x == 153.0
    assert gdf.geometry.iloc[0].y == -27.0
