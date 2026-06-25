import copy

import pytest

from terndata.ecoplots import AsyncEcoPlots, EcoPlots, EcoPlotsError


def test_observations_client_defaults_to_empty_filters():
    ec = EcoPlots()

    assert ec.get_filter() == {}
    assert ec.get_api_query_filters() == {}
    assert len(ec) == 0
    assert not ec


def test_samples_client_adds_persistent_dataset_filter():
    ec = EcoPlots(mode="samples")

    assert ec.get_filter("dataset") == ["TERN Ecosystem Surveillance"]
    assert ec.get_api_query_filters("dataset") == ["http://linked.data.gov.au/dataset/ausplots"]
    assert len(ec) == 1
    assert ec


def test_samples_mode_can_be_first_positional_argument():
    ec = EcoPlots("samples")
    async_ec = AsyncEcoPlots("samples")

    assert ec.get_filter("dataset") == ["TERN Ecosystem Surveillance"]
    assert async_ec.get_filter("dataset") == ["TERN Ecosystem Surveillance"]


def test_mode_is_case_insensitive_and_fuzzy():
    ec = EcoPlots("SAMPLE")
    typo_ec = EcoPlots("smaples")
    async_ec = AsyncEcoPlots("observations mode")

    assert ec.get_filter("dataset") == ["TERN Ecosystem Surveillance"]
    assert typo_ec.get_filter("dataset") == ["TERN Ecosystem Surveillance"]
    assert async_ec.get_filter() == {}


def test_invalid_mode_raises_clear_error():
    with pytest.raises(EcoPlotsError, match="Invalid mode 'xyz'"):
        EcoPlots("xyz")


def test_init_prints_mode_message(capsys):
    EcoPlots("samples")
    sample_output = capsys.readouterr().out

    EcoPlots()
    observations_output = capsys.readouterr().out

    assert "initialized in samples mode" in sample_output
    assert "TERN Ecosystem Surveillance dataset filter is applied automatically" in sample_output
    assert "initialized in observations mode" in observations_output


def test_legacy_positional_constructor_order_still_works():
    ec = EcoPlots(
        {"site_id": ["TCFTNS0002"]},
        {"site_id": ["https://example.test/site/TCFTNS0002"]},
        "observations",
    )

    assert ec.get_filter("site_id") == ["TCFTNS0002"]
    assert ec.get_api_query_filters("site_id") == ["https://example.test/site/TCFTNS0002"]


def test_select_remove_and_clear_observation_filters(no_filter_validation):
    ec = EcoPlots()

    returned = ec.select(site_id="TCFTNS0002").select(feature_type=["soil profile"])

    assert returned is ec
    assert ec.get_filter("site_id") == ["TCFTNS0002"]
    assert ec.get_filter("feature_type") == ["soil profile"]

    ec.remove(site_id="TCFTNS0002")

    assert ec.get_filter("site_id") is None
    assert ec.get_filter("feature_type") == ["soil profile"]

    ec.clear()

    assert ec.get_filter() == {}
    assert ec.get_api_query_filters() == {}


def test_samples_clear_preserves_persistent_dataset(no_filter_validation):
    ec = EcoPlots(mode="samples")
    ec.select(has_image=True, soil_subsite_id=[1, "2"])

    ec.clear()

    assert ec.get_filter() == {"dataset": ["TERN Ecosystem Surveillance"]}
    assert ec.get_api_query_filters() == {"dataset": ["http://linked.data.gov.au/dataset/ausplots"]}


def test_region_requires_region_type(no_filter_validation):
    ec = EcoPlots()

    with pytest.raises(EcoPlotsError, match="region_type"):
        ec.select(region="Queensland")


def test_invalid_filter_key_raises(no_filter_validation):
    ec = EcoPlots()

    with pytest.raises(EcoPlotsError, match="Invalid filter keys"):
        ec.select(not_a_filter="value")


def test_samples_rejects_observations_only_has_image_filter(no_filter_validation):
    ec = EcoPlots()

    with pytest.raises(EcoPlotsError, match="has_image"):
        ec.select(has_image=True)


def test_samples_validates_special_filter_shapes(no_filter_validation):
    ec = EcoPlots(mode="samples")

    ec.select(has_image=True, soil_subsite_id=["1", 2], soil_depth_range={"min": 0, "max": 0.3})

    assert ec.get_filter("has_image") is True
    assert ec.get_api_query_filters("has_image") is True
    assert ec.get_filter("soil_subsite_id") == [1, 2]
    assert ec.get_filter("soil_depth_range") == [0.0, 0.3]


def test_samples_protects_persistent_dataset():
    ec = EcoPlots(mode="samples")

    with pytest.raises(EcoPlotsError, match="Cannot remove 'dataset'"):
        ec.remove(dataset=None)


def test_copy_and_deepcopy_preserve_filter_state(no_filter_validation):
    ec = EcoPlots().select(site_id=["A", "B"])

    shallow = copy.copy(ec)
    deep = copy.deepcopy(ec)

    assert shallow == ec
    assert deep == ec
    assert shallow is not ec
    assert deep is not ec
