import pytest

from terndata.ecoplots._base import EcoPlotsBase


@pytest.fixture
def no_filter_validation(monkeypatch):
    """Disable API/cache-backed filter validation for local state tests."""

    monkeypatch.setattr(EcoPlotsBase, "_validate_filters", lambda self: True)
