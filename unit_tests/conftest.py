import pytest


@pytest.fixture
def config() -> dict:
    """A syntactically valid but fake connector config. Never a real key."""
    return {"api_key": "orca_test_key_not_real"}
