import logging
import re

import pytest

from source_orca_scan import SourceOrcaScan


def test_spec_loads_and_marks_api_key_secret():
    spec = SourceOrcaScan().spec(logging.getLogger("airbyte"))
    schema = spec.connectionSpecification

    assert "api_key" in schema["required"]
    assert schema["properties"]["api_key"]["airbyte_secret"] is True
    assert schema["properties"]["api_key"]["type"] == "string"


def test_spec_start_date_is_optional():
    spec = SourceOrcaScan().spec(logging.getLogger("airbyte"))
    schema = spec.connectionSpecification

    assert "start_date" in schema["properties"]
    assert "start_date" not in schema["required"]


@pytest.mark.parametrize(
    "value, ok",
    [
        ("2024-01-01T00:00:00Z", True),  # what the Airbyte UI datepicker writes
        ("2024-01-01T00:00:00", True),  # typed by hand
        ("2024-01-01", True),
        ("2024-01-01T00:00", False),
        ("01/01/2024", False),
        ("2024-01-01T00:00:00+01:00", False),
    ],
)
def test_spec_start_date_pattern_accepts_the_datepicker_form(value, ok):
    schema = SourceOrcaScan().spec(logging.getLogger("airbyte")).connectionSpecification
    pattern = schema["properties"]["start_date"]["pattern"]
    assert bool(re.match(pattern, value)) is ok
