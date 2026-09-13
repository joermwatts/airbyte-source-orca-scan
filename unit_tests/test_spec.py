import logging

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
