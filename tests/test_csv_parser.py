import pytest
from aggregator_utils import parse_csv_data

# 1. Test basic, well-formed CSV parsing
def test_parse_csv_data_success():
    csv_content = (
        "switch_id,metric_1,metric_2\\n"
        "SW-01,10.5,UP\\n"
        "SW-02,99.9,DOWN"
    )
    expected = {
        "SW-01": {"metric_1": "10.5", "metric_2": "UP"},
        "SW-02": {"metric_1": "99.9", "metric_2": "DOWN"},
    }
    assert parse_csv_data(csv_content) == expected

# 2. Test handling of corrupt/incomplete CSV data
def test_parse_csv_corrupt_data():
    # CSV is missing a value in the second data row
    csv_content = (
        "switch_id,metric_1,metric_2\\n"
        "SW-01,10.5,UP\\n"
        "SW-02,99.9"
    )
    with pytest.raises(ValueError, match="Row has an incorrect number of fields"):
        parse_csv_data(csv_content)

# 3. Test handling of empty input
def test_parse_csv_empty_input():
    assert parse_csv_data("") == {}

# 4. Test handling of CSV with only a header
def test_parse_csv_header_only():
    csv_content = "switch_id,metric_1,metric_2"
    assert parse_csv_data(csv_content) == {}
