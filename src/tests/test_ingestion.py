import pytest
from unittest.mock import mock_open, patch
from src.data_pipeline.ingestion import load_rentals_data, load_airbnb_data, map_ids
import json
import pandas as pd


@pytest.fixture
def rentals_data_file(tmp_path):
    file_path = tmp_path / "rentals.json"
    return file_path


@pytest.fixture
def airbnb_data_file(tmp_path):
    file_path = tmp_path / "airbnb.csv"
    return file_path


def test_load_rentals_data(rentals_data_file):
    mock_data = [
        {"_id": ["12345"], "crawledAt": ["2022-04-11T12:34:56.789Z"]},
        {"_id": ["67890"], "crawledAt": ["2022-04-12T13:45:57.890Z"]}
    ]
    # Create a mock file object that returns the mock data
    mock_file = mock_open(read_data=json.dumps(mock_data))

    with patch("src.data_pipeline.ingestion.open", mock_file):
        data = load_rentals_data(rentals_data_file)

    assert isinstance(data, list)
    assert len(data) == 2
    assert all(isinstance(item["_id"], str) for item in data)
    assert all(isinstance(item["crawledAt"], str) for item in data)


def test_load_airbnb_data(airbnb_data_file):
    mock_data = pd.DataFrame({
        "_id": ["12345", "67890"],
        "crawledAt": ["2022-04-11T12:34:56.789Z", "2022-04-12T13:45:57.890Z"]
    })
    with patch("src.data_pipeline.ingestion.pd.read_csv", return_value=mock_data):
        data = load_airbnb_data(airbnb_data_file)

    assert isinstance(data, pd.DataFrame)
    assert data.shape == (2, 2)
    assert all(isinstance(value, str) for value in data["_id"])
    assert all(isinstance(value, str) for value in data["crawledAt"])


def test_map_ids():
    data = [
        {"_id": ["12345"], "crawledAt": ["2022-04-11T12:34:56.789Z"]},
        {"_id": ["67890"], "crawledAt": ["2022-04-12T13:45:57.890Z"]}
    ]
    mapped_data = map_ids(data)

    assert isinstance(mapped_data, list)
    assert len(mapped_data) == 2
    assert all(isinstance(item["_id"], str) for item in mapped_data)
    assert all(isinstance(item["crawledAt"], str) for item in mapped_data)
