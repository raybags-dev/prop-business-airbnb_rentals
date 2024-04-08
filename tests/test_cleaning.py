import os
import pytest
import unittest
import pandas as pd
from pathlib import Path
from src.data_pipeline import cleaning
from src.data_pipeline.cleaning import clean_rentals_data
from src.data_pipeline.cleaning  import preprocess_data


# <----------- UNIT TESTS ---------->
def test_data_cleaned_folder_exists():
    folder_path = Path('src/data/data_cleaned')
    assert folder_path.exists(), f"Folder '{folder_path}' does not exist."


def test_files_start_with_cleaned():
    folder_path = Path('src/data/data_cleaned')
    files = [file.name for file in folder_path.iterdir()]
    for file in files:
        assert file.startswith('cleaned_'), f"File '{file}' does not start with 'cleaned_'."


@pytest.fixture
def sample_rentals_data():
    """Sample rentals data for testing"""
    data = {
        'rent': ['1000 EUR', '1200 EUR', '1500 EUR', ''],
        'deposit': ['2000 EUR', '2200 EUR', '2500 EUR', ''],
        'additionalCostsRaw': ['100 EUR', '', '200 EUR', '']
    }
    return pd.DataFrame(data)


def test_clean_rentals_data_no_error(sample_rentals_data):
    """Test clean_rentals_data function for no error"""
    try:
        cleaned_data = clean_rentals_data(sample_rentals_data)
    except Exception as e:
        pytest.fail(f"Error occurred during cleaning: {str(e)}")


def test_preprocess_data():
    """Test preprocess_data function"""
    # Sample data with special characters and formatting issues
    sample_data = {
        'column1': ['This is a string\\u2019', 'Another string with \u20ac ', 'More text //'],
        'column2': ['Some text with slashes /', 'One more string with \u20ac', 'More text \u20ac ']
    }
    df = pd.DataFrame(sample_data)

    # Preprocess the data
    processed_data = preprocess_data(df)
    print(processed_data)

    # Check if all columns are string type after preprocessing
    assert all(processed_data[col].dtype == 'O' for col in processed_data.columns), ("Columns not converted to string "
                                                                                     "type")

    # Check if special characters are properly replaced
    assert all("'" not in value for value in processed_data['column1']), "Single quotes not replaced"
    # assert all('€' in value for value in processed_data['column1']), "Euro symbol not replaced"
    # assert all(',' in value for value in processed_data['column1']), "Forward slashes not replaced"
    # assert all(',' in value for value in processed_data['column2']), "Forward slashes not replaced"
    # assert all('€' in value for value in processed_data['column2']), "Euro symbol not replaced"

    print("Preprocessing tests passed successfully.")


#
# @pytest.fixture
# def airbnb_data() -> pd.DataFrame:
#     pass
#
#
# @pytest.fixture
# def rentals_file_path(tmp_path: Path) -> Path:
#     file_name = "rentals.json"
#     file_path = tmp_path / file_name
#     return file_path
#
#
# @pytest.fixture
# def airbnb_file_path(tmp_path: Path) -> Path:
#     file_name = "airbnb.csv"
#     file_path = tmp_path / file_name
#     return file_path
#
#
# @pytest.fixture
# def test_clean_rentals_data(rentals_data: pd.DataFrame, rentals_file_path: Path) -> None:
#     pass
#
#
# @pytest.fixture
# def test_clean_airbnb_data(airbnb_data: pd.DataFrame, airbnb_file_path: Path) -> None:
#     pass
