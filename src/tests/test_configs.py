import json
import pytest
from pathlib import Path

# Load configs from the file
with open('configs/configs.json', 'r') as file:
    configs = json.load(file)


# <----------- CONFIGURATION UNIT TESTS ---------->
def test_data_cleaned_folder_exists():
    folder_path = Path('src/data/data_cleaned')
    assert folder_path.exists(), f"Folder '{folder_path}' does not exist."


def test_depth_exists():
    assert 'depth' in configs, "Field 'depth' is missing."
    assert isinstance(configs['depth'], str), "Field 'depth' should be a string."


def test_source_file_csv_exists():
    assert 'source_file_csv' in configs, "Field 'source_file_csv' is missing."


def test_source_file_json_exists():
    assert 'source_file_json' in configs, "Field 'source_file_json' is missing."


def test_clear_geo_cache_exists():
    assert 'clear_geo_cache' in configs, "Field 'clear_geo_cache' is missing."
    assert isinstance(configs['clear_geo_cache'], bool), "Field 'clear_geo_cache' should be a boolean."


def test_upload_to_dbfs_exists():
    assert 'upload_to_dbfs' in configs, "Field 'upload_to_dbfs' is missing."
    assert isinstance(configs['upload_to_dbfs'], bool), "Field 'upload_to_dbfs' should be a boolean."


def test_is_txt_output_exists():
    assert 'is_txt_output' in configs, "Field 'is_txt_output' is missing."
    assert isinstance(configs['is_txt_output'], bool), "Field 'is_txt_output' should be a boolean."


def test_is_json_output_exists():
    assert 'is_json_output' in configs, "Field 'is_json_output' is missing."
    assert isinstance(configs['is_json_output'], bool), "Field 'is_json_output' should be a boolean."


def test_unnecessary_columns_to_dropped_exists():
    assert 'unnecessary_columns_to_dropped' in configs, "Field 'unnecessary_columns_to_dropped' is missing."


def test_columns_to_drop_due_nan_exists():
    assert 'columns_to_drop_due_nan' in configs, "Field 'columns_to_drop_due_nan' is missing."
