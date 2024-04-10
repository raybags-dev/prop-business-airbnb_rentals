import json
import unittest
from pathlib import Path


# <----------- UNIT TESTS ---------->
def test_data_cleaned_folder_exists():
    folder_path = Path('data/data_cleaned')
    assert folder_path.exists(), f"Folder '{folder_path}' does not exist."


def test_files_start_with_cleaned():
    folder_path = Path('data/data_cleaned')
    files = [file.name for file in folder_path.iterdir()]
    for file in files:
        assert file.startswith('cleaned_'), f"File '{file}' does not start with 'cleaned_'."


class TestSourceFileCsvFormat(unittest.TestCase):
    def test_source_file_csv_format(self):
        with open('../configs/configs.json', 'r') as file:
            configs = json.load(file)

        expected_depth = 2
        expected_source_file_csv = "../data/airbnb.csv"
        expected_source_file_json = "../data/rentals.json"
        expected_clear_geo_cache = False
        expected_upload_to_dbfs = False
        expected_unnecessary_columns_to_dropped = ["crawlStatus", "firstSeenAt", "detailsCrawledAt", "lastSeenAt", "pageTitle"]
        expected_columns_to_drop_due_nan = ["rent", "deposit", "additionalCostsRaw"]

        # Compare each field of the loaded configs dictionary to its expected value
        self.assertEqual(configs.get('depth'), expected_depth)
        self.assertEqual(configs.get('source_file_csv'), expected_source_file_csv)
        self.assertEqual(configs.get('source_file_json'), expected_source_file_json)
        self.assertEqual(configs.get('clear_geo_cache'), expected_clear_geo_cache)
        self.assertEqual(configs.get('upload_to_dbfs'), expected_upload_to_dbfs)
        self.assertEqual(configs.get('unnecessary_columns_to_dropped'), expected_unnecessary_columns_to_dropped)
        self.assertEqual(configs.get('columns_to_drop_due_nan'), expected_columns_to_drop_due_nan)
