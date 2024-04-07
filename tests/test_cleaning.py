import pandas as pd
import pytest
from pathlib import Path
from typing import Any, List, Dict
from src.data_pipeline import cleaning


@pytest.fixture
def rentals_data() -> pd.DataFrame:
    pass


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
