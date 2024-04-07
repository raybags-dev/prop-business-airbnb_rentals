import json
import pandas as pd
from typing import Any, List, Dict
from pathlib import Path
from src.middleware import error_handler
from src.utils.loader import worker_emulator
from src.utils.unzipper import handle_unzip


@error_handler.handle_error
def load_rentals_data(file_path: Path) -> List[Dict[str, Any]]:
    worker_emulator('Loading rental data initiated...', True)
    handle_unzip(file_path, '.json')
    """Load data from rentals.json"""
    if not isinstance(file_path, Path):  # Check if file_path is a Path object
        raise TypeError('Invalid filepath input')
    with open(file_path, 'r') as f:
        data = json.load(f)
    worker_emulator('Process Completed: ', False)
    return data


@error_handler.handle_error
def load_airbnb_data(file_path: Path) -> pd.DataFrame:
    """Load data from airbnb.csv"""
    worker_emulator('Loading airbnb data initiated...', True)
    handle_unzip(file_path, '.csv')
    if not isinstance(file_path, Path):
        raise TypeError('Invalid filepath input')
    worker_emulator('Done', False)
    return pd.read_csv(file_path)
