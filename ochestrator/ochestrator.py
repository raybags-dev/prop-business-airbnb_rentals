import json
import sys
from pathlib import Path


def load_configs():
    try:
        config_file_path = Path('../configs/configs.json')

        # Check if the file exists and is not empty
        if not config_file_path.exists() or config_file_path.stat().st_size == 0:
            raise Exception(f"> Error: true\n> Source: The configuration file '{config_file_path}'\n> Message: File does "
                            f"not exist Or is empty.")

        # Load JSON configuration file
        with open(config_file_path, 'r') as file:
            configs = json.load(file)

        # Parse values from the loaded JSON
        depth = int(configs.get('depth', 'None')) if configs.get('depth') != 'None' else None
        source_file_csv = configs.get('source_file_csv')
        source_file_json = configs.get('source_file_json')
        clear_geo_cache = configs.get('clear_geo_cache', False)
        upload_to_dbfs = configs.get('upload_to_dbfs', True)
        is_txt_output = configs.get('is_txt_output', False)
        is_json_output = configs.get('is_json_output', False)
        unnecessary_columns_to_dropped = configs.get('unnecessary_columns_to_dropped', [])
        columns_to_drop_due_nan = configs.get('columns_to_drop_due_nan', [])

        return {
            'depth': depth,
            'source_file_csv': source_file_csv,
            'source_file_json': source_file_json,
            'clear_geo_cache': clear_geo_cache,
            'upload_to_dbfs': upload_to_dbfs,
            'unnecessary_columns_to_dropped': unnecessary_columns_to_dropped,
            'columns_to_drop_due_nan': columns_to_drop_due_nan,
            "is_txt_output": is_txt_output,
            "is_json_output": is_json_output
        }

    except Exception as e:
        print(f'{e}')
        sys.exit()
