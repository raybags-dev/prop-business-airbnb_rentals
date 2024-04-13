import json
import sys
from pathlib import Path
from logger.logger import initialize_logging, my_log


def load_configs():
    try:
        config_file_path = Path('../configs/configs.json')

        if not config_file_path.exists() or config_file_path.stat().st_size == 0:
            msg = f"> Error: true\n> Source: The configuration file '{config_file_path}'\n> Message: File does not exist Or is empty"
            raise Exception(msg)

        with open(config_file_path, 'r') as file:
            configs = json.load(file)

        if 'depth' not in configs or (configs['depth'] is not None and (not isinstance(configs['depth'], int) or configs['depth'] < 1)):
            msg = "> Error: 'depth' should be a positive integer greater than or equal to 1, or None."
            raise ValueError(msg)

        if 'source_file_csv' not in configs or 'source_file_json' not in configs:
            msg = "> Error: 'source_file_csv' and 'source_file_json' are required."
            raise ValueError(msg)

        fields_to_check = ['clear_geo_cache', 'upload_to_dbfs', 'is_txt_output', 'is_json_output']
        missing_fields = [field for field in fields_to_check if field not in configs]
        if missing_fields:
            for field in missing_fields:
                raise ValueError(f"> Error: Required fields ({field}) are missing in configurations.")

        if not all(isinstance(configs[key], bool) for key in fields_to_check):
            msg = "> Error: 'clear_geo_cache', 'upload_to_dbfs', 'is_txt_output', and 'is_json_output' should be boolean values."
            raise ValueError(msg)

        if not isinstance(configs['unnecessary_columns_to_dropped'], list) or not isinstance(configs['columns_to_drop_due_nan'], list):
            msg = "> Error: 'unnecessary_columns_to_dropped' and 'columns_to_drop_due_nan' should be lists."
            raise ValueError(msg)

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
        # print(f'Error: {e}')
        my_log(f"{e}", 'info')
        sys.exit()
