import json


def load_configs():
    # Load JSON configuration file
    with open('../configs/configs.json', 'r') as file:
        configs = json.load(file)

    # Parse values from the loaded JSON
    depth = int(configs.get('depth', 'None')) if configs.get('depth') != 'None' else None
    source_file_csv = configs.get('source_file_csv')
    source_file_json = configs.get('source_file_json')
    clear_geo_cache = configs.get('clear_geo_cache', False)
    upload_to_dbfs = configs.get('upload_to_dbfs', True)
    unnecessary_columns_to_dropped = configs.get('unnecessary_columns_to_dropped', [])
    columns_to_drop_due_nan = configs.get('columns_to_drop_due_nan', [])

    return {
        'depth': depth,
        'source_file_csv': source_file_csv,
        'source_file_json': source_file_json,
        'clear_geo_cache': clear_geo_cache,
        'upload_to_dbfs': upload_to_dbfs,
        'unnecessary_columns_to_dropped': unnecessary_columns_to_dropped,
        'columns_to_drop_due_nan': columns_to_drop_due_nan
    }


# unnecessary_columns_to_dropped = configs['unnecessary_columns_to_dropped']
# columns_to_drop_due_nan = configs['columns_to_drop_due_nan']
