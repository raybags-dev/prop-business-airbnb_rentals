import os
import json
from pathlib import Path
from typing import Union
from src.data_pipeline import ingestion, cleaning
from src.middleware import error_handler
from src.utils.geoLocator import assign_zipcode
from ochestrator.ochestrator import load_configs

configs = load_configs()
source_file_csv = configs['source_file_csv']
source_file_json = configs['source_file_json']
clear_geo_cache = configs['clear_geo_cache']
is_json_output = configs['is_json_output']
is_txt_output = configs['is_txt_output']


rentalsJSONFilePath = Path(source_file_json)
airbnbCSVFilePath = Path(source_file_csv)


@error_handler.handle_error
def data_processor_pipeline() -> bool:
    depth = configs['depth']
    # Ingest data
    rentals_data = ingestion.load_rentals_data(rentalsJSONFilePath)
    airbnb_data = ingestion.load_airbnb_data(airbnbCSVFilePath)

    # Clean data
    cleaned_rentals_data = cleaning.clean_rentals_data(rentals_data)
    cleaned_airbnb_data = cleaning.clean_airbnb_data(airbnb_data)

    # Convert DataFrame to dictionary
    cleaned_rentals_dict = cleaned_rentals_data.to_dict(orient='records')
    cleaned_airbnb_dict = cleaned_airbnb_data.to_dict(orient='records')

    # Write cleaned data to JSON files
    return writes_executor(cleaned_rentals_dict, cleaned_airbnb_dict, depth)


@error_handler.handle_error
def writes_executor(cleaned_rentals_data, cleaned_airbnb_data, depth: Union[None, int] = None):
    is_completed = True

    if is_json_output == False and is_txt_output == False:
        raise Exception("Error: Invalid configurations filetype out can not be both false")

    if is_json_output == True and is_txt_output == True:
        raise Exception("Error: Invalid configurations filetype out can not be both true")

    # Create a new directory for cleaned data if it doesn't exist
    cleaned_data_dir = Path('data/data_cleaned')
    cleaned_data_dir.mkdir(parents=True, exist_ok=True)

    # File paths for .json or .txt files
    rentals_json_file_path = cleaned_data_dir / 'cleaned_rentals_data.json'
    airbnb_json_file_path = cleaned_data_dir / 'cleaned_airbnb_data.json'
    rentals_txt_file_path = cleaned_data_dir / 'cleaned_rentals_data.txt'
    airbnb_txt_file_path = cleaned_data_dir / 'cleaned_airbnb_data.txt'

    # Process objects up to the specified depth if applicable
    if depth is not None and isinstance(depth, int) and depth > 0:
        cleaned_rentals_data = cleaned_rentals_data[:depth]
        cleaned_airbnb_data = cleaned_airbnb_data[:depth]

        # Enriching airbnb objects data from reverse geo search Google API
        assign_zipcode(cleaned_airbnb_data, clear_geo_cache)

        # Write cleaned rentals or airbnb data to either .txt or .json file
        json_file_writer(rentals_json_file_path, cleaned_rentals_data, airbnb_json_file_path, cleaned_airbnb_data, is_json_output)
        txt_file_writer(rentals_txt_file_path, cleaned_rentals_data, airbnb_txt_file_path, cleaned_airbnb_data, is_txt_output)
    else:
        # Enriching airbnb objects data from reverse geo search Google API
        assign_zipcode(cleaned_airbnb_data, clear_geo_cache)

        # Write cleaned rentals or airbnb data to either .txt or .json file
        json_file_writer(rentals_json_file_path, cleaned_rentals_data, airbnb_json_file_path, cleaned_airbnb_data, is_json_output)
        txt_file_writer(rentals_txt_file_path, cleaned_rentals_data, airbnb_txt_file_path, cleaned_airbnb_data, is_txt_output)

    # Count objects
    count_objects(cleaned_rentals_data, 'Rentals')
    count_objects(cleaned_airbnb_data, 'Airbnb')

    # Handle notifications
    writes_notification_handler(is_completed, cleaned_rentals_data, cleaned_airbnb_data)

    return is_completed


@error_handler.handle_error
def json_file_writer(rentals_json_file_path, cleaned_rentals_data, airbnb_json_file_path, cleaned_airbnb_data, is_json_output):
    if is_json_output:
        # Write cleaned rentals data to JSON file
        with open(rentals_json_file_path, 'w') as json_file:
            json.dump(cleaned_rentals_data, json_file, indent=4)
        print("Cleaned rentals data written to JSON file:", rentals_json_file_path)

        # Write cleaned airbnb data to .json file
        with open(airbnb_json_file_path, 'w') as json_file:
            json.dump(cleaned_airbnb_data, json_file, indent=4)
        print("Cleaned airbnb data written to JSON file:", airbnb_json_file_path)
        # remove .txt files
        delete_files_by_extension(rentals_json_file_path, '.txt')


@error_handler.handle_error
def txt_file_writer(rentals_txt_file_path, cleaned_rentals_data, airbnb_txt_file_path, cleaned_airbnb_data, is_txt_output):
    if is_txt_output:
        # Write cleaned rentals data to .txt file
        with open(rentals_txt_file_path, 'w') as txt_file:
            for item in cleaned_rentals_data:
                txt_file.write(str(item) + '\n')

        # Write cleaned airbnb data to text file
        with open(airbnb_txt_file_path, 'w') as txt_file:
            for item in cleaned_airbnb_data:
                txt_file.write(str(item) + '\n')
        # remove .json files
        delete_files_by_extension(airbnb_txt_file_path, '.json')


def count_objects(object_list, category):
    print(f'{category} data objects written to file -> {len(object_list)}')


def delete_files_by_extension(filepath, extension):
    parent_directory = Path(filepath).parent
    for file_path in parent_directory.iterdir():
        # Check if the file has the specified extension
        if file_path.suffix == extension:
            try:
                file_path.unlink()
                print(f"File '{file_path}' deleted successfully.")
            except OSError as e:
                print(f"Error deleting file '{file_path}': {e}")


# Notifications/clean/writes
@error_handler.handle_error
def writes_notification_handler(is_completed, cleaned_rentals_data, cleaned_airbnb_data) -> dict:
    if is_completed:
        data_dir_path = Path('data/data_cleaned')

        # Count the number of files in the data/data_cleaned directory
        file_count = len([name for name in os.listdir(data_dir_path) if os.path.isfile(data_dir_path / name)])

        # Count objects in cleaned_rentals_data and cleaned_airbnb_data
        rentals_object_count = len(cleaned_rentals_data)
        airbnb_object_count = len(cleaned_airbnb_data)

        message = {
            "status": "completed",
            "isDataCleaned": True,
            "fileCount": f'{file_count}',
            "rentals_object_count": rentals_object_count,
            "airbnb_object_count": airbnb_object_count,
            "message": f"All files processed, data has been stored in {file_count} files in {data_dir_path}"
        }
        formatted_message = json.dumps(message, indent=4)
        print(formatted_message)
        return message
    else:
        message = {
            "status": "incomplete",
            "isDataCleaned": False,
            "fileCount": "Unknown",
            "rentals_object_count": "Unknown",
            "airbnb_object_count": "Unknown",
            "message": "Data processing was not completed successfully."
        }
        formatted_message = json.dumps(message, indent=4)
        print(formatted_message)
        return message
