import os
import json
from pathlib import Path
from src.data_pipeline import ingestion, cleaning
from src.utils.loader import worker_emulator
from src.middleware import error_handler
from src.utils.geoLocator import assign_zipcode


# Define file paths using Path objects
rentalsJSONFilePath = Path('../data/rentals.json')
airbnbCSVFilePath = Path('../data/airbnb.csv')


@error_handler.handle_error
def data_processor_pipeline() -> bool:
    # Ingest data
    rentals_data = ingestion.load_rentals_data(rentalsJSONFilePath)
    airbnb_data = ingestion.load_airbnb_data(airbnbCSVFilePath)

    # Clean data
    cleaned_rentals_data = cleaning.clean_rentals_data(rentals_data)
    cleaned_airbnb_data = cleaning.clean_airbnb_data(airbnb_data)

    # Convert DataFrame to dictionary
    cleaned_rentals_dict = cleaned_rentals_data.to_dict(orient='records')
    cleaned_airbnb_dict = cleaned_airbnb_data.to_dict(orient='records')

    # fetch missing zipcodes
    cleaned_airbnb_dict = assign_zipcode(cleaned_airbnb_dict)

    # Write cleaned data to JSON files
    return writes_executor(cleaned_rentals_dict, cleaned_airbnb_dict)


@error_handler.handle_error
def writes_executor(cleaned_rentals_data, cleaned_airbnb_data, depth=None):
    is_completed = True

    # Create a new directory for cleaned data if it doesn't exist
    cleaned_data_dir = Path('data/data_cleaned')
    cleaned_data_dir.mkdir(parents=True, exist_ok=True)

    # Define file paths for JSON files
    # rentals_json_file_path = cleaned_data_dir / 'cleaned_rentals_data.json'
    # airbnb_json_file_path = cleaned_data_dir / 'cleaned_airbnb_data.json'

    # Define file paths for text files
    rentals_txt_file_path = cleaned_data_dir / 'cleaned_rentals_data.txt'
    airbnb_txt_file_path = cleaned_data_dir / 'cleaned_airbnb_data.txt'

    worker_emulator('Writing data to file...', True)

    # Apply depth control
    if depth is not None and isinstance(depth, int) and depth > 0:
        cleaned_rentals_data = cleaned_rentals_data[:depth]
        cleaned_airbnb_data = cleaned_airbnb_data[:depth]

    # Write cleaned rentals data to JSON file
    # with open(rentals_json_file_path, 'w') as json_file:
    #     json.dump(cleaned_rentals_data, json_file, indent=4)
    #     print("Cleaned rentals data written to JSON file:", rentals_json_file_path)
    #
    # # Write cleaned airbnb data to JSON file
    # with open(airbnb_json_file_path, 'w') as json_file:
    #     json.dump(cleaned_airbnb_data, json_file, indent=4)
    #     print("Cleaned airbnb data written to JSON file:", airbnb_json_file_path)

    # Write cleaned rentals data to text file
    with open(rentals_txt_file_path, 'w') as txt_file:
        for item in cleaned_rentals_data:
            txt_file.write(str(item) + '\n')

    # Write cleaned airbnb data to text file
    with open(airbnb_txt_file_path, 'w') as txt_file:
        for item in cleaned_airbnb_data:
            txt_file.write(str(item) + '\n')

    # Count objects written to files
    count_objects(cleaned_rentals_data, 'Rentals')
    count_objects(cleaned_airbnb_data, 'Airbnb')

    writes_notification_handler(is_completed, cleaned_rentals_data, cleaned_airbnb_data)
    worker_emulator('Done', False)

    return is_completed


def count_objects(object_list, category):
    print(f'{category} data objects written to file -> {len(object_list)}')


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

