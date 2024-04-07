import subprocess
from pathlib import Path
from colorama import init
from src.middleware import error_handler
from src.utils.loader import worker_emulator

init()


@error_handler.handle_error
def upload_files_to_databricks(is_data_ready: bool) -> None:
    if not is_data_ready:
        print("> Data is not ready. Skipping upload to Databricks.")
        return

    source_directory = Path('./data/data_cleaned/')
    destination_directory = Path('/adb-2165640261815898.18.azuredatabricks.net/browse/folders/3694927554019636?o'
                                 '=2165640261815898')

    # List all files in the source directory
    files = source_directory.glob('*')

    # Delete existing files in the destination directory
    delete_command = f"databricks fs rm -r dbfs:{destination_directory}"
    try:
        worker_emulator('> Deleting old files...', True)
        subprocess.run(delete_command, shell=True, check=True, stderr=subprocess.PIPE)
        print(f"\n> Deletion of files in '{destination_directory}' successful.")
        worker_emulator('Old files deleted successfully ☑️ ', False)
    except subprocess.CalledProcessError as e:
        print(f"Error deleting existing files: {e}")

    # Upload each file to Databricks workspace
    for file in files:
        if file.is_file():
            dest_file = destination_directory / file.name

            upload_command = f"databricks fs cp {file} dbfs:{dest_file}"
            try:
                worker_emulator(f'> Uploading file {file}...', True)
                subprocess.run(upload_command, shell=True, check=True, stderr=subprocess.PIPE)
                worker_emulator(f'> File {file} uploaded.', False)
            except subprocess.CalledProcessError as e:
                print(f"> Error uploading file '{file}': {e}")