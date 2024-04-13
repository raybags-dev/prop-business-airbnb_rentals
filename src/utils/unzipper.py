import os
import zipfile
from pathlib import Path
from src.middleware import error_handler
from src.utils.loader import worker_emulator
from logger.logger import initialize_logging, my_log


@error_handler.handle_error
def handle_unzip(file_path: Path, desired_extension: str) -> None:
    folder_path = file_path.parent
    file_name = file_path.stem
    target_file_name = file_name + desired_extension

    # Check if a .zip file with the same name as the filename exists
    zip_file_path = folder_path / (file_name + '.zip')
    if not zip_file_path.exists():
        print(f"No corresponding .zip file found for {file_path.name}.")

    # Check if the desired extension already exists for the file
    target_path = folder_path / target_file_name
    if target_path.exists():
        print(f"{target_file_name} reachable")
    else:
        worker_emulator(f'Unzipping {zip_file_path.name} in progress...', True)
        with zipfile.ZipFile(zip_file_path, 'r') as zip_ref:
            for file_info in zip_ref.infolist():
                target_path = folder_path / Path(file_info.filename).name
                if target_path.exists():
                    print(f"File transformation already in desired state: {Path(file_info.filename).name}")
                else:
                    zip_ref.extract(file_info, folder_path)
                    os.rename(os.path.join(folder_path, file_info.filename), target_path)
                    print(f"File {zip_file_path.name} unzipped and renamed to {Path(file_info.filename).name}")
        worker_emulator(f'File: < {zip_file_path.name} > unzipped!', False)

