import os
from src.utils.handle_writes import data_processor_pipeline
from src.middleware import error_handler
from ochestrator.ochestrator import load_configs
from src.cloud.databricks import upload_files_to_databricks
from logger.logger import initialize_logging, my_log
from dotenv import load_dotenv

load_dotenv()
initialize_logging()
configs = load_configs()


@error_handler.handle_error
def main():
    # data processor pipeline
    is_data_ready = data_processor_pipeline()
    upload_to_dbfs = configs['upload_to_dbfs']

    if is_data_ready:
        my_log('Pipeline run completed. Data successfully written on respective files. ', 'info')

    # upload data pipeline
    if is_data_ready and upload_to_dbfs:
        dbfs_workspace = os.environ.get("DATABRICKS_WORKSPACE")
        my_log(f'Uploading files to {dbfs_workspace}...', 'info')
        upload_files_to_databricks(is_data_ready)
        my_log(f'Upload completed successfully ✔️', 'info')


if __name__ == "__main__":
    main()
