from src.utils.handle_writes import data_processor_pipeline
from src.middleware import error_handler
from src.middleware.warnings import suppress_warnings
from ochestrator.ochestrator import load_configs
from src.cloud.databricks import upload_files_to_databricks

configs = load_configs()


@error_handler.handle_error
@suppress_warnings
def main():
    # data processor pipeline
    is_data_ready = data_processor_pipeline()
    upload_to_dbfs = configs['upload_to_dbfs']

    # upload data pipeline
    if is_data_ready and upload_to_dbfs:
        upload_files_to_databricks(is_data_ready)


if __name__ == "__main__":
    main()
