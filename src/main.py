from src.utils.handle_writes import data_processor_pipeline
from src.middleware import error_handler
from src.cloud.databricks import upload_files_to_databricks


@error_handler.handle_error
def main():
    # data processor pipeline
    is_data_ready = data_processor_pipeline()
    # is_data_ready = True  # should be uncommented for testing

    # upload data pipeline
    if is_data_ready:
        upload_files_to_databricks(is_data_ready)


if __name__ == "__main__":
    main()
