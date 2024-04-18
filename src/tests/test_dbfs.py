import unittest
from unittest.mock import patch, MagicMock, call
import subprocess
from unittest.mock import ANY
from src.cloud.databricks import upload_files_to_databricks


class TestUploadFilesToDatabricks(unittest.TestCase):
    @patch('src.utils.loader.worker_emulator')
    @patch('subprocess.run')
    @patch('os.environ.get')
    @patch('pathlib.Path.glob')
    def test_environment_and_glob_calls(self, mock_glob, mock_getenv, mock_run, mock_worker_emulator):
        # Mock the environment variable
        mock_getenv.return_value = "test_workspace"
        # Mock the list of files
        mock_files = [
            MagicMock(name='file1', is_file=MagicMock(return_value=True)),
            MagicMock(name='file2', is_file=MagicMock(return_value=True))
        ]
        # Set the return value for the mock glob method
        mock_glob.return_value = mock_files

        # Call the function
        upload_files_to_databricks(True)

        # Assertions
        mock_getenv.assert_called_once_with("DATABRICKS_WORKSPACE")
        mock_glob.assert_called_once_with('*')

    @patch('subprocess.run')
    @patch('os.environ.get', return_value='test_workspace')
    def test_delete_command(self, mock_env, mock_run):
        # Call the function
        upload_files_to_databricks(True)
        # Assert that mock_run was called with the expected command
        mock_run.assert_called_once_with(ANY, shell=True, check=True, stderr=subprocess.PIPE)
        # Check if the expected command is a substring of the actual command
        actual_command = mock_run.call_args[0][0]  # Get the actual command from the mock call
        assert 'databricks fs rm -r dbfs:/test_workspace' in actual_command, f"Unexpected command: {actual_command}"

    @patch('subprocess.run')
    @patch('os.environ.get', return_value='test_workspace')
    def test_skip_upload_when_data_not_ready(self, mock_env, mock_run):
        # Call the function with is_data_ready=False
        upload_files_to_databricks(False)

        # Assertions
        mock_run.assert_not_called()  # subprocess.run should not be called
        mock_run.reset_mock()  # Reset mock to ensure other tests are not affected

    @patch('subprocess.run')
    @patch('os.environ.get', return_value='test_workspace')
    @patch('pathlib.Path.glob', side_effect=OSError("Source directory does not exist"))
    def test_nonexistent_source_directory(self, mock_glob, mock_env, mock_run):
        upload_files_to_databricks(True)
        # Assertions
        mock_run.assert_not_called()  # subprocess.run should not be called
        mock_run.reset_mock()  # Reset mock

    @patch('subprocess.run', side_effect=subprocess.CalledProcessError(1, "cmd"))
    @patch('os.environ.get', return_value='test_workspace')
    def test_handle_errors_during_deletion(self, mock_env, mock_run):
        upload_files_to_databricks(True)
        # Assertions
        mock_run.assert_called_with(ANY, shell=True, check=True, stderr=subprocess.PIPE)
