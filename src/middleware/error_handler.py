import warnings
from src.utils.loader import worker_emulator


def handle_error(func):
    def wrapper(*args, **kwargs):
        try:
            # Filter out the NotOpenSSLWarning from urllib3
            warnings.filterwarnings("ignore", category=DeprecationWarning, module="urllib3")

            return func(*args, **kwargs)
        except Exception as e:
            print('An error occurred: ❌ ', e)
            worker_emulator('Failed', False)
            return None
    return wrapper
