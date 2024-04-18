import warnings
from src.utils.loader import worker_emulator
from logger.logger import initialize_logging, my_log
from colorama import init, Fore, Style

init()
initialize_logging()


def handle_error(func):
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except Exception as e:
            print(f"{Fore.RED}\n*****************\nAn error occurred: {e}\n*****************{Style.RESET_ALL}")
            worker_emulator('Failed', False)
            my_log(f"{e}", 'error')
            return None
    return wrapper
