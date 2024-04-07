from src.utils.loader import worker_emulator


def handle_error(func):
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except Exception as e:
            print('An error occurred: ❌❌ ', e)
            worker_emulator('Failed',False)
            return None
    return wrapper

