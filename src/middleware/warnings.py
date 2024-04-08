import warnings


def suppress_warnings(func):
    """Decorator to ignore warnings during the execution of a function."""
    def wrapper(*args, **kwargs):
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            return func(*args, **kwargs)
    return wrapper
