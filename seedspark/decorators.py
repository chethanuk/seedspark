# Decorator for automatic pre-start and post-stop
import functools

from loguru import logger as log


def spark_session_management(func):
    @functools.wraps(func)
    def wrapper(self, *args, **kwargs):
        self.pre_start()
        try:
            return func(self, *args, **kwargs)
        finally:
            self.post_stop()

    return wrapper


# Decorator for logging and exception handling
def log_exceptions(func):
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        try:
            log.debug(f"Executing {func.__name__}")
            return func(*args, **kwargs)
        except Exception as e:
            log.error(f"Exception in {func.__name__}: {e}")
            raise

    return wrapper


def connection_check(func):
    @functools.wraps(func)
    def wrapper(self, *args, **kwargs):
        # Check if the connection to ClickHouse is established
        if not self._check_connection():
            raise Exception("ClickHouse connection not established.")
        return func(self, *args, **kwargs)

    return wrapper
