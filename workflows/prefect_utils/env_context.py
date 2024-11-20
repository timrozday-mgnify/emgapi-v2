import os


class TemporaryEnv:
    """
    Context manager for temporarily assigning env vars.
    Usage example:

    with TemporaryEnv(USERNAME='reinhardt', PASSWORD='guitar'):
        some_library_depending_on_env_vars_in_env()
    """

    def __init__(self, **kwargs):
        self.temporary_env_vars = kwargs

    def __enter__(self):
        self.previous_values = {
            key: os.environ.get(key, "") for key in self.temporary_env_vars.keys()
        }
        os.environ.update(self.temporary_env_vars)

    def __exit__(self, exc_type, exc_value, exc_traceback):
        os.environ.update(self.previous_values)
