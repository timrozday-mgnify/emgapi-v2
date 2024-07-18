import tempfile

from .settings import *

# Use an in-memory database for tests
DATABASES = {
    "default": {
        "ENGINE": "django.db.backends.sqlite3",
        "NAME": ":memory:",
    }
}

EMG_CONFIG.slurm.default_workdir = tempfile.gettempdir()
