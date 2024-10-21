import tempfile

from .settings import *

EMG_CONFIG.slurm.default_workdir = tempfile.gettempdir()

STORAGES = {
    "staticfiles": {
        "BACKEND": "whitenoise.storage.CompressedStaticFilesStorage",
    },
}
