import tempfile

from .settings import *

EMG_CONFIG.slurm.default_workdir = tempfile.gettempdir()
EMG_CONFIG.webin.emg_webin_account = "webin-fake"
EMG_CONFIG.webin.emg_webin_password = "not-a-pw"

STORAGES = {
    "staticfiles": {
        "BACKEND": "whitenoise.storage.CompressedStaticFilesStorage",
    },
}
