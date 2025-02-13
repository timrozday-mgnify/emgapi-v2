import tempfile

from .settings import *

EMG_CONFIG.slurm.default_workdir = tempfile.gettempdir()
EMG_CONFIG.webin.emg_webin_account = "webin-fake"
EMG_CONFIG.webin.emg_webin_password = "not-a-pw"
EMG_CONFIG.webin.dcc_account = "dcc_fake"
EMG_CONFIG.webin.dcc_password = "not-a-dcc-pw"

STORAGES = {
    "staticfiles": {
        "BACKEND": "whitenoise.storage.CompressedStaticFilesStorage",
    },
}
