import tempfile

from .settings import *

EMG_CONFIG.slurm.default_workdir = tempfile.gettempdir()
