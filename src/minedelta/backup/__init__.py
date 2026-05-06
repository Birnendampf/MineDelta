import contextlib

from .base import BackupInfo, BaseBackupManager
from .diff import DiffBackupManager
from .hardlink import HardlinkBackupManager

__all__ = ["BackupInfo", "BaseBackupManager", "DiffBackupManager", "HardlinkBackupManager"]

with contextlib.suppress(ImportError):
    from .git import GitBackupManager

    __all__ += ["GitBackupManager"]

with contextlib.suppress(ImportError):
    from .fast import FastBackupManager

    __all__ += ["FastBackupManager"]
