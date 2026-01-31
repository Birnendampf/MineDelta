"""Contains the `BaseBackupManager` and `BackupInfo` classes.

The `BaseBackupManager` is provided only to allow custom backup managers to be derived from it.
It is not intended to be used directly.
"""

import abc
import datetime
import os
import shutil
from collections.abc import Callable
from pathlib import Path
from typing import TYPE_CHECKING, Annotated, Any, ClassVar, Final, Generic, Literal, TypeVar

import msgspec

if TYPE_CHECKING:
    from _typeshed import StrPath

__all__ = ["BaseBackupManager", "BackupInfo", "BACKUP_IGNORE", "BACKUP_IGNORE_FROZENSET"]

_id_T = TypeVar("_id_T", str, int)


def _noop(_: Any) -> None: ...


BACKUP_IGNORE: Final = ["datapacks", "session.lock", "DistantHorizons.sqlite", "icon.png"]
BACKUP_IGNORE_FROZENSET: Final = frozenset(BACKUP_IGNORE)


class BackupInfo(msgspec.Struct):
    """Information about a backup.

    Attributes:
        timestamp: when the backup was created. Timezone aware.
        id: Unique identifier of the backup.
        desc: Additional information about the backup. May be None as not all backup methods support
          storing such information.
    """

    timestamp: Annotated[datetime.datetime, msgspec.Meta(tz=True)]
    id: str
    desc: str | None = None


class BaseBackupManager(Generic[_id_T], metaclass=abc.ABCMeta):
    """Base class for backup managers. Do not initialize this class directly.

    Managers are Generic over the type of index they accept (either str or int).
    """

    __slots__ = "_backup_dir", "_world"
    index_by: ClassVar[Literal["idx", "id"]]

    def __init__(self, save: "StrPath", backup_dir: Path):
        """Create a new backup manager.

        Args:
            save: world to create backups for
            backup_dir: where to store the backups
        """
        self._world = save
        self._backup_dir = backup_dir

    def prepare(self) -> None:
        """Prepare the manager for creating the backups.

        This method is idempotent.
        """
        self._backup_dir.mkdir(parents=True, exist_ok=True)

    @abc.abstractmethod
    def create_backup(
        self, description: str | None = None, progress: Callable[[str], None] = _noop
    ) -> BackupInfo:
        """Create a backup.

        Args:
            description: Description of the backup. Not every backend supports storing descriptions
            progress: Will be called with a string describing the progress of the backup creation
        Returns:
            A new `BackupInfo` reflecting the backup created
        """

    @abc.abstractmethod
    def restore_backup(self, id_: _id_T, progress: Callable[[str], None] = _noop) -> None:
        """Restore a backup.

        Args:
            id_: Identifier of the backup to restore. What parameter to use here is indicated by
              the Managers `index_by` attribute
            progress: Will be called with a string describing the progress of the backup restoration
        """

    @abc.abstractmethod
    def delete_backup(self, id_: _id_T, progress: Callable[[str], None] = _noop) -> None:
        """Delete a backup.

        Args:
            id_: Identifier of the backup to delete. What parameter to use here is indicated by
              the Managers `index_by` attribute
            progress: Will be called with a string describing the progress of the backup deletion
        """

    @abc.abstractmethod
    def list_backups(self) -> list[BackupInfo]:
        """Returns a list of backups, ordered newest to oldest."""

    def _clear_world(self) -> None:
        leaves = []
        for root, dirs, files in os.walk(self._world):
            has_kept_files = False
            for name in files:
                if name in BACKUP_IGNORE_FROZENSET:
                    has_kept_files = True
                    continue
                Path(root, name).unlink()

            if not dirs:
                if not has_kept_files:
                    leaves.append(root)
            else:
                dirs[:] = set(dirs) - BACKUP_IGNORE_FROZENSET
        for leaf in leaves:
            try:
                os.removedirs(leaf)
            except OSError:
                pass

    # TODO: add cron functionality with aiocron


def _delete_file_or_dir(path: Path) -> None:
    try:
        path.unlink()
    except IsADirectoryError:
        shutil.rmtree(path)
    except FileNotFoundError:
        pass
