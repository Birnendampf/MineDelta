"""Contains the `BaseBackupManager` and `BackupInfo` classes.

The `BaseBackupManager` is provided only to allow custom backup managers to be derived from it.
It is not intended to be used directly.
"""

import abc
import contextlib
import datetime
import os
import shutil
import sys
from collections.abc import Callable, Iterator
from pathlib import Path
from typing import TYPE_CHECKING, Annotated, Any, ClassVar, Final, Generic, Literal, TypeVar

import msgspec

if sys.version_info >= (3, 12):  # pragma: no cover
    from typing import override
else:
    from typing_extensions import override


if TYPE_CHECKING:
    from _typeshed import StrPath

__all__ = ["BACKUP_IGNORE", "BACKUP_IGNORE_FROZENSET", "BackupInfo", "BaseBackupManager"]

_id_T = TypeVar("_id_T", str, int)  # noqa: N816


def _noop(_: Any) -> None: ...  # noqa: ANN401


BACKUP_IGNORE: Final = ["datapacks", "session.lock", "DistantHorizons.sqlite", "icon.png", ".git"]
BACKUP_IGNORE_FROZENSET: Final = frozenset(BACKUP_IGNORE)


class BackupInfo(msgspec.Struct, omit_defaults=True):
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
            with contextlib.suppress(OSError):
                os.removedirs(leaf)

    # TODO: add cron functionality with aiocron


def _delete_file_or_dir(path: Path) -> None:
    try:
        path.unlink()
    except IsADirectoryError:
        shutil.rmtree(path)
    except FileNotFoundError:
        pass


_BackupInfoT = TypeVar("_BackupInfoT", bound="BackupInfo")


class _MetaDataManager(BaseBackupManager[int], Generic[_BackupInfoT], metaclass=abc.ABCMeta):
    index_by = "idx"
    __slots__ = ("_backups_data_path",)

    _BackupDataENCODER: ClassVar = msgspec.msgpack.Encoder(uuid_format="bytes")
    # noinspection PyClassVar
    _BackupDataDECODER: ClassVar[msgspec.msgpack.Decoder[list[_BackupInfoT]]] = (
        msgspec.msgpack.Decoder(list[BackupInfo])
    )

    @override
    def __init__(self, save: "StrPath", backup_dir: Path):
        super().__init__(save, backup_dir)
        self._backups_data_path: Final = backup_dir / "backups.dat"

    def _load_backups_data(self) -> list[_BackupInfoT]:
        try:
            return self._BackupDataDECODER.decode(self._backups_data_path.read_bytes())
        except FileNotFoundError:
            return msgspec.json.decode(
                self._backups_data_path.with_suffix(".json").read_bytes(), type=list[_BackupInfoT]
            )

    def _write_backups_data(self, backups_data: list[_BackupInfoT]) -> None:
        self._backups_data_path.write_bytes(self._BackupDataENCODER.encode(backups_data))

    def write_backups_data_json(self) -> None:
        """Convert the backups data to human-readable JSON format."""
        decoded = self._BackupDataDECODER.decode(self._backups_data_path.read_bytes())
        self._backups_data_path.with_suffix(".json").write_bytes(
            msgspec.json.format(msgspec.json.encode(decoded, order="deterministic"))
        )

    @override
    def list_backups(self) -> list[BackupInfo]:
        try:
            backups_data = self._load_backups_data()
        except FileNotFoundError:
            return []
        return msgspec.convert(backups_data, list[BackupInfo], from_attributes=True)

    def _load_backups_data_validate_idx(self, idx: int) -> list[_BackupInfoT]:
        if idx < 0:
            raise IndexError("index must be >= 0")
        try:
            backup_infos = self._load_backups_data()
        except FileNotFoundError:
            raise IndexError("no backups data found") from None
        if idx >= len(backup_infos):
            raise IndexError(f"no backup found with index {idx}")
        return backup_infos

    @contextlib.contextmanager
    def _prepare_create_backup(
        self,
        description: str | None,
        progress: Callable[[str], None],
        backup_data_type: type[_BackupInfoT],
    ) -> Iterator[tuple[_BackupInfoT, _BackupInfoT | None]]:
        timestamp = datetime.datetime.now(datetime.UTC).replace(microsecond=0)
        try:
            backups_data = self._load_backups_data()
            previous: _BackupInfoT | None = backups_data[0]
        except (FileNotFoundError, IndexError):
            backups_data = []
            previous = None
        prev_ids = {data.id for data in backups_data}
        id_ = timestamp.strftime("%Y-%m-%dT%H:%M:%S")
        suffix = 0
        id_format = id_ + "_{}"
        while id_ in prev_ids:
            id_ = id_format.format(suffix)
            suffix += 1

        progress(f'creating backup "{id_}"')
        new_backup = backup_data_type(timestamp, id_, description)
        yield new_backup, previous
        # write back data if no exceptions occurred
        backups_data.insert(0, new_backup)
        self._write_backups_data(backups_data)
