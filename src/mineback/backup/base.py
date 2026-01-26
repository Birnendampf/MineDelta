import abc
import datetime
import os
import shutil
from pathlib import Path
from typing import (
    Annotated,
    TYPE_CHECKING,
    Final,
    TypeVar,
    Generic,
    ClassVar,
    Literal,
    Callable,
    Any,
)

import msgspec

if TYPE_CHECKING:
    from _typeshed import StrPath

_id_T = TypeVar("_id_T", str, int)


def _noop(_: Any) -> None: ...


BACKUP_IGNORE: Final = ["datapacks", "session.lock", "DistantHorizons.sqlite", "icon.png"]
BACKUP_IGNORE_FROZENSET: Final = frozenset(BACKUP_IGNORE)


class BackupInfo(msgspec.Struct):
    timestamp: Annotated[datetime.datetime, msgspec.Meta(tz=True)]
    id: str
    desc: str | None = None


class BaseBackupManager(Generic[_id_T], metaclass=abc.ABCMeta):
    __slots__ = "_backup_dir", "_world"
    index_by: ClassVar[Literal["idx", "id"]]

    def __init__(self, save: "StrPath", backup_dir: Path):
        self._world = save
        self._backup_dir = backup_dir

    def prepare(self) -> None:
        self._backup_dir.mkdir(parents=True, exist_ok=True)

    @abc.abstractmethod
    def create_backup(
        self, description: str | None = None, progress: Callable[[str], None] = _noop
    ) -> BackupInfo: ...

    @abc.abstractmethod
    def restore_backup(self, id_: _id_T, progress: Callable[[str], None] = _noop) -> None: ...

    @abc.abstractmethod
    def delete_backup(self, id_: _id_T, progress: Callable[[str], None] = _noop) -> None: ...

    @abc.abstractmethod
    def list_backups(self) -> list[BackupInfo]: ...

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
