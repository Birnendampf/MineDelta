import collections
import datetime
import filecmp
import operator
import os
import shutil
import time
from collections.abc import Iterator
from os import DirEntry
from pathlib import Path
from typing import Any, Callable

from .base import BaseBackupManager, BackupInfo, BACKUP_IGNORE_FROZENSET, BACKUP_IGNORE, _noop


def copytree_backup_ignore(_: Any, names: list[str]) -> frozenset[str]:
    return BACKUP_IGNORE_FROZENSET.intersection(names)


class HardlinkBackupManager(BaseBackupManager[str]):
    __slots__ = ()
    index_by = "id"

    def create_backup(
        self, description: str | None = None, progress: Callable[[str], None] = _noop
    ) -> BackupInfo:
        timestamp = round(time.time())
        new_backup = self._backup_dir / str(timestamp)
        new_info = BackupInfo(
            datetime.datetime.fromtimestamp(timestamp, datetime.UTC), str(timestamp), None
        )
        if new_backup.is_dir():
            return new_info
        elif new_backup.exists():
            new_backup.unlink(True)

        other_backups = self._get_valid_backups()
        try:
            prev, prev_timestamp = max(other_backups, key=operator.itemgetter(1))
        except ValueError:
            progress("copying world (no previous backup found)")
            shutil.copytree(self._world, new_backup, ignore=copytree_backup_ignore)
            return new_info

        assert prev_timestamp < timestamp, "found backup from the future???"

        if progress is not _noop:
            prev_datetime = datetime.datetime.fromtimestamp(prev_timestamp, datetime.UTC)
            progress(f"comparing against backup from {prev_datetime}")
        compare = filecmp.dircmp(self._world, prev, BACKUP_IGNORE)
        compare_stack = collections.deque((compare,))
        while compare_stack:
            compare = compare_stack.pop()
            compare_stack.extend(compare.subdirs.values())

            current_new = new_backup / Path(compare.right).relative_to(prev)
            current_new.mkdir(exist_ok=True)
            for name in compare.left_only + compare.diff_files:
                file = Path(compare.left, name)
                new_file = current_new / name
                try:
                    shutil.copy2(file, new_file)
                except IsADirectoryError:
                    shutil.copytree(file, new_file, ignore=copytree_backup_ignore)
            for name in compare.same_files:
                (current_new / name).hardlink_to(Path(compare.right, name))
        return new_info

    def _get_valid_backups(self) -> Iterator[tuple[DirEntry[str], int]]:
        return (
            (child, int(child.name))
            for child in os.scandir(self._backup_dir)
            if child.name.isdecimal() and child.is_dir()
        )

    def _get_sorted_backups(self) -> list[tuple[DirEntry[str], int]]:
        return sorted(self._get_valid_backups(), key=operator.itemgetter(1), reverse=True)

    def restore_backup(self, id_: str, progress: Callable[[str], None] = _noop) -> None:
        backup = self._backup_dir / id_
        assert backup.is_dir()
        progress("deleting current world")
        self._clear_world()
        if progress is not _noop:
            restore_datetime = datetime.datetime.fromtimestamp(int(id_), datetime.UTC)
            progress(f"restoring backup from {restore_datetime}")
        shutil.copytree(backup, self._world, dirs_exist_ok=True)

    def delete_backup(self, id_: str, progress: Callable[[str], None] = _noop) -> None:
        backup = self._backup_dir / id_
        if progress is not _noop:
            delete_datetime = datetime.datetime.fromtimestamp(int(id_), datetime.UTC)
            progress(f"deleting backup from {delete_datetime}")
        shutil.rmtree(backup)

    def list_backups(self) -> list[BackupInfo]:
        return [
            BackupInfo(datetime.datetime.fromtimestamp(backup[1], datetime.UTC), str(backup[1]))
            for backup in self._get_sorted_backups()
        ]
