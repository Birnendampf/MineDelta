"""Create backups by hardlinking duplicate files.

For more details, see `HardlinkBackupManager`.
"""

import contextlib
import filecmp
import os
import shutil
import sys
from collections.abc import Callable
from pathlib import Path

import msgspec

from .base import BACKUP_IGNORE, BACKUP_IGNORE_FROZENSET, BackupInfo, _MetaDataManager, _noop

if sys.version_info >= (3, 12):  # pragma: no cover
    from typing import override
else:
    from typing_extensions import override

__all__ = ["HardlinkBackupManager"]


def copytree_backup_ignore(_: str, names: list[str]) -> frozenset[str]:
    return BACKUP_IGNORE_FROZENSET.intersection(names)


class HardlinkBackupManager(_MetaDataManager[BackupInfo]):
    """Create backups by copying the world and hardlinking unchanged files to previous backups."""

    __slots__ = ()
    _BackupDataDECODER = msgspec.msgpack.Decoder(list[BackupInfo])

    @override
    def create_backup(
        self, description: str | None = None, progress: Callable[[str], None] = _noop
    ) -> BackupInfo:
        with self._prepare_create_backup(description, progress, BackupInfo) as (
            new_backup,
            previous,
        ):
            new_backup_file = self._backup_dir / new_backup.id
            if not previous:
                progress("copying world (no previous backup found)")
                shutil.copytree(self._world, new_backup_file, ignore=copytree_backup_ignore)
                return new_backup

            prev_world = self._backup_dir / previous.id
            progress(f'comparing against "{previous.id}"')
            compare = filecmp.dircmp(self._world, prev_world, BACKUP_IGNORE)
            compare_stack = [compare]
            while compare_stack:
                compare = compare_stack.pop()
                compare_stack.extend(compare.subdirs.values())

                current_new = new_backup_file / Path(compare.right).relative_to(prev_world)
                current_new.mkdir(exist_ok=True)
                for name in compare.left_only + compare.diff_files:
                    file = Path(compare.left, name)
                    new_file = current_new / name
                    try:
                        shutil.copy2(file, new_file)
                    except (IsADirectoryError, PermissionError):
                        shutil.copytree(file, new_file, ignore=copytree_backup_ignore)
                for name in compare.same_files:
                    (current_new / name).hardlink_to(Path(compare.right, name))
            return new_backup

    @override
    def restore_backup(self, id_: int, progress: Callable[[str], None] = _noop) -> None:
        backups_data = self._load_backups_data_validate_idx(id_)
        data_chosen = backups_data[id_]
        chosen = self._backup_dir / data_chosen.id
        progress("deleting current world")
        self._clear_world()
        progress(f'restoring "{data_chosen.id}"')
        shutil.copytree(chosen, self._world, dirs_exist_ok=True)

    @override
    def delete_backup(self, id_: int, progress: Callable[[str], None] = _noop) -> None:
        backups_data = self._load_backups_data_validate_idx(id_)
        data_chosen = backups_data.pop(id_)
        chosen = self._backup_dir / data_chosen.id
        progress(f'deleting "{data_chosen.id}"')
        shutil.rmtree(chosen)
        self._write_backups_data(backups_data)

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
