import filecmp
import os
from collections.abc import Callable, Iterable
from pathlib import Path
from typing import Any

import pytest

from minedelta.backup import BaseBackupManager, GitBackupManager, base, diff
from minedelta.region import RegionFile


# noinspection PyProtectedMember
def assert_matches_world(world: Path, reference: Path) -> None:
    compare_stack = [("", filecmp.dircmp(world, reference))]
    while compare_stack:
        common_dir, compare = compare_stack.pop()
        compare_stack.extend(compare.subdirs.items())
        right_only = set(compare.right_only)
        if common_dir in diff.MCA_FOLDERS:
            # empty regions can be missing
            for file in compare.right_only:
                if not Path(compare.right, file).stat().st_size:
                    right_only.discard(file)

            for file in compare.diff_files:
                # noinspection PyTypeChecker
                with (
                    RegionFile.open(Path(compare.right, file)) as ref_region,
                    RegionFile.open(Path(compare.left, file)) as actual_region,
                ):
                    for ref_header, actual_header in zip(
                        ref_region._headers, actual_region._headers, strict=True
                    ):
                        assert ref_header.not_created == actual_header.not_created
                        assert not actual_header.unmodified
                        if not actual_header.not_created:
                            assert ref_region._check_unchanged(
                                ref_header, actual_region, actual_header, common_dir == "region"
                            )
        assert not compare.left_only
        for file in right_only:
            assert_all_ignored(Path(compare.right, file))


def assert_all_ignored(path: Path) -> None:
    if path.name in base.BACKUP_IGNORE_FROZENSET:
        return
    assert os.path.isdir(path)  # noqa: PTH112
    for _, dirs, files in os.walk(path):
        dirs[:] = set(dirs) - base.BACKUP_IGNORE_FROZENSET
        assert not set(files) - base.BACKUP_IGNORE_FROZENSET


def test_restore_backup(
    loaded_manager: BaseBackupManager[Any],
    world_variations: Iterable[Path],
    subtests: pytest.Subtests,
) -> None:
    restore_func = get_restore_func(loaded_manager)

    for i, variation in enumerate(world_variations):
        with subtests.test(idx=i):
            restore_func(i)
            assert_matches_world(Path(loaded_manager._world), variation)


def get_restore_func(manager: BaseBackupManager[Any]) -> Callable[[int], None]:
    if manager.index_by == "idx":

        def restore_func(idx: int) -> None:
            manager.restore_backup(idx)
    else:
        infos = manager.list_backups()

        def restore_func(idx: int) -> None:
            manager.restore_backup(infos[idx].id)

    return restore_func


@pytest.mark.parametrize("delete_idx", range(2))
def test_delete_backup(
    world_variations: tuple[Path, ...],
    loaded_manager: BaseBackupManager[Any],
    delete_idx: int,
    subtests: pytest.Subtests,
) -> None:
    if loaded_manager.index_by == "idx":
        loaded_manager.delete_backup(delete_idx)
    else:
        infos = loaded_manager.list_backups()
        loaded_manager.delete_backup(infos[delete_idx].id)
    variations = list(world_variations)
    deleted = variations.pop(delete_idx)
    test_restore_backup(loaded_manager, variations, subtests)
    orig_world = loaded_manager._world
    loaded_manager._world = deleted
    if isinstance(loaded_manager, GitBackupManager):
        loaded_manager.prepare()
        loaded_manager.create_backup(deleted.name)
        (deleted / ".git").unlink()
    else:
        loaded_manager.create_backup(deleted.name)
    variations.insert(0, deleted)
    loaded_manager._world = orig_world
    test_restore_backup(loaded_manager, variations, subtests)


def test_preserve_ignore(manager: BaseBackupManager[Any]) -> None:
    ignored_files = []
    manager.create_backup("empty")
    world = Path(manager._world)

    icon = world / "icon.png"
    icon.touch()
    ignored_files.append(icon)

    session_lock = world / "session.lock"
    session_lock.touch()
    ignored_files.append(session_lock)

    deep_ignored = world / "DIM-1" / "data" / "DistantHorizons.sqlite"
    deep_ignored.parent.mkdir(parents=True)
    deep_ignored.write_text("Not a real DB lol")
    ignored_files.append(deep_ignored)

    some_pack = world / "datapacks" / "some_pack"
    some_pack.parent.mkdir()
    some_pack.touch()
    ignored_files.append(some_pack)

    restore_func = get_restore_func(manager)
    restore_func(0)
    for file in ignored_files:
        assert file.exists()
