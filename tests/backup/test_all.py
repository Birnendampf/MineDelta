import filecmp
import os
from pathlib import Path

import pytest

from minedelta.backup import BaseBackupManager, base, diff
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
    load_manager: BaseBackupManager, world_variations: tuple[Path, ...], subtests: pytest.Subtests
) -> None:
    if load_manager.index_by == "idx":

        def restore_func(idx: int) -> None:
            load_manager.restore_backup(idx)
    else:
        infos = load_manager.list_backups()

        def restore_func(idx: int) -> None:
            return load_manager.restore_backup(infos[idx].id)

    for i, variation in enumerate(reversed(world_variations)):
        with subtests.test(idx=i):
            restore_func(i)
            assert_matches_world(Path(load_manager._world), variation)
