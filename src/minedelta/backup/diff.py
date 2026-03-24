"""Create backups by storing only changed chunks in region files for previous Backups.

For more details, see `DiffBackupManager`.
"""

import concurrent.futures
import contextlib
import filecmp
import os
import shutil
import sys
import tempfile
from collections.abc import Callable, Iterable, Set
from pathlib import Path
from typing import TYPE_CHECKING, Final, Self

import msgspec

from minedelta._thread_scope import ThreadScope
from minedelta.region import RegionFile

from .base import BACKUP_IGNORE, BACKUP_IGNORE_FROZENSET, BackupInfo, _MetaDataManager, _noop

if TYPE_CHECKING:
    from _typeshed import StrPath, Unused

if sys.version_info >= (3, 12):  # pragma: no cover
    from typing import override
else:
    from typing_extensions import override

if sys.version_info >= (3, 14):  # pragma: no cover
    import tarfile

    from compression import zstd

else:
    from backports import zstd
    from backports.zstd import tarfile

__all__ = ["DiffBackupManager"]

MCA_FOLDERS: Final = frozenset(("region", "entities", "poi"))


class BackupData(BackupInfo):
    not_present: set[str] = set()  # noqa: RUF012  # msgspec understands

    @property
    def name(self) -> str:
        """Return the name corresponding to this backup (id + ".tar.zst")."""
        return self.id + ".tar.zst"


def _extract_backup(
    backup_dir: Path,
    temp_dir: "StrPath",
    backup_data: BackupData,
    skip: Iterable[str] | None = None,
) -> Path:
    """Extract only paths not listed in `skip`.

    Args:
        backup_dir: Directory to extract backups from.
        temp_dir: Directory to extract to.
        backup_data: Metadata of backup to extract.
        skip: Set of paths to skip.

    Returns:
        the path of the extracted backup.
    """
    extracted = Path(temp_dir, backup_data.id)
    src = backup_dir / backup_data.name
    try:
        __extract(src, extracted, skip)
    except OSError:  # maybe some other compression method?
        extracted = Path(temp_dir, "fallback_" + backup_data.id)
        _py_extract(src, extracted, skip)
    return extracted


def _py_extract(
    src: "StrPath", dest: "StrPath", exclude: Iterable["StrPath"] | None = None
) -> None:
    skip = [os.fspath(file) for file in exclude or ()]
    if skip:
        skipped_dirs = []

        def custom_filter(member: tarfile.TarInfo, dest_path: str) -> tarfile.TarInfo | None:
            if member.name in skip:
                if member.isdir():
                    skipped_dirs.append(member.name)
                return None
            if any(member.name.startswith(d) for d in skipped_dirs):
                return None
            return tarfile.data_filter(member, dest_path)
    else:
        custom_filter = tarfile.data_filter
    with tarfile.open(src, "r:*") as tar:
        tar.extractall(dest, filter=custom_filter)  # noqa: S202


def _py_create_archive(
    src: "StrPath",
    dest: "StrPath",
    exclude: Set[str] = frozenset(),
    n_workers: int = 0,
    level: int = 0,
) -> None:
    if not exclude:
        _backup_filter = None
    else:

        def _backup_filter(tarinfo: tarfile.TarInfo) -> tarfile.TarInfo | None:
            """Filter for creating tarfiles that drops files from BACKUP_IGNORE."""
            # using os.path because it is not worth it to create a Path just for this
            if os.path.basename(tarinfo.name) in exclude:  # noqa: PTH119
                return None
            return tarinfo

    with tarfile.open(
        dest,
        "w:zst",
        options={
            zstd.CompressionParameter.nb_workers: n_workers,
            zstd.CompressionParameter.compression_level: level,
        },
    ) as new_tar:
        new_tar.add(src, "", filter=_backup_filter)


try:
    import filtar
except ImportError:  # pragma: no cover
    __extract = _py_extract
    _create_archive = _py_create_archive
else:
    __extract = filtar.extract
    _create_archive = filtar.create


class DiffBackupManager(_MetaDataManager[BackupData]):
    """Manager to create backups that only store changed chunks.

    The newest backup (at idx 0) is essentially complete copy, every previous n-th backup stores the
    changes needed to turn the newer (n-1)th backup into itself. Illustration:

    ===  ===========  =================
    idx  files        what is stored
    ===  ===========  =================
    0    a0    c1 d2  a0    c1 d2
    1       b0 c0 d1  -a b0 c1->0 d2->1
    2             d0     -b -c    d1->0
    ===  ===========  =================

    Some methods in this class take an additional `executor` parameter. This allows a
    ThreadPoolexecutor to be reused between calls. If not specified, a new one with the number of
    workers equal to the number of available cpu cores will be used
    """

    __slots__ = ("_zstd_level", "_zstd_workers")
    _BackupDataDECODER = msgspec.msgpack.Decoder(list[BackupData])

    def __init__(
        self,
        save: "StrPath",
        backup_dir: Path,
        zstd_worker_count: int = 0,
        compression_level: int = 0,
    ):
        """Create a new DiffBackupManager.

        Args:
            save: world to create backups for
            backup_dir: where to store the backups
            zstd_worker_count: how many workers to use for compression.
              More workers improve speed at the cost of memory usage.
              Check if minedelta already saturates your CPU before increasing this.
            compression_level: compression level to use when creating backups.
        """
        self._zstd_workers = zstd_worker_count
        self._zstd_level = compression_level
        super().__init__(save, backup_dir)

    @override
    def create_backup(
        self,
        description: str | None = None,
        progress: Callable[[str], None] = _noop,
        executor: concurrent.futures.Executor | None = None,
    ) -> BackupInfo:
        with (
            self._prepare_create_backup(description, progress, BackupData) as (new_backup, prev),
            # create temporary directory in backup dir to ensure replace succeeds
            tempfile.TemporaryDirectory(dir=self._backup_dir) as temp_dir,
        ):
            new_backup_file = Path(temp_dir, new_backup.name)
            # the tarfile is intentionally opened and closed here, not in a seperate thread.
            with ThreadScope(executor, "create backup") as scope:
                scope.submit(self._compress_world, new_backup_file)
                if prev:
                    # True temporary directory to reduce IO, see #39
                    with tempfile.TemporaryDirectory() as temp_2:
                        progress(f'turning "{prev.id}" into diff')
                        prev_world = _extract_backup(self._backup_dir, temp_2, prev)
                        not_present = _filter_diff(
                            src=self._world, dest=prev_world, scope=scope, progress=progress
                        )
                        progress(f'recompressing "{prev.id}"')
                        new_previous = Path(temp_dir, prev.name)
                        _create_archive(
                            prev_world,
                            new_previous,
                            n_workers=self._zstd_workers,
                            level=self._zstd_level,
                        )
                        # ensure backup creation went well before overwriting prev
                progress("compressing world")
            new_backup_file.replace(self._backup_dir / new_backup.name)
            if prev:
                prev.not_present = not_present
                new_previous.replace(self._backup_dir / prev.name)

        return BackupInfo(new_backup.timestamp, new_backup.id, new_backup.desc)

    def _compress_world(self, dest: Path) -> None:
        _create_archive(
            self._world, dest, BACKUP_IGNORE_FROZENSET, self._zstd_workers, self._zstd_level
        )

    @override
    def restore_backup(
        self,
        id_: int,
        progress: Callable[[str], None] = _noop,
        executor: concurrent.futures.Executor | None = None,
    ) -> None:
        backups_data = self._load_backups_data_validate_idx(id_)
        progress(f'restoring backup "{backups_data[id_].id}"')
        backups_slice = backups_data[1 : id_ + 1]
        with (
            tempfile.TemporaryDirectory() as temp_dir,
            ThreadScope(executor, "restore backup") as scope,
        ):
            tasks = []
            skip: frozenset[str] = frozenset()
            for backup in reversed(backups_slice):
                tasks.append(
                    scope.submit(_extract_backup, self._backup_dir, temp_dir, backup, skip)
                )
                skip |= backup.not_present
            newest_backup = _extract_backup(self._backup_dir, temp_dir, backups_data[0], skip)
            with _RegionFileCache() as region_file_cache:
                for i, (backup_data, extract_task) in enumerate(
                    zip(backups_slice, reversed(tasks), strict=True), 1
                ):
                    progress(f'[{i}/{len(backups_slice)}] applying "{backup_data.id}"')
                    _apply_diff(
                        dest=newest_backup, src=extract_task.result(), cache=region_file_cache
                    )
            progress("deleting current world")
            self._clear_world()
            progress("restoring backup")
            shutil.copytree(newest_backup, self._world, dirs_exist_ok=True)

    @override
    def delete_backup(
        self,
        id_: int,
        progress: Callable[[str], None] = _noop,
        executor: concurrent.futures.Executor | None = None,
    ) -> None:
        backups_data = self._load_backups_data_validate_idx(id_)
        if id_ == len(backups_data) - 1:  # deleting oldest is easy
            data_chosen = backups_data.pop()
            progress(f'deleting oldest backup "{data_chosen.id}"')
            (self._backup_dir / data_chosen.name).unlink()
            self._write_backups_data(backups_data)
            return

        data_older = backups_data[id_ + 1]
        data_chosen = backups_data[id_]
        chosen_not_present = data_chosen.not_present.copy()
        progress(f'merging "{data_older.id}" into "{data_chosen.id}"')
        older_archive = self._backup_dir / data_older.name
        with (
            tempfile.TemporaryDirectory() as temp_dir,
            ThreadScope(executor, "delete backup") as scope,
        ):
            chosen_fut = scope.submit(
                _extract_backup,
                self._backup_dir,
                temp_dir,
                data_chosen,
                data_older.not_present,
            )
            older = _extract_backup(self._backup_dir, temp_dir, data_older)
            chosen = chosen_fut.result()
            _apply_diff(src=older, dest=chosen, defragment=True)
            # handle the following situation (1 being deleted):
            # idx | files | diff | new diff
            # 0   | a0    |      |
            # 1   |       | -a   | a0
            # 2   | a0    | a0   | (deleted)
            for file in data_chosen.not_present:
                if Path(older, file).exists():
                    chosen_not_present.discard(file)
            progress(f'recompressing "{data_chosen.id}" as "{data_older.name}"')
            _create_archive(
                chosen, older_archive, n_workers=self._zstd_workers, level=self._zstd_level
            )
        if id_:
            # handle the following situation (1 being deleted):
            # idx | files       | diff              | new diff
            # 0   | a0    c1 d2 |                   |
            # 1   |    b0 c0 d1 | -a b0 c1->0 d2->1 | -a -b -c d2->0
            # 2   |          d0 |    -b -c    d1->0 | (deleted)
            # note that -b is contained in the new diff, because we do not know that b0 was
            # deleted again at idx 0
            data_older.not_present |= chosen_not_present
        else:
            data_older.not_present.clear()
        del backups_data[id_]
        self._write_backups_data(backups_data)
        (self._backup_dir / data_chosen.name).unlink()


# FILTERING


def _filter_diff(
    *,
    src: "StrPath",
    dest: "StrPath",
    scope: concurrent.futures.Executor | ThreadScope | None,
    progress: Callable[[str], None] = _noop,
) -> set[str]:
    """Delete files and chunks from `dest` in common with `src`. `src` is not altered.

    Files and directories from BACKUP_IGNORE are skipped.

    Args:
        src: directory to compare against
        dest: directory to perform changes in
        scope: Executor to use for filtering
        progress: Will be called with a string describing which anvil file is being filtered
    Returns: set of files found in `src` but not `dest`, relavtive to src
    """
    compare = filecmp.dircmp(src, dest, BACKUP_IGNORE)
    not_present = set()
    compare_stack = [("", compare)]
    filer_task_count = 0
    with ThreadScope(scope, "filter_diff") as scope:  # noqa: PLR1704
        while compare_stack:
            common_dir, compare = compare_stack.pop()
            compare_stack.extend(compare.subdirs.items())
            for file in compare.left_only:
                not_present.add(Path(compare.left, file).relative_to(src).as_posix())
            for file in compare.same_files:
                Path(compare.right, file).unlink()
            if common_dir not in MCA_FOLDERS:
                continue
            for file in compare.diff_files:
                if file.endswith(".mcc"):
                    continue
                src_file = Path(compare.left, file)
                dest_file = Path(compare.right, file)
                if not src_file.stat().st_size:
                    continue
                if not dest_file.stat().st_size:
                    dest_file.unlink()
                    not_present.add(src_file.relative_to(src).as_posix())
                    continue
                filer_task_count += 1
                scope.submit(_filter_region, src_file, dest_file, common_dir == "region")
    progress(f"filtered {filer_task_count} regions")

    return not_present


def _filter_region(src_file: Path, dest_file: Path, is_chunk: bool) -> None:
    with RegionFile(src_file) as new_region, RegionFile(dest_file) as old_region:
        unchanged = old_region.filter_diff_defragment(new_region, is_chunk)
    if unchanged:
        dest_file.unlink()


# APPLYING


class _RegionFileCache:
    __slots__ = ("_cached_regions",)

    def __init__(self) -> None:
        self._cached_regions: dict[Path, RegionFile] = {}

    def __enter__(self) -> Self:
        return self

    def get(self, path: Path) -> RegionFile:
        with contextlib.suppress(KeyError):
            return self._cached_regions[path]
        new_region = RegionFile(path).__enter__()
        self._cached_regions[path] = new_region
        return new_region

    def __exit__(self, *_: "Unused") -> None:
        exceptions: list[Exception] = []
        for region in self._cached_regions.values():
            try:
                region.__exit__()
            except Exception as e:
                exceptions.append(e)
        self._cached_regions.clear()
        if exceptions:
            raise ExceptionGroup("Exceptions occured while trying to close Regions", exceptions)


def _apply_diff(
    *,
    src: "StrPath",
    dest: "StrPath",
    defragment: bool = False,
    cache: _RegionFileCache | None = None,
) -> None:
    for dirpath, dirs, files in os.walk(src):
        dest_dirpath = dest / Path(dirpath).relative_to(src)
        for dirname in dirs:
            (dest_dirpath / dirname).mkdir(exist_ok=True)
        for file in files:
            src_file = Path(dirpath, file)
            dest_file = dest_dirpath / file
            if _should_apply_diff(src_file, dest_file):
                dest_region_cm = (
                    contextlib.nullcontext(cache.get(dest_file)) if cache else RegionFile(dest_file)
                )
                with RegionFile(src_file) as src_region, dest_region_cm as dest_region:
                    dest_region.apply_diff(src_region, defragment)
            else:
                shutil.copy2(src_file, dest_file)


def _should_apply_diff(src_file: Path, dest_file: Path) -> bool:
    if src_file.suffix != ".mca" or not src_file.stat().st_size:
        return False
    try:
        if not dest_file.stat().st_size:
            return False
    except (OSError, ValueError):
        return False

    return True
