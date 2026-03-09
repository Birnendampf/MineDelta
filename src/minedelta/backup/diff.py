"""Create backups by storing only changed chunks in region files for previous Backups.

For more details, see `DiffBackupManager`.
"""

import concurrent.futures
import contextlib
import filecmp
import os
import shutil
import sys
import tarfile
import tempfile
from collections.abc import Callable, Container
from pathlib import Path
from typing import TYPE_CHECKING, Final, Self

import msgspec

from minedelta._dummy_executor import DummyExecutor
from minedelta.region import RegionFile

from .base import BACKUP_IGNORE, BACKUP_IGNORE_FROZENSET, BackupInfo, _MetaDataManager, _noop

if sys.version_info >= (3, 12):  # pragma: no cover
    from typing import override
else:
    from typing_extensions import override

if TYPE_CHECKING:
    from _typeshed import StrPath, Unused

__all__ = ["MAX_WORKERS", "DiffBackupManager"]

MCA_FOLDERS: Final = frozenset(("region", "entities", "poi"))

_cpu_count: int | None = None
with contextlib.suppress(AttributeError):
    _cpu_count = os.process_cpu_count()  # type: ignore[attr-defined]
if _cpu_count is None:  # pragma: no cover
    # sys.version_info < 3.13
    with contextlib.suppress(AttributeError):
        _cpu_count = len(os.sched_getaffinity(0))
if _cpu_count is None:  # pragma: no cover
    _cpu_count = os.cpu_count()

MAX_WORKERS = _cpu_count or 1
del _cpu_count

# InterpreterPool is not supported
_DefaultExecutor = concurrent.futures.ThreadPoolExecutor


def _get_executor(
    executor: concurrent.futures.Executor | None,
) -> contextlib.nullcontext[concurrent.futures.Executor] | concurrent.futures.Executor:
    if executor:
        return contextlib.nullcontext(executor)
    if MAX_WORKERS == 1:
        return DummyExecutor()
    return _DefaultExecutor(max_workers=MAX_WORKERS)


class BackupData(BackupInfo):
    not_present: set[str] = set()  # noqa: RUF012  # msgspec understands

    @property
    def name(self) -> str:
        """Return the name corresponding to this backup (id + ".tar.gz")."""
        return self.id + ".tar.gz"


def _extract_backup(
    backup_dir: Path,
    temp_dir: "StrPath",
    backup_data: BackupData,
    skip: Container[str] | None = None,
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

    extracted = Path(temp_dir, backup_data.id)
    with tarfile.open(backup_dir / backup_data.name, "r:gz") as tar:
        tar.extractall(extracted, filter=custom_filter)  # noqa: S202
    return extracted


def _backup_filter(tarinfo: tarfile.TarInfo) -> tarfile.TarInfo | None:
    """Filter for creating tarfiles that drops files from BACKUP_IGNORE."""
    # using os.path because it is not worth it to create a Path just for this
    if os.path.basename(tarinfo.name) in BACKUP_IGNORE_FROZENSET:  # noqa: PTH119
        return None
    return tarinfo


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

    __slots__ = ()
    _BackupDataDECODER = msgspec.msgpack.Decoder(list[BackupData])

    @override
    def create_backup(
        self,
        description: str | None = None,
        progress: Callable[[str], None] = _noop,
        executor: concurrent.futures.Executor | None = None,
    ) -> BackupInfo:
        with (
            self._prepare_create_backup(description, progress, BackupData) as (
                new_backup,
                previous,
            ),
            # create Temporary directory in backup dir to ensure replace succeeds
            tempfile.TemporaryDirectory(dir=self._backup_dir) as temp_dir,
            _get_executor(executor) as ex,
        ):
            new_backup_file = Path(temp_dir, new_backup.name)
            # the tarfile is intentionally opened and closed here, not in a seperate thread.
            with tarfile.open(new_backup_file, "x:gz") as new_tar:
                progress("compressing world")
                backup_fut = ex.submit(new_tar.add, self._world, "", filter=_backup_filter)
                if previous:
                    prev_world = _extract_backup(self._backup_dir, temp_dir, previous)
                    progress(f'turning "{previous.id}" into diff')
                    not_present = _filter_diff(
                        src=self._world, dest=prev_world, executor=ex, progress=progress
                    )
                    progress(f'recompressing "{previous.id}"')
                    new_previous = Path(temp_dir, previous.name)
                    with tarfile.open(new_previous, "x:gz") as prev_tar:
                        prev_tar.add(prev_world, "")
                # ensure backup creation went well before overwriting previous
                backup_fut.result()
            new_backup_file.replace(self._backup_dir / new_backup.name)
            if previous:
                previous.not_present = not_present
                new_previous.replace(self._backup_dir / previous.name)

        return BackupInfo(new_backup.timestamp, new_backup.id, new_backup.desc)

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
        with tempfile.TemporaryDirectory() as temp_dir, _get_executor(executor) as ex:
            tasks = []
            skip: frozenset[str] = frozenset()
            for backup in reversed(backups_slice):
                tasks.append(ex.submit(_extract_backup, self._backup_dir, temp_dir, backup, skip))
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
        with tempfile.TemporaryDirectory() as temp_dir, _get_executor(executor) as ex:
            chosen_fut = ex.submit(
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
            with tarfile.open(older_archive, "w:gz") as tar:
                tar.add(chosen, "")

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
    executor: concurrent.futures.Executor,
    progress: Callable[[str], None] = _noop,
) -> set[str]:
    """Delete files and chunks from `dest` in common with `src`. `src` is not altered.

    Files and directories from BACKUP_IGNORE are skipped.

    Args:
        src: directory to compare against
        dest: directory to perform changes in
        executor: Executor to use for filtering
        progress: Will be called with a string describing which anvil file is being filtered
    Returns: set of files found in `src` but not `dest`, relavtive to src
    """
    compare = filecmp.dircmp(src, dest, BACKUP_IGNORE)
    not_present = set()
    compare_stack = [("", compare)]
    filter_tasks = []
    # filter region files
    lazy_progress = (  # only compute relative path if necessary
        _noop if progress is _noop else lambda path: progress(f"filtered {path.relative_to(src)}")
    )
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
            filter_tasks.append(
                executor.submit(
                    _filter_region, src_file, dest_file, common_dir == "region", lazy_progress
                )
            )

    _collect_filter_tasks(filter_tasks)

    return not_present


def _collect_filter_tasks(tasks: list[concurrent.futures.Future[None]]) -> None:
    """Await tasks and group exceptions, cancelling pending tasks if any occur."""
    done, not_done = concurrent.futures.wait(tasks, return_when=concurrent.futures.FIRST_EXCEPTION)
    if not not_done:
        return
    # an exception occured
    for fut in not_done:
        fut.cancel()
    is_base = False
    exceptions = []
    for fut in done:
        if not (exception := fut.exception()):
            continue
        if not isinstance(exception, Exception):
            is_base = True
        exceptions.append(exception)
    # mypy does not get this kind of narrowing
    raise (BaseExceptionGroup if is_base else ExceptionGroup)(  # type: ignore[type-var]
        "Exceptions occured while filtering Regions", exceptions
    )


def _filter_region(
    src_file: Path, dest_file: Path, is_chunk: bool, progress: Callable[[Path], None]
) -> None:
    with RegionFile(src_file) as new_region, RegionFile(dest_file) as old_region:
        unchanged = old_region.filter_diff_defragment(new_region, is_chunk)
        if unchanged:
            dest_file.unlink()
    progress(src_file)


# APPLYING


class _RegionFileCache:
    __slots__ = ("_cached_regions", "_exit_stack")

    def __init__(self) -> None:
        self._cached_regions: dict[Path, RegionFile] = {}
        self._exit_stack = contextlib.ExitStack()

    def __enter__(self) -> Self:
        return self

    def get(self, path: Path) -> RegionFile:
        with contextlib.suppress(KeyError):
            return self._cached_regions[path]
        new_region = self._exit_stack.enter_context(RegionFile(path))
        self._cached_regions[path] = new_region
        return new_region

    def __exit__(self, *_: "Unused") -> None:
        self._cached_regions.clear()
        self._exit_stack.close()


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
