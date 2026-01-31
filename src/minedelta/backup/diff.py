"""Create backups by storing only changed chunks in region files for previous Backups.

For more details, see `DiffBackupManager`.
"""

import concurrent.futures
import contextlib
import datetime
import filecmp
import os
import shutil
import sys
import tarfile
import tempfile
import uuid
from collections.abc import Callable, Iterator
from pathlib import Path
from typing import TYPE_CHECKING, Annotated, Final, cast

import msgspec

from minedelta.region import RegionFile

from .base import (
    BACKUP_IGNORE,
    BACKUP_IGNORE_FROZENSET,
    BackupInfo,
    BaseBackupManager,
    _delete_file_or_dir,
    _noop,
)

if sys.version_info >= (3, 12):
    from typing import override
else:
    from typing_extensions import override

if TYPE_CHECKING:
    from _typeshed import StrPath


__all__ = ["MAX_WORKERS", "DiffBackupManager", "_convert_backup_data_to_json"]

MCA_FOLDERS: Final = ("region", "entities", "poi")

_cpu_count: int | None = None
try:
    _cpu_count = os.process_cpu_count()  # type: ignore[attr-defined]
except AttributeError:
    pass
if _cpu_count is None:
    # sys.version_info < 3.13
    try:
        _cpu_count = len(os.sched_getaffinity(0))
    except AttributeError:
        # not UNIX
        pass
if _cpu_count is None:
    _cpu_count = os.cpu_count()

MAX_WORKERS = _cpu_count or 1
del _cpu_count

# note that ThreadPool is the only executor that currently works, due to unshareable objects
# TODO: maybe support more executors if needed
_DefaultExecutor = concurrent.futures.ThreadPoolExecutor


class BackupData(msgspec.Struct, omit_defaults=True):
    timestamp: Annotated[datetime.datetime, msgspec.Meta(tz=True)]
    id: uuid.UUID
    not_present: set[str]
    desc: str | None = None

    @property
    def name(self) -> str:
        return f"{self.id}.tar.gz"


_BackupDataENCODER: Final = msgspec.msgpack.Encoder(uuid_format="bytes")
_BackupDataDECODER: Final = msgspec.msgpack.Decoder(list[BackupData])


@contextlib.contextmanager
def _extract_to_temp(archive: "StrPath") -> Iterator[str]:
    """Extract archive into a temporary directory and return it."""
    with tempfile.TemporaryDirectory() as extracted:
        with tarfile.open(archive, "r:gz") as tar:
            tar.extractall(extracted, filter="data")
        yield extracted


@contextlib.contextmanager
def _extract_compress(archive: "StrPath") -> Iterator[str]:
    """Extract archive into a temporary directory. recompress when exiting context.

    Args:
        archive: Archive to extract
    Returns: Extracted archive
    """
    with _extract_to_temp(archive) as extracted:
        yield extracted
        with tarfile.open(archive, "w:gz") as tar:
            tar.add(extracted, "")


class DiffBackupManager(BaseBackupManager[int]):
    """Manager to create backups that only store changed chunks.

    The newest backup is essentially complete copy, every previous n-th backup stores the changes needed to
    turn the (n-1)th backup into itself. Illustration:

    ===  ===========  =================
    idx  files        diff
    ===  ===========  =================
    0    a0    c1 d2
    1       b0 c0 d1  -a b0 c1->0 d2->1
    2             d0     -b -c    d1->0
    ===  ===========  =================

    Some methods in this module take an additional `executor` parameter. This allows a
    ThreadPoolexecutor to be reused between calls. if not specified, a new one with the number of
    workers equal to the number of available cpu cores will be used
    """

    __slots__ = "_backups_data_path"
    index_by = "idx"

    @override
    def __init__(self, save: "StrPath", backup_dir: Path):
        super().__init__(save, backup_dir)
        self._backups_data_path: Final = backup_dir / "backups.dat"

    @override
    def create_backup(
        self,
        description: str | None = None,
        progress: Callable[[str], None] = _noop,
        executor: concurrent.futures.ThreadPoolExecutor | None = None,
    ) -> BackupInfo:
        # TODO: there could be a race condition if the world is modified while a backup is created
        #  /save-off needs to be run beforehand
        timestamp = datetime.datetime.now(datetime.UTC).replace(microsecond=0)
        id_ = uuid.uuid4()
        progress(f'creating backup "{id_}"')
        new_backup = BackupData(timestamp, id_, set(), description)
        try:
            backups_data = self._load_backups_data()
            previous: BackupData | None = backups_data[0]
        except (FileNotFoundError, IndexError):
            backups_data = []
            previous = None
        progress("compressing world")
        if not previous:
            self._compress_world(new_backup.name)
        else:
            previous_backup_path = self._backup_dir / previous.name
            # noinspection PyAbstractClass
            with contextlib.ExitStack() as stack:
                if MAX_WORKERS > 1 or executor:  # compress current while filtering prev
                    if not executor:
                        executor = stack.enter_context(_DefaultExecutor(MAX_WORKERS))
                    backup_fut: concurrent.futures.Future[None] | None = executor.submit(
                        self._compress_world, new_backup.name
                    )
                    prev_world = stack.enter_context(_extract_to_temp(previous_backup_path))
                else:
                    executor = None
                    backup_fut = None
                    self._compress_world(new_backup.name)
                    prev_world = stack.enter_context(_extract_compress(previous_backup_path))

                progress(f'turning "{previous.id}" into diff')
                previous.not_present = _filter_diff(
                    src=self._world, dest=prev_world, progress=progress, executor=executor
                )
                progress(f'recompressing "{previous.id}"')
                if backup_fut:
                    new_previous = self._backup_dir / ("new_" + previous.name)
                    with tarfile.open(new_previous, "w:gz") as tar:
                        tar.add(prev_world, "")
                    # make sure backup creation went well before overwriting previous
                    backup_fut.result()
                    new_previous.replace(previous_backup_path)

        backups_data.insert(0, new_backup)
        self._write_backups_data(backups_data)
        return BackupInfo(timestamp, str(id_), description)

    def _compress_world(self, name: str) -> None:
        with tarfile.open(self._backup_dir / name, "x:gz") as tar:
            tar.add(self._world, "", filter=_backup_filter)

    @override
    def restore_backup(
        self,
        id_: int,
        progress: Callable[[str], None] = _noop,
        executor: concurrent.futures.ThreadPoolExecutor | None = None,
    ) -> None:
        backups_data = self._load_backups_data_validate_idx(id_)
        progress(f'restoring backup "{backups_data[id_].id}"')
        backups_slice = backups_data[0 : id_ + 1]
        # it's not actually abstract
        # noinspection PyAbstractClass
        with contextlib.ExitStack() as stack:
            if MAX_WORKERS > 1 or executor:
                if not executor:
                    executor = stack.enter_context(_DefaultExecutor(MAX_WORKERS))
                # mypy insists map is generic, but it is apparently not
                # noinspection PyUnresolvedReferences
                map_meth = cast("type[map[str]]", cast(object, executor.map))
            else:
                map_meth = map
            # extract in parallel. the GIL is released when decompressing
            extracted_backups = map_meth(
                stack.enter_context,
                (
                    _extract_to_temp(self._backup_dir / backup_data.name)
                    for backup_data in backups_slice
                ),
            )
            backup_save = next(extracted_backups)
            for i, (backup_data, extracted) in enumerate(
                zip(backups_slice[1:], extracted_backups, strict=True), 1
            ):
                progress(f'[{i}/{len(backups_slice)}] applying "{backup_data.id}"')
                _apply_diff(dest=backup_save, src=extracted)
                _clear_not_present(backup_save, backup_data)
            progress("deleting current world")
            self._clear_world()
            progress("restoring backup")
            shutil.copytree(backup_save, self._world, dirs_exist_ok=True)

    @override
    def delete_backup(
        self,
        id_: int,
        progress: Callable[[str], None] = _noop,
        executor: concurrent.futures.ThreadPoolExecutor | None = None,
    ) -> None:
        backups_data = self._load_backups_data_validate_idx(id_)
        if id_ == len(backups_data) - 1:  # deleting oldest is easy
            data_chosen = backups_data.pop()
            progress(f'deleting oldest backup "{data_chosen.id}"')
            (self._backup_dir / data_chosen.name).unlink()
            self._write_backups_data(backups_data)
            return
        data_chosen = backups_data[id_]
        data_older = backups_data.pop(id_ + 1)
        data_chosen.timestamp, data_chosen.desc = data_older.timestamp, data_older.desc

        progress(f'merging "{data_older.id}" into "{data_chosen.id}"')
        older_archive = self._backup_dir / data_older.name
        # noinspection PyAbstractClass
        with contextlib.ExitStack() as stack:
            if MAX_WORKERS > 1 or executor:  # extract in parallel
                if not executor:
                    executor = stack.enter_context(_DefaultExecutor(MAX_WORKERS))
                older_fut = executor.submit(stack.enter_context, _extract_to_temp(older_archive))
                chosen = stack.enter_context(_extract_compress(self._backup_dir / data_chosen.name))
                older = older_fut.result()
            else:
                older = stack.enter_context(_extract_to_temp(older_archive))
                chosen = stack.enter_context(_extract_compress(self._backup_dir / data_chosen.name))

            _clear_not_present(chosen, data_older)
            _apply_diff(src=older, dest=chosen, defragment=True)
            # handle the following situation (1 being deleted):
            # idx | files | diff | new diff
            # 0   | a0    |      |
            # 1   |       | -a   | a0
            # 2   | a0    | a0   | (deleted)
            for file in data_chosen.not_present.copy():
                if Path(older, file).exists():
                    data_chosen.not_present.discard(file)
            progress(f'recompressing "{data_chosen.id}"')
        if id_:
            # handle the following situation (1 being deleted):
            # idx | files       | diff              | new diff
            # 0   | a0    c1 d2 |                   |
            # 1   |    b0 c0 d1 | -a b0 c1->0 d2->1 | -a -b -c d2->0
            # 2   |          d0 |    -b -c    d1->0 | (deleted)
            # note that -b is contained in the new diff, because we do not know that b0 was
            # deleted again at idx 0
            data_chosen.not_present.update(data_older.not_present)
        self._write_backups_data(backups_data)
        older_archive.unlink()

    @override
    def list_backups(self) -> list[BackupInfo]:
        backups_data = self._load_backups_data()
        return [BackupInfo(data.timestamp, str(data.id), data.desc) for data in backups_data]

    def _load_backups_data(self) -> list[BackupData]:
        try:
            return _BackupDataDECODER.decode(self._backups_data_path.read_bytes())
        except FileNotFoundError:
            return msgspec.json.decode(
                self._backups_data_path.with_suffix(".json").read_bytes(), type=list[BackupData]
            )

    def _write_backups_data(self, backups_data: list[BackupData]) -> None:
        self._backups_data_path.write_bytes(_BackupDataENCODER.encode(backups_data))

    def _load_backups_data_validate_idx(self, idx: int) -> list[BackupData]:
        if idx < 0:
            raise IndexError("index must be >= 0")
        backup_infos = self._load_backups_data()
        if idx >= len(backup_infos):
            raise IndexError(f"no backup found with index {idx}")
        return backup_infos


def _backup_filter(tarinfo: tarfile.TarInfo) -> tarfile.TarInfo | None:
    """Filter for creating tarfiles that drops files from BACKUP_IGNORE."""
    if os.path.basename(tarinfo.name) in BACKUP_IGNORE_FROZENSET:
        return None
    return tarinfo


def _clear_not_present(path: "StrPath", info: BackupData) -> None:
    for file in info.not_present:
        file_path = Path(path, file)
        _delete_file_or_dir(file_path)


def _filter_diff(
    *,
    src: "StrPath",
    dest: "StrPath",
    progress: Callable[[str], None] = _noop,
    executor: concurrent.futures.ThreadPoolExecutor | None = None,
) -> set[str]:
    """Delete files and chunks from `dest` in common with `src`. `src` is not altered.

    Files and directories from BACKUP_IGNORE are skipped.

    Args:
        src: directory to compare against
        dest: directory to perform changes in
        progress: Will be called with a string describing which anvil file is being filtered
        executor: Executor to use for filtering or None for single threaded operation
    Returns: list of files found in `src` but not `dest`
    """
    compare = filecmp.dircmp(src, dest, BACKUP_IGNORE)
    not_present = set()
    compare_stack = [("", compare)]
    to_be_filtered: list[tuple[Path, Path, bool]] = []
    while compare_stack:
        common_dir, compare = compare_stack.pop()
        compare_stack.extend(compare.subdirs.items())
        for file in compare.left_only:
            # documentation warns about this on Windows, but it's fine since Python 3.6 (PEP529)
            not_present.add(Path(compare.left, file).relative_to(src).as_posix())
        for file in compare.same_files:
            Path(compare.right, file).unlink()
        if common_dir not in MCA_FOLDERS:
            continue
        for file in compare.diff_files:
            src_file = Path(compare.left, file)
            dest_file = Path(compare.right, file)
            if src_file.stat().st_size == 0:
                continue
            if dest_file.stat().st_size == 0:
                dest_file.unlink()
                not_present.add(src_file.relative_to(src).as_posix())
                continue
            to_be_filtered.append((src_file, dest_file, common_dir == "region"))
    if executor:
        tasks = [
            executor.submit(_filter_region, src_file, dest_file, is_chunk)
            for src_file, dest_file, is_chunk in to_be_filtered
        ]
        if progress is not _noop:
            for task in concurrent.futures.as_completed(tasks):
                progress(f"filtered {task.result().relative_to(src)}")
    else:
        for src_file, dest_file, is_chunk in to_be_filtered:
            _filter_region(src_file, dest_file, is_chunk)
            progress(f"filtered {src_file.relative_to(src)}")

    return not_present


def _filter_region(src_file: Path, dest_file: Path, is_chunk: bool) -> Path:
    # noinspection PyTypeChecker
    with RegionFile.open(src_file) as new_region, RegionFile.open(dest_file) as old_region:
        unchanged = old_region.filter_diff_defragment(new_region, is_chunk)
        if unchanged:
            dest_file.unlink()
    return src_file


def _apply_diff(*, src: "StrPath", dest: "StrPath", defragment: bool = False) -> None:
    for dirpath, dirs, files in os.walk(src):
        dest_dirpath = dest / Path(dirpath).relative_to(src)
        for dirname in dirs:
            (dest_dirpath / dirname).mkdir(exist_ok=True)
        for file in files:
            src_file = Path(dirpath, file)
            dest_file = dest_dirpath / file
            if _should_apply_diff(dest_file, src_file):
                # STUPID IDIOT SOFTWARE
                # noinspection PyTypeChecker
                with (
                    RegionFile.open(dest_file) as dest_region,
                    RegionFile.open(src_file) as src_region,
                ):
                    dest_region.apply_diff(src_region, defragment)
            else:
                shutil.copy2(src_file, dest_file)


def _should_apply_diff(dest_file: Path, src_file: Path) -> bool:
    if src_file.suffix != ".mca" or not src_file.stat().st_size:
        return False
    try:
        if not dest_file.stat().st_size:
            return False
    except (OSError, ValueError):
        return False

    return True


def _convert_backup_data_to_json(backup_data: Path) -> None:
    decoded = _BackupDataDECODER.decode(backup_data.read_bytes())
    backup_data.with_suffix(".json").write_bytes(msgspec.json.format(msgspec.json.encode(decoded)))
