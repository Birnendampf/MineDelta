# /// script
# requires-python = ">=3.11"
# dependencies = [
#     "minedelta[standard]",
#     "rapidnbt>=1.3.5",
# ]
#
# [tool.uv.sources]
# minedelta = { path = "../", editable = true }
# ///

"""Script to benchmark MineDelta. See the README next to this script for more details."""

import argparse
import base64
import concurrent.futures
import contextlib
import dataclasses
import gc
import hashlib
import itertools
import logging
import os
import shutil
import stat
import struct
import tempfile
import time
from collections.abc import Callable, MutableMapping
from pathlib import Path
from typing import TYPE_CHECKING, Any

import rapidnbt

from minedelta import backup, nbt, region

# noinspection PyProtectedMember
from minedelta._thread_scope import MAX_WORKERS, DummyExecutor
from minedelta.backup import DiffBackupManager

if TYPE_CHECKING:
    from typing import Protocol, TypeAlias

    from _typeshed import StrPath

    class _CompareFunc(Protocol):
        def __call__(
            self, left: bytes, right: bytes, exclude_last_update: bool = False
        ) -> bool: ...

    class _CompareFileFunc(Protocol):
        def __call__(
            self,
            left: "StrPath",
            left_comp_type: int,
            right: "StrPath",
            right_comp_type: int,
            exclude_last_update: bool = False,
        ) -> bool: ...

    ConfigurationType: TypeAlias = tuple[
        int, int, int, type[concurrent.futures.Executor], tuple["_CompareFunc", "_CompareFileFunc"]
    ]


logger = logging.getLogger("bench")


def main() -> None:  # noqa: D103
    parser = argparse.ArgumentParser(description="Benchmark minedelta")
    parser.add_argument(
        "-C",
        "--capture-directory",
        type=Path,
        default=Path(__file__).parent / ".captures",
        help="Path to the captures that should be used for benchmarking. "
        'Defaults to ".captures" in the same directory as this script.',
    )
    parser.add_argument(
        "-t",
        "--temp-dir",
        help="Change the temporary directory. Set this to a tmpfs/ramdisk for better consistency.",
    )
    verbosity_group = parser.add_mutually_exclusive_group()
    verbosity_group.add_argument("-q", "--quiet", action="store_true")
    verbosity_group.add_argument("-v", "--verbose", action="count", default=0)

    sub = parser.add_subparsers(title="commands", required=True)

    capture_help = "Capture a snapshot of the world"
    capture = sub.add_parser("capture", help=capture_help, description=capture_help)
    capture.set_defaults(func=_capture, manager=None)
    capture.add_argument("--clean", action="store_true", help="Remove all existing snapshots")
    capture.add_argument("world", type=Path, help="Path of the world to capture")

    run_help = "Run a benchmark"
    run = sub.add_parser("run", help=run_help, description=run_help)
    run.set_defaults(func=_run)

    _add_actions(run)

    manager_group = run.add_argument_group(
        "managers", "Which manager to benchmark. Defaults to all if not specified."
    )
    for manager, short_flag in (
        (backup.HardlinkBackupManager, "--hl"),
        (backup.GitBackupManager, "--gt"),
        (backup.DiffBackupManager, "--df"),
    ):
        manager_group.add_argument(
            f"--{manager.__name__[:-13].lower()}",
            short_flag,
            action="append_const",
            const=manager,
            dest="manager",
            help=((manager.__doc__ or "").splitlines()[0]),
        )

    d_v_help = "Compare DiffBackupManager configurations"
    diff_variations = sub.add_parser(
        "diff-variations",
        help=d_v_help,
        description=d_v_help
        + ". Options can have multiple arguments (like --zstd-lvl 1 2 3), in which case every"
        + " possible combination will be run",
    )
    diff_variations.set_defaults(func=_diff_variations)
    _add_actions(diff_variations)
    zstd_group = diff_variations.add_argument_group("zstandard options")
    zstd_group.add_argument(
        "--zstd-lvl", nargs="+", type=int, help="The zstd compression level to use."
    )
    zstd_group.add_argument(
        "--zstd-threads",
        nargs="+",
        type=int,
        help="How many worker threads to use for compression.",
    )
    executor_group = diff_variations.add_argument_group(
        "Executor options", "Defaults to ThreadPoolExecutor if no executors are specified"
    )
    executor_group.add_argument(
        "-n",
        "--workers",
        nargs="+",
        type=int,
        help="The number of workers to use. Must be above 0. Defaults to the number of CPU cores"
        f" on this system ({MAX_WORKERS})",
    )
    for executor, name, help_text in (
        (concurrent.futures.ThreadPoolExecutor, "thread", "Use ThreadPoolExecutor."),
        (concurrent.futures.ProcessPoolExecutor, "process", "Use ProcessPoolExecutor."),
        (DummyExecutor, "single", "Run single-threaded."),
    ):
        executor_group.add_argument(
            f"--{name}", action="append_const", const=executor, dest="executors", help=help_text
        )

    parser_group = diff_variations.add_argument_group(
        "parser implementation", "Nbtcompare is used by default."
    )
    # noinspection PyProtectedMember
    parser_group.add_argument(
        "--py-compare",
        action="append_const",
        const=(nbt._py_compare_nbt, nbt._py_compare_nbt_files),
        dest="nbt_parsers",
        help="Use the Python fallback NBT parser.",
    )
    parser_group.add_argument(
        "--nbtcompare",
        action="append_const",
        const=(nbt.compare_nbt, nbt.compare_nbt_files),
        dest="nbt_parsers",
        help="Use nbtcompare, a purpose-built NBT parser written in Rust.",
    )
    parser_group.add_argument(
        "--rapidnbt",
        action="append_const",
        const=(_rapidnbt_compare_nbt, _rapidnbt_compare_nbt_files),
        dest="nbt_parsers",
        help="Use rapidnbt, a general NBT parser written in C++.",
    )

    args = parser.parse_args()
    _configure_logging(args)
    args.func(args)


def _add_actions(parser: argparse.ArgumentParser) -> None:
    action_group = parser.add_argument_group(
        "actions", "Which actions to benchmark. Performs all by default."
    )
    for action in ("create", "restore", "delete"):
        action_group.add_argument(
            f"-{action[:1]}",
            f"--{action}",
            action="append_const",
            const=action,
            dest="actions",
            help=f"Benchmark {action[:-1]}ing backups",
        )


def _capture(args: argparse.Namespace) -> None:
    world: Path = args.world.expanduser()
    capture_directory: Path = args.capture_directory.expanduser()
    existing_count = 0
    with contextlib.suppress(FileNotFoundError):
        existing_count = len(_sorted_captures(capture_directory))
        logger.debug(f"Found {existing_count} existing snapshots")
    if args.clean:
        logger.info("Cleaning up old snapshots")
        shutil.rmtree(capture_directory)
        existing_count = 0
    capture_directory.mkdir(parents=True, exist_ok=True)
    new_capture = capture_directory / str(existing_count)
    logger.info("Creating snapshot...")
    shutil.copytree(world, new_capture)
    if logger.isEnabledFor(logging.DEBUG):
        logger.debug(f"Finished ({du(new_capture) // 2**20}MiB)")


def _sorted_captures(capture_dir: Path) -> list[Path]:
    captures = [c for c in capture_dir.iterdir() if c.name.isdecimal()]
    captures.sort(key=lambda p: int(p.stem))
    if not captures:
        raise FileNotFoundError(f"No captures found in {capture_dir}")
    return captures


@dataclasses.dataclass(slots=True)
class _BenchmarkResult:
    create_times: list[int] = dataclasses.field(default_factory=list)
    backup_size: int = 0
    restore_times: list[int] = dataclasses.field(default_factory=list)
    delete_times: list[int] = dataclasses.field(default_factory=list)


def _run(args: argparse.Namespace) -> None:
    _configure_tempdir(args)
    captures = _sorted_captures(args.capture_directory)
    actions: set[str] = set(args.actions or ("create", "restore", "delete"))
    managers: set[type[backup.BaseBackupManager[Any]]] = set(
        args.manager
        or (backup.HardlinkBackupManager, backup.GitBackupManager, backup.DiffBackupManager)
    )
    results = {
        manager.__name__: _benchmark_manager(manager, actions, captures, bool(args.verbose))
        for manager in managers
    }
    _print_results(actions, captures, results)


def _print_results(
    actions: set[str], captures: list[Path], results: dict[str, _BenchmarkResult]
) -> None:
    raw_size = sum(du(capture) for capture in captures)
    for manager, result in results.items():
        print(manager)
        for action in sorted(actions):
            times: list[int] = getattr(result, action + "_times")
            print(
                f"  {action}: {sum(times) / len(times) / 10**9:.3f}s",
                "(raw data: [" + ", ".join(f"{t:_}" for t in times) + "])",
            )
        print(
            f"  size: {result.backup_size / 2**20:.0f}MiB. "
            f"({1 - (result.backup_size / raw_size):.1%} reduction)"
        )


def _diff_variations(args: argparse.Namespace) -> None:
    _configure_tempdir(args)
    captures = _sorted_captures(args.capture_directory)
    actions: set[str] = set(args.actions or ("create", "restore", "delete"))
    default_zstd_level = 0
    default_zstd_threads = 0
    default_workers = MAX_WORKERS
    default_executor = concurrent.futures.ThreadPoolExecutor
    default_parser = (nbt.compare_nbt, nbt.compare_nbt_files)
    # grouped together:
    # - zstd options + worker count
    # - executor + compare parsers
    # noinspection PyTypeChecker
    unsorted_configs: set[ConfigurationType] = {
        (*config, default_executor, default_parser)
        for config in itertools.product(
            set(args.zstd_lvl or (default_zstd_level,)),
            set(args.zstd_threads or (default_zstd_threads,)),
            set(args.workers or (default_workers,)),
        )
    } | {
        (default_zstd_level, default_zstd_threads, default_workers, *config)
        for config in itertools.product(
            set(args.executors or (default_executor,)), set(args.nbt_parsers or (default_parser,))
        )
    }
    configurations = sorted(
        unsorted_configs, key=lambda c: (*c[:3], c[3].__name__, _parse_func_to_str(c[4][0]))
    )
    logger.debug("\n  ".join(("Configurations:", *(_conf_to_str(c) for c in configurations))))
    results = {}
    for config in configurations:
        zstd_lvl, zstd_threads, workers, ex_type, parser = config
        region.compare_nbt, region.compare_nbt_files = parser  # type: ignore[attr-defined]
        str_config = _conf_to_str(config)
        logger.info(f"Config: {str_config}")
        # noinspection PyArgumentList
        with (
            ex_type(max_workers=workers)
            if issubclass(
                ex_type,
                concurrent.futures.ThreadPoolExecutor | concurrent.futures.ProcessPoolExecutor,
            )
            else ex_type()
        ) as executor:
            results[str_config] = _benchmark_manager(
                DiffBackupManager,
                actions,
                captures,
                bool(args.verbose),
                {"zstd_worker_count": zstd_threads, "compression_level": zstd_lvl},
                {"executor": executor},
            )
    _print_results(actions, captures, results)


def _conf_to_str(configuration: "ConfigurationType") -> str:
    zstd_lvl, zstd_threads, workers, executor, (parse_func, _) = configuration
    func_str = _parse_func_to_str(parse_func)
    parse_func_str = func_str
    executor_str = "Single" if issubclass(executor, DummyExecutor) else executor.__name__
    return (
        f"{zstd_lvl = :2}, {zstd_threads = :2}, {workers = :2}, "
        f"executor = {executor_str + ',':21}"
        f"parse_func = {parse_func_str}"
    )


def _parse_func_to_str(parse_func: "_CompareFunc") -> str:
    # noinspection PyProtectedMember
    return (
        "py_compare"
        if parse_func is nbt._py_compare_nbt
        else "nbtcompare"
        if parse_func is nbt.compare_nbt
        else "rapidnbt"
    )


def _rapidnbt_compare_nbt(left: bytes, right: bytes, exclude_last_update: bool = False) -> bool:
    left_compound = rapidnbt.nbtio.loads(left, rapidnbt.NbtFileFormat.BIG_ENDIAN)
    right_compound = rapidnbt.nbtio.loads(right, rapidnbt.NbtFileFormat.BIG_ENDIAN)
    return _do_compare(left_compound, right_compound, exclude_last_update)


def _rapidnbt_compare_nbt_files(
    left: "StrPath",
    _left_comp_type: int,
    right: "StrPath",
    _right_comp_type: int,
    exclude_last_update: bool = False,
) -> bool:
    left_compound = rapidnbt.nbtio.load(left, rapidnbt.NbtFileFormat.BIG_ENDIAN)  # type: ignore[arg-type]
    right_compound = rapidnbt.nbtio.load(right, rapidnbt.NbtFileFormat.BIG_ENDIAN)  # type: ignore[arg-type]
    return _do_compare(left_compound, right_compound, exclude_last_update)


def _do_compare(
    left_compound: rapidnbt.CompoundTag | None,
    right_compound: rapidnbt.CompoundTag | None,
    exclude_last_update: bool,
) -> bool:
    if left_compound is None or right_compound is None:
        raise RuntimeError("parse failure")
    if exclude_last_update:
        del left_compound["LastUpdate"], right_compound["LastUpdate"]
    return left_compound == right_compound


# noinspection PyProtectedMember,PyTypeChecker
def _benchmark_manager(  # noqa: PLR0913
    manager_type: type[backup.BaseBackupManager[Any]],
    actions: set[str],
    captures: list[Path],
    verbose: bool,
    constructor_args: dict[str, Any] | None = None,
    action_args: dict[str, Any] | None = None,
) -> _BenchmarkResult:
    constructor_args = constructor_args or {}
    action_args = action_args or {}
    result = _BenchmarkResult()
    manager_name = manager_type.__name__
    progress = _ProgressAdapter(manager_name[:-13]).debug if verbose else backup.base._noop
    cache = None
    capture_dir = captures[0].parent
    # hardlinks are not preserved in copies so it wastes a LOT of disk space
    # + HardlinkBackupManager is fast enough to not need caching
    cacheable = not issubclass(manager_type, backup.HardlinkBackupManager)
    if issubclass(manager_type, backup.GitBackupManager):
        logger.debug("Disabling git auto gc")
        os.environ["GIT_AUTO_GC"] = "0"

    logger.info(f"Benchmarking {manager_name}")
    with tempfile.TemporaryDirectory() as tmpdir:
        world = Path(tmpdir, "world")
        backup_dir = Path(tmpdir, "backup")
        # noinspection PyArgumentList
        manager = manager_type(world, backup_dir, **constructor_args)
        gc.disable()
        try:
            if "create" not in actions:
                cache = _get_cache(manager_name, capture_dir)
                if cache:
                    logger.debug(f"Found cached manager: {cache.name}")
                    shutil.copytree(cache, backup_dir)
                else:
                    logger.info("Preparing manager (this may take a while...)")
                    old_level = logger.level
                    logger.setLevel(logging.WARNING)
                    _benchmark_create(manager, captures, backup.base._noop, action_args)
                    logger.setLevel(old_level)
            else:
                result.create_times = _benchmark_create(manager, captures, progress, action_args)
            world.mkdir()
            manager.prepare()
            result.backup_size = du(backup_dir)
            if not cache and cacheable:
                logger.debug(f"Caching {manager_name}")
                _set_cache(manager, capture_dir)
            gc.collect()

            if "restore" in actions:
                result.restore_times = _benchmark_restore(manager, progress, action_args)
            gc.collect()

            if "delete" in actions:
                result.delete_times = _benchmark_delete(manager, progress, action_args)
            gc.collect()
        finally:
            gc.enable()
        if verbose:
            logger.debug(f"Cleaning up (removing {du(tmpdir) // 2**20}MiB)")
    return result


def _benchmark_create(
    manager: backup.BaseBackupManager[Any],
    captures: list[Path],
    progress: Callable[[str], Any],
    kwargs: dict[str, Any],
) -> list[int]:
    create_times = []
    for capture in captures:
        logger.info(f"Creating backup for snapshot {capture.name}")
        shutil.copytree(capture, manager.world)
        manager.prepare()
        start = time.perf_counter_ns()
        # noinspection PyArgumentList
        manager.create_backup(capture.name, progress, **kwargs)
        end = time.perf_counter_ns()
        create_times.append(end - start)
        shutil.rmtree(manager.world)
    return create_times


def _benchmark_restore(
    manager: backup.BaseBackupManager[Any], progress: Callable[[str], Any], kwargs: dict[str, Any]
) -> list[int]:
    restore_times = []
    for idx, info in reversed(list(enumerate(manager.list_backups()))):
        logger.info(f"Restoring snapshot {info.desc}")
        id_ = info.id if manager.index_by == "id" else idx

        start = time.perf_counter_ns()
        # noinspection PyArgumentList
        manager.restore_backup(id_, progress, **kwargs)
        end = time.perf_counter_ns()
        restore_times.append(end - start)
    return restore_times


def _benchmark_delete(
    manager: backup.BaseBackupManager[Any], progress: Callable[[str], Any], kwargs: dict[str, Any]
) -> list[int]:
    backups = manager.list_backups()
    oldest = backups[-1].id if manager.index_by == "id" else (len(backups) - 1)
    delete_times = []
    if len(backups) > 1:
        need_state_copy = len(backups) <= 2 and isinstance(manager, backup.DiffBackupManager)
        backup_dir = manager.backup_dir
        new_dir = backup_dir.parent / "old_backups"

        if need_state_copy:
            # deleting the newest is only ever affected by the second-newest backup
            # so if there are 3 backups, there will still be 2 when deleting newest,
            # avoiding any fast paths
            logger.debug("Copying manager state")
            shutil.copytree(backup_dir, new_dir)
        logger.info("Deleting oldest")
        start = time.perf_counter_ns()
        # noinspection PyTypeChecker
        manager.delete_backup(oldest, progress, **kwargs)
        end = time.perf_counter_ns()
        delete_times.append(end - start)
        if need_state_copy:
            logger.debug("Restoring manager state")
            shutil.rmtree(backup_dir)
            shutil.move(new_dir, backup_dir)
        backups = manager.list_backups()
        newest = backups[0].id if manager.index_by == "id" else 0
        logger.info("Deleting newest")
    else:
        logger.info("Deleting backup")
        newest = oldest
    start = time.perf_counter_ns()
    manager.delete_backup(newest, progress)
    end = time.perf_counter_ns()
    delete_times.append(end - start)
    return delete_times


def _configure_logging(args: argparse.Namespace) -> None:
    if args.quiet:
        level = logging.WARNING
    elif args.verbose > 1:
        level = logging.DEBUG
    else:
        level = logging.INFO
    logging.basicConfig(
        format="[%(asctime)s.%(msecs)03d] %(levelname)s:%(name)s:%(message)s",
        datefmt="%H:%M:%S",
        level=level,
    )
    if args.verbose == 1:
        logger.setLevel(level=logging.DEBUG)


class _ProgressAdapter(logging.LoggerAdapter[logging.Logger]):
    def __init__(self, manager_name: str):
        self.manager_name = manager_name
        super().__init__(logger)

    def process(
        self,
        msg: Any,  # noqa: ANN401
        kwargs: MutableMapping[str, Any],
    ) -> tuple[str, MutableMapping[str, Any]]:
        return f"({self.manager_name}) {msg}", kwargs


def _configure_tempdir(args: argparse.Namespace) -> None:
    if args.temp_dir:
        try:
            with tempfile.TemporaryFile(dir=args.temp_dir) as temp:
                temp.write(b":3")
        except OSError as e:
            msg = f"'{args.temp_dir}' is not a valid temporary directory"
            raise type(e)(msg) from None
        tempfile.tempdir = args.temp_dir
    elif os.name == "nt" or not Path(tempfile.gettempdir()).is_mount():
        logger.warning(
            "No temporary directory specified! Results may be inconsistent unless "
            f"'{tempfile.gettempdir()}' is on a tmpfs or ramdisk."
        )


def du(path: "StrPath") -> int:
    """Similar to the du command.

    Hardlinks are deduplicated, directories' size on disk is counted (not 0),
    otherwise apparent size is used
    """
    seen_inodes = set()
    total_size = 0
    scan_stack = [path]
    while scan_stack:
        with os.scandir(scan_stack.pop()) as it:
            for entry in it:
                is_dir = entry.is_dir(follow_symlinks=False)
                if is_dir:
                    scan_stack.append(entry)
                elif entry.inode() in seen_inodes:
                    continue
                st = entry.stat(follow_symlinks=False)
                if not (is_dir or st.st_nlink == 1):
                    seen_inodes.add(entry.inode())
                total_size += st.st_size
    return total_size


# file type, size and mtime
_fingerprint_struct = struct.Struct(">HQQ")
_FINGERPRINT = ""


def get_fingerprint(path: Path) -> str:
    """Recursively hash the path, type, size and mtime of all captures."""
    hasher = hashlib.sha1(usedforsecurity=False)  # faster than md5 on most CPUs
    for capture in _sorted_captures(path):
        for root, dirs, files in os.walk(capture):
            dirs.sort()
            rel_root = os.path.relpath(root, capture)
            for file_name in sorted(files):
                # ruff: disable[PTH118, PTH116] hot code path, no pathlib here
                file = os.path.join(root, file_name)
                hasher.update(os.path.join(rel_root, file_name).encode())
                st = os.stat(file)
                hasher.update(
                    _fingerprint_struct.pack(stat.S_IFMT(st.st_mode), st.st_size, st.st_mtime_ns)
                )
                # ruff: enable[PTH118, PTH116]
    return base64.urlsafe_b64encode(hasher.digest())[:-1].decode("ascii")


def _get_cache(manager_name: str, capture_dir: Path) -> Path | None:
    global _FINGERPRINT  # noqa: PLW0603
    cache = capture_dir / ".cache"
    if not cache.is_dir():
        return None
    candidates = list(cache.glob(manager_name + "_*"))
    if not candidates:
        return None
    if not _FINGERPRINT:
        _FINGERPRINT = get_fingerprint(capture_dir)
    chosen = None
    for candidate in candidates:
        if candidate.name == f"{manager_name}_{_FINGERPRINT}":
            chosen = candidate
        else:
            shutil.rmtree(candidate)
    return chosen


def _set_cache(manager: backup.BaseBackupManager[Any], capture_dir: Path) -> None:
    cache_dir = capture_dir / ".cache"
    cache_dir.mkdir(parents=True, exist_ok=True)
    fingerprint = get_fingerprint(capture_dir)
    new_cache_entry = cache_dir / f"{type(manager).__name__}_{fingerprint}"
    backup.base.delete_file_or_dir(new_cache_entry)
    shutil.copytree(manager.backup_dir, new_cache_entry)


if __name__ == "__main__":
    main()
