# /// script
# requires-python = ">=3.11"
# dependencies = [
#     "minedelta[standard]",
# ]
#
# [tool.uv.sources]
# minedelta = { path = "../", editable = true }
# ///

"""Script to benchmark MineDelta."""

import argparse
import base64
import dataclasses
import gc
import hashlib
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

from minedelta import backup

if TYPE_CHECKING:
    from _typeshed import StrPath

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

    action_group = run.add_argument_group(
        "actions", "which actions to benchmark. Performs all by default."
    )
    for action in ("create", "restore", "delete"):
        action_group.add_argument(
            f"-{action[:1]}",
            f"--{action}",
            action="append_const",
            const=action,
            dest="action",
            help=f"benchmark {action[:-1]}ing backups",
        )

    manager_group = run.add_argument_group(
        "managers", "which manager to benchmark. Defaults to all if not specified."
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
    args = parser.parse_args()
    _configure_logging(args)
    _configure_tempdir(args)
    args.func(args)


def _capture(args: argparse.Namespace) -> None:
    world: Path = args.world.expanduser()
    capture_directory: Path = args.capture_directory.expanduser()
    existing_count = 0
    if capture_directory.is_dir():
        existing_count = len(_valid_captures(capture_directory))
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


def _valid_captures(capture_dir: Path) -> list[Path]:
    return [c for c in capture_dir.iterdir() if c.name.isdecimal()]


@dataclasses.dataclass(slots=True)
class _BenchmarkResult:
    create_times: list[int] = dataclasses.field(default_factory=list)
    backup_size: int = 0
    restore_times: list[int] = dataclasses.field(default_factory=list)
    delete_times: list[int] = dataclasses.field(default_factory=list)


def _run(args: argparse.Namespace) -> None:
    capture_dir: Path = args.capture_directory.expanduser()
    actions: set[str] = set(args.action or ("create", "restore", "delete"))
    managers: set[type[backup.BaseBackupManager[Any]]] = set(
        args.manager
        or (backup.HardlinkBackupManager, backup.GitBackupManager, backup.DiffBackupManager)
    )
    captures = sorted(_valid_captures(capture_dir), key=lambda p: int(p.stem))
    if not captures:
        raise FileNotFoundError(f"No captures found in {capture_dir}")
    results = {
        manager.__name__: _benchmark_manager(manager, actions, captures, bool(args.verbose))
        for manager in managers
    }
    raw_size = sum(du(capture) for capture in captures)
    for manager, result in results.items():
        print(manager)
        for action in sorted(actions):
            times = getattr(result, action + "_times")
            print(
                f"  {action}: {sum(times) / len(times) / 10**9:.3f}s",
                "(raw):",
                "[" + ", ".join(f"{t:_}" for t in times) + "]",
            )
        print(
            f"  size: {result.backup_size / 2**20:.0f}MiB. ({1 - (result.backup_size / raw_size):.1%} reduction)"
        )


# noinspection PyProtectedMember
def _benchmark_manager(
    manager_type: type[backup.BaseBackupManager[Any]],
    _actions: set[str],
    captures: list[Path],
    verbose: bool,
) -> _BenchmarkResult:
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
        manager = manager_type(world, backup_dir)
        gc.disable()
        try:
            if "create" not in _actions:
                cache = _get_cache(manager_name, capture_dir)
                if cache:
                    logger.debug(f"Found cached manager: {cache.name}")
                    shutil.copytree(cache, backup_dir)
                else:
                    logger.info("Preparing manager (this may take a while...)")
                    old_level = logger.level
                    logger.setLevel(logging.WARNING)
                    _benchmark_create(manager, captures, backup.base._noop)
                    logger.setLevel(old_level)
            else:
                result.create_times = _benchmark_create(manager, captures, progress)
            world.mkdir()
            manager.prepare()
            result.backup_size = du(backup_dir)
            if not cache and cacheable:
                logger.debug(f"Caching {manager_name}")
                _set_cache(manager, capture_dir)
            gc.collect()

            if "restore" in _actions:
                result.restore_times = _benchmark_restore(manager, progress)
            gc.collect()

            if "delete" in _actions:
                result.delete_times = _benchmark_delete(manager, progress)
            gc.collect()
        finally:
            gc.enable()
        if verbose:
            logger.debug(f"Cleaning up (removing {du(tmpdir) // 2**20}MiB)")
    return result


def _benchmark_create(
    manager: backup.BaseBackupManager[Any], captures: list[Path], progress: Callable[[str], Any]
) -> list[int]:
    create_times = []
    for capture in captures:
        shutil.copytree(capture, manager.world)
        logger.info(f"Creating snapshot {capture.name}")
        manager.prepare()
        start = time.perf_counter_ns()
        manager.create_backup(capture.name, progress)
        end = time.perf_counter_ns()
        create_times.append(end - start)
        shutil.rmtree(manager.world)
    return create_times


def _benchmark_restore(
    manager: backup.BaseBackupManager[Any], progress: Callable[[str], Any]
) -> list[int]:
    restore_times = []
    for idx, info in reversed(list(enumerate(manager.list_backups()))):
        logger.info(f"Restoring snapshot {info.desc}")
        id_ = info.id if manager.index_by == "id" else idx

        start = time.perf_counter_ns()
        manager.restore_backup(id_, progress)
        end = time.perf_counter_ns()
        restore_times.append(end - start)
    return restore_times


def _benchmark_delete(
    manager: backup.BaseBackupManager[Any], progress: Callable[[str], Any]
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
        manager.delete_backup(oldest, progress)
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
            "No temporary directory specified. "
            f"Results may be inconsistent unless '{tempfile.gettempdir()}' is on a tmpfs or ramdisk"
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
    hasher = hashlib.sha1(usedforsecurity=False)  # faster than md5 most CPUs
    for capture in _valid_captures(path):
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
