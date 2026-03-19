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
import dataclasses
import gc
import logging
import os
import shutil
import tempfile
import time
from pathlib import Path
from typing import TYPE_CHECKING, Any

from minedelta import backup

if TYPE_CHECKING:
    from types import TracebackType

    from _typeshed import StrPath, Unused

logger = logging.getLogger("bench")


def main() -> None:  # noqa: D103
    parser = argparse.ArgumentParser(description="Benchmark minedelta")
    parser.add_argument(
        "-c",
        "--capture-directory",
        type=Path,
        default=Path(__file__).parent / ".captures",
        help="Path to the captures that should be used for benchmarking. "
        'Defaults to ".captures" in the same directory as this script.',
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
    for manager in (
        backup.HardlinkBackupManager,
        backup.GitBackupManager,
        backup.DiffBackupManager,
    ):
        manager_group.add_argument(
            f"--{manager.__name__[:-13].lower()}",
            action="append_const",
            const=manager,
            dest="manager",
            help=((manager.__doc__ or "").splitlines()[0]),
        )
    args = parser.parse_args()
    _set_verbosity(args)
    args.func(args)


def _capture(args: argparse.Namespace) -> None:
    world: Path = args.world.expanduser()
    capture_directory: Path = args.capture_directory.expanduser()
    existing_count = 0
    if capture_directory.is_dir():
        existing_count = len(list(capture_directory.iterdir()))
        logger.debug(f"Found {existing_count} existing snapshots")
    if args.clean:
        logger.info("Cleaning up old snapshots")
        shutil.rmtree(capture_directory, onerror=_rmtree_on_error)
        existing_count = 0
    capture_directory.mkdir(parents=True, exist_ok=True)
    new_capture = capture_directory / f"{existing_count}"
    logger.info("Creating snapshot...")
    shutil.copytree(world, new_capture)
    if logger.isEnabledFor(logging.DEBUG):
        logger.debug(f"Finished ({_du(new_capture) // 2**20}MiB)")


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
    results = {
        manager.__name__: _benchmark_manager(manager, actions, capture_dir) for manager in managers
    }
    for manager, result in results.items():
        print(manager)
        print(f"\taverage: {sum(result.create_times) / len(result.create_times) / 10**9:.3f}s")
        print(f"\tsize: {result.backup_size / 2**20:.0f}MiB")


def _benchmark_manager(
    manager_type: type[backup.BaseBackupManager[Any]], _actions: set[str], capture_dir: Path
) -> _BenchmarkResult:
    result = _BenchmarkResult()
    with tempfile.TemporaryDirectory() as tmpdir:
        logger.info(f"Benchmarking {manager_type.__name__}")
        world = Path(tmpdir, "world")
        world.mkdir()
        backup_dir = Path(tmpdir, "backup")
        manager = manager_type(world, backup_dir)
        gc.disable()
        try:
            create_times = _benchmark_creation(manager, capture_dir)
            if "create" in _actions:
                result.create_times = create_times
                # noinspection PyProtectedMember
                result.backup_size = _du(manager._backup_dir)
            gc.collect()
        finally:
            gc.enable()
    return result


# noinspection PyProtectedMember
def _benchmark_creation(manager: backup.BaseBackupManager[Any], capture_dir: Path) -> list[int]:
    create_times = []
    orig_world = manager._world
    progress_func = logger.debug if logger.isEnabledFor(logging.DEBUG) else backup.base._noop
    for capture in sorted(capture_dir.iterdir(), key=lambda p: int(p.stem)):
        logger.info(f"creating snapshot {capture.name}")
        manager._world = capture
        manager.prepare()
        try:
            start = time.perf_counter_ns()
            manager.create_backup(capture.name, progress_func)
            end = time.perf_counter_ns()
            create_times.append(end - start)
        finally:
            if isinstance(manager, backup.GitBackupManager):
                (capture / ".git").unlink()
    manager._world = orig_world
    manager.prepare()
    return create_times


def _set_verbosity(args: argparse.Namespace) -> None:
    if args.quiet:
        level = logging.WARNING
    elif args.verbose:
        level = logging.DEBUG
    else:
        level = logging.INFO
    logging.basicConfig(level=level)
    logging.getLogger("dulwich").setLevel(logging.INFO)


def _rmtree_on_error(
    _function: "Unused",
    path: str,
    exc_info: tuple[type[BaseException], BaseException, "TracebackType"],
) -> None:
    logger.warning(f"failed to remove {path}", exc_info=exc_info)


def _du(path: Path) -> int:
    """Similar to the du command.

    Hardlinks are deduplicated, directories' size on disk is counted (not 0),
    otherwise apparent size is used
    """
    seen_inodes = set()
    total_size: int = 0
    scan_stack: list[StrPath] = [path]
    while scan_stack:
        with os.scandir(scan_stack.pop()) as it:
            for entry in it:
                if entry.inode() in seen_inodes:
                    continue
                seen_inodes.add(entry.inode())
                total_size += entry.stat(follow_symlinks=False).st_size
                if entry.is_dir(follow_symlinks=False):
                    scan_stack.append(entry)
    return total_size


if __name__ == "__main__":
    main()
