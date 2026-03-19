# /// script
# requires-python = ">=3.11"
# dependencies = [
#     "minedelta[standard]",
#     "platformdirs>=4.9.4",
# ]
#
# [tool.uv.sources]
# minedelta = { path = "../", editable = true }
# ///

"""Script to benchmark MineDelta."""

import argparse
import logging
import shutil
import sys
from pathlib import Path
from typing import TYPE_CHECKING

# noinspection PyProtectedMember
from minedelta._thread_scope import MAX_WORKERS
from minedelta.backup import diff

if sys.version_info >= (3, 14):
    import tarfile
else:
    from backports.zstd import tarfile

if TYPE_CHECKING:
    from types import TracebackType

    from _typeshed import Unused

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

    sub = parser.add_subparsers(title="Commands", required=True)

    capture_help = "Capture a snapshot of the world"
    capture = sub.add_parser("capture", help=capture_help, description=capture_help)
    capture.add_argument("--clean", action="store_true", help="Remove all existing snapshots")
    capture.add_argument("world", type=Path, help="Path of the world to capture")
    capture.set_defaults(func=_capture)

    args = parser.parse_args()
    _set_verbosity(args)
    args.func(args)


def _set_verbosity(args: argparse.Namespace) -> None:
    if args.quiet:
        level = logging.WARNING
    elif args.verbose:
        level = logging.DEBUG
    else:
        level = logging.INFO
    logging.basicConfig(level=level)


def _patch_tar_logger(tar: tarfile.TarFile, logger_: logging.Logger) -> None:
    lg = logger_.getChild("tar")

    def new_dbg(level: int, msg: str) -> None:
        if level <= (tar.debug or 0):
            lg.debug(msg)

    tar._dbg = new_dbg  # type: ignore[attr-defined]


def _rmtree_on_error(
    _function: "Unused",
    path: str,
    exc_info: tuple[type[BaseException], BaseException, "TracebackType"],
) -> None:
    logger.warning(f"failed to remove {path}", exc_info=exc_info)


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
    new_capture = capture_directory / f"{existing_count}.bak"
    # noinspection PyProtectedMember
    with tarfile.open(  # type: ignore[call-overload]
        new_capture,
        "x:" + diff._DEFAULT_COMPRESSION,
        options={400: MAX_WORKERS},
        debug=max(args.verbose - 1, 0),
    ) as tar:
        _patch_tar_logger(tar, logger)
        logger.info(f"Compressing {world}...")
        tar.add(world, ".")
    logger.debug(f"Finished ({new_capture.stat().st_size // 2**20}MiB)")


if __name__ == "__main__":
    main()
