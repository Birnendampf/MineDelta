"""Individually compress files and store meta in sqlite database."""

import concurrent.futures
import contextlib
import datetime
import os
import shutil
import sqlite3
import sys
import time
from collections.abc import Callable, Generator
from functools import partial
from pathlib import Path
from typing import TYPE_CHECKING, Final

from blake3 import blake3

from minedelta._thread_scope import ThreadScope

from .base import BACKUP_IGNORE_FROZENSET, BackupInfo, BaseBackupManager, _noop

if TYPE_CHECKING:
    from _typeshed import StrPath

if sys.version_info >= (3, 12):  # pragma: no cover
    from typing import override
else:
    from typing_extensions import override

if sys.version_info >= (3, 14):  # pragma: no cover
    # noinspection PyCompatibility
    from compression import zstd
else:
    # noinspection PyPackageRequirements
    from backports import zstd

__all__ = ["FastBackupManager"]

BUF_SIZE: Final = 2**19


class FastBackupManager(BaseBackupManager[str]):
    """Individually compress files and store meta in sqlite database."""

    index_by = "id"

    __slots__ = ()

    @override
    def prepare(self) -> None:
        super().prepare()
        (self._backup_dir / "objects").mkdir(exist_ok=True)
        with contextlib.closing(sqlite3.connect(self._backup_dir / "index.sqlite")) as conn:
            with conn:
                conn.executescript(
                    """
                    PRAGMA journal_mode = WAL;
                    PRAGMA synchronous = NORMAL;
                    PRAGMA foreign_keys = ON;

                    BEGIN;
                    CREATE TABLE IF NOT EXISTS Snapshots
                    (
                        id      INTEGER PRIMARY KEY,
                        date    INTEGER NOT NULL,
                        message TEXT
                    );
                    CREATE TABLE IF NOT EXISTS FileIndex
                    (
                        id       INTEGER REFERENCES Snapshots (id) ON DELETE CASCADE,
                        path     TEXT,
                        size     INTEGER NOT NULL,
                        mtime    INTEGER NOT NULL,
                        hash     BLOB    NOT NULL,
                        is_delta INTEGER NOT NULL,
                        PRIMARY KEY (id, path)
                    ) WITHOUT ROWID;
                    """
                )
            conn.execute("PRAGMA optimize")

    @contextlib.contextmanager
    def _get_cursor(self) -> Generator[sqlite3.Cursor, None, None]:
        with (
            contextlib.closing(
                sqlite3.connect(self._backup_dir / "index.sqlite", isolation_level="IMMEDIATE")
            ) as conn,
            conn,
        ):
            yield conn.executescript("PRAGMA synchronous = NORMAL; PRAGMA foreign_keys = ON;")

    @override
    def create_backup(
        self,
        description: str | None = None,
        progress: Callable[[str], None] = _noop,
        executor: concurrent.futures.Executor | None = None,
    ) -> BackupInfo:
        timestamp = int(time.time())
        with self._get_cursor() as cursor:
            cursor.execute(
                "INSERT INTO Snapshots (date, message) VALUES (?, ?)", (timestamp, description)
            )
            new_id = cursor.lastrowid
            assert new_id is not None  # noqa: S101
            prev: dict[tuple[str, int], tuple[int, bytes]] = {
                row[:2]: row[2:]
                for row in cursor.execute(
                    """
                    SELECT path, size, mtime, hash
                    FROM FIleIndex
                    WHERE id == (SELECT max(id) FROM FileIndex)
                    """
                )
            }
            rows = []
            scan_stack = [self._world]
            with ThreadScope(executor, "create backup") as scope:
                while scan_stack:
                    with os.scandir(scan_stack.pop()) as it:
                        for entry in it:
                            if entry.name in BACKUP_IGNORE_FROZENSET:
                                continue
                            if entry.is_dir():
                                scan_stack.append(entry)
                                continue
                            rel_path = os.path.relpath(entry, self._world)
                            st = entry.stat()
                            mtime = st.st_mtime_ns // 10**3
                            prev_mtime, prev_hash = prev.get((rel_path, st.st_size), (None, b""))
                            if mtime == prev_mtime:
                                file_hash = prev_hash
                            else:
                                file_hash = self._hash_file(entry)
                                if file_hash != prev_hash:
                                    scope.submit(self._add_file, entry, file_hash)
                            rows.append((new_id, rel_path, st.st_size, mtime, file_hash, 0))
                cursor.executemany("INSERT INTO FileIndex VALUES (?, ?, ?, ?, ?, ?)", rows)
        return BackupInfo(
            datetime.datetime.fromtimestamp(timestamp, datetime.UTC), str(new_id), description
        )

    def _get_obj_path(self, sha: bytes) -> Path:
        hex_sha = sha.hex()
        return self._backup_dir / "objects" / hex_sha[:2] / hex_sha[2:]

    def _add_file(self, path: "StrPath", sha: bytes) -> None:
        target_path = self._get_obj_path(sha)
        target_path.parent.mkdir(exist_ok=True)
        with (
            contextlib.suppress(FileExistsError, PermissionError),
            open(target_path, "xb", BUF_SIZE, opener=partial(os.open, mode=0o444)) as _w_f,
            open(path, "rb", 0) as r_f,
            zstd.open(_w_f, "w") as w_f,
        ):
            w_f.write(b"")  # ZstdFile does not flush on empty files. this may be a bug.
            # noinspection PyTypeChecker
            shutil.copyfileobj(r_f, w_f, BUF_SIZE)

    @staticmethod
    def _hash_file(path: "StrPath") -> bytes:
        return blake3().update_mmap(path).digest(16)

    @override
    def restore_backup(self, id_: str, progress: Callable[[str], None] = _noop) -> None:
        with self._get_cursor() as cursor:
            rows: list[tuple[str, bytes, int]] = cursor.execute(
                "SELECT path, hash, mtime FROM FileIndex WHERE id = ?", (id_,)
            ).fetchall()
            if not rows:
                self._validate_id(cursor, id_)
        self._clear_world()
        world = Path(self._world)
        world.mkdir(parents=True, exist_ok=True)
        for file, sha, mtime in rows:
            mtime_ns = mtime * 10**3
            path = world / file
            path.parent.mkdir(parents=True, exist_ok=True)
            with zstd.open(self._get_obj_path(sha)) as r_f, open(path, "wb") as w_f:
                # noinspection PyTypeChecker
                shutil.copyfileobj(r_f, w_f, BUF_SIZE)
            os.utime(path, ns=(mtime_ns, mtime_ns))

    @staticmethod
    def _validate_id(cursor: sqlite3.Cursor, id_: str) -> None:
        try:
            idx = int(id_)
        except ValueError:
            raise ValueError(f"id {id_} is not an integer") from None
        if idx < 0:
            raise IndexError("index must be >= 0")
        if cursor.execute("SELECT 1 FROM Snapshots WHERE id = ?", (id_,)).fetchone() is None:
            raise IndexError(f"no backup found with id {id_}")

    @override
    def delete_backup(self, id_: str, progress: Callable[[str], None] = _noop) -> None:
        with self._get_cursor() as cursor:
            cursor.execute("DELETE FROM Snapshots WHERE id = ?", (id_,))
            if not cursor.rowcount:
                self._validate_id(cursor, id_)

    @override
    def list_backups(self) -> list[BackupInfo]:
        with self._get_cursor() as cursor:
            return [
                BackupInfo(
                    datetime.datetime.fromtimestamp(row[0], tz=datetime.UTC), str(row[1]), row[2]
                )
                for row in cursor.execute(
                    "SELECT date, id, message FROM Snapshots ORDER BY id DESC"
                )
            ]

    def gc(self, progress: Callable[[str], None] = _noop) -> int:
        """Collect unused objects."""
        with self._get_cursor() as cursor:
            present: set[str] = {
                row[0].hex()
                for row in cursor.execute("SELECT DISTINCT hash FROM FileIndex").fetchall()
            }
            cursor.executescript("VACUUM; PRAGMA optimize;")
        removed = 0
        for top in (self._backup_dir / "objects").iterdir():
            for file in top.iterdir():
                if top.name + file.name not in present:
                    removed += 1
                    file.unlink()
        return removed
