"""Individually compress files and store meta in sqlite database."""

import contextlib
import datetime
import hashlib
import os
import shutil
import sqlite3
import sys
import time
from collections.abc import Callable
from functools import partial
from pathlib import Path
from typing import TYPE_CHECKING, Final

from . import BackupInfo
from .base import BACKUP_IGNORE_FROZENSET, BaseBackupManager, _id_T, _noop

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
    from backports import zstd

BUF_SIZE: Final = 2**19


class FastBackupManager(BaseBackupManager[int]):
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

    @override
    def create_backup(
        self, description: str | None = None, progress: Callable[[str], None] = _noop
    ) -> BackupInfo:
        timestamp = int(time.time())
        with (
            contextlib.closing(
                sqlite3.connect(self._backup_dir / "index.sqlite", isolation_level="IMMEDIATE")
            ) as conn,
            conn,
        ):
            cursor = conn.executescript("PRAGMA synchronous = NORMAL; PRAGMA foreign_keys = ON;")
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
            scan_stack = [self._world]
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
                                with contextlib.suppress(FileExistsError, PermissionError):
                                    # EAFP, elegantly allows multithreading in the future
                                    self._add_file(entry, file_hash)
                        row = (new_id, rel_path, st.st_size, mtime, file_hash, 0)
                        cursor.execute("INSERT INTO FileIndex VALUES (?, ?, ?, ?, ?, ?)", row)
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
            open(path, "rb", 0) as r_f,
            open(target_path, "xb", BUF_SIZE, opener=partial(os.open, mode=0o444)) as _w_f,
            zstd.open(_w_f, "w") as w_f,
        ):
            # noinspection PyTypeChecker
            shutil.copyfileobj(r_f, w_f, BUF_SIZE)

    @staticmethod
    def _hash_file(path: "StrPath") -> bytes:
        with open(path, "rb", 0) as f:
            return hashlib.file_digest(f, hashlib.sha1).digest()

    @override
    def restore_backup(self, id_: str, progress: Callable[[str], None] = _noop) -> None:
        raise NotImplementedError("TODO")

    @override
    def delete_backup(self, id_: _id_T, progress: Callable[[str], None] = _noop) -> None:
        raise NotImplementedError("TODO")

    @override
    def list_backups(self) -> list[BackupInfo]:
        raise NotImplementedError("TODO")
