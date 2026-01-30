import datetime
import itertools
import shutil
from functools import wraps
from pathlib import Path
from typing import cast, TYPE_CHECKING
from collections.abc import Callable

try:
    import dulwich as dw
    import dulwich.errors
    import dulwich.gc
    import dulwich.objects
    import dulwich.objectspec
    import dulwich.porcelain
    import dulwich.refs
    import dulwich.repo
except ImportError:
    raise ImportError("dulwich is not installed") from None

from .base import BaseBackupManager, BackupInfo, BACKUP_IGNORE, _noop, _delete_file_or_dir

if TYPE_CHECKING:
    from _typeshed import StrPath


class GitBackupManager(BaseBackupManager[str]):
    __slots__ = ()
    index_by = "id"

    @classmethod
    def _check_repo(cls, path: "StrPath", bare: bool) -> dw.repo.Repo | None:
        try:
            r = dw.repo.Repo(path)
        except dw.errors.NotGitRepository:
            return None
        if r.bare != bare:
            r.close()
            return None
        if bare or cls._check_repo(r.controldir(), True):
            return r
        r.close()
        return None

    @staticmethod
    def _gc_progress(progress: Callable[[str], None]) -> Callable[[str], None]:
        @wraps(progress)
        def new_progress(s: str) -> None:
            if s[:15] == "Checking object":
                return
            progress(s)

        return progress if progress is _noop else new_progress

    def prepare(self) -> None:
        # cases:
        # - self._backup_dir exists and is a bare git repository => ensure link exists and return
        # - self._backup_dir exists and is not a bare git repository
        #   OR self._backup_dir is not a directory
        #   remove self._backup_dir, then decide:
        #   - self._world is not a git repository  => overwrite with git repository
        #   - (self._
        world = Path(self._world)
        world_git = world / ".git"
        r = self._check_repo(self._world, False)
        need_link = True
        need_move = True
        if r is not None:
            if Path(r.controldir()).resolve() == self._backup_dir.resolve():
                # repo exists and is where it is supposed to be
                need_link = False
                need_move = False
        else:
            _delete_file_or_dir(world_git)
            if (_backup_dir_repo := self._check_repo(self._backup_dir, True)) is not None:
                _backup_dir_repo.close()
                # repo exists but has no link to it
                need_move = False
            else:
                r = dw.repo.Repo.init(self._world, default_branch=b"main", symlinks=True, format=1)

        if need_move:
            assert r is not None
            _delete_file_or_dir(self._backup_dir)
            shutil.move(r.controldir(), self._backup_dir)
            r.close()
        if need_link:
            world_git.write_bytes(b"gitdir: " + bytes(self._backup_dir))
            r = dw.repo.Repo(self._world)

        assert r is not None

        with r:
            cfg = r.get_config()
            import socket

            cfg.set("user", "name", "NKI")
            cfg.set("user", "email", f"NKI@{socket.gethostname()}")
            cfg.set("core", "preloadIndex", True)
            cfg.write_to_path()

        (world / ".gitignore").write_text("\n".join(BACKUP_IGNORE) + "\n")

    def create_backup(
        self, description: str | None = None, progress: Callable[[str], None] = _noop
    ) -> BackupInfo:
        with dw.repo.Repo(self._world) as r:
            dw.porcelain.add(r)
            progress("creating commit")
            commit_id = r.get_worktree().commit((description or "Automated Backup").encode())
            return self._commit_to_backup_info(cast(dw.objects.Commit, r[commit_id]))

    def restore_backup(self, id_: str, progress: Callable[[str], None] = _noop) -> None:
        with dw.repo.Repo(self._world) as r:
            tree = dw.objectspec.parse_tree(r, id_)
            # reset to tree to prevent HEAD from being altered
            # this is undocumented so verify this behavior whenever updating dulwich
            progress(f"resetting to {id_[:10]}")
            dw.porcelain.reset(r, "hard", tree)
            progress("cleaning world")
            dw.porcelain.clean(r, r.path)
            dw.gc.maybe_auto_gc(r, progress=self._gc_progress(progress))

    def delete_backup(self, id_: str, progress: Callable[[str], None] = _noop) -> None:
        # oh boy this one's complicated. it rewrites history and WILL mess up branches
        with dw.repo.Repo(self._world) as r:
            # noinspection PyTypeChecker
            assert len(r.refs.keys(base=dw.refs.Ref(dw.refs.LOCAL_BRANCH_PREFIX))) == 1, (
                "Multiple branches detected"
            )
            chosen = dw.objectspec.parse_commit(r, id_)
            chosen_id = chosen.id
            progress(f"preparing to delete {id_[:10]}")
            walker = r.get_walker()
            last_commits = r.get_parents(chosen_id, chosen)
            assert len(last_commits) < 2, "Merge commit detected"

            old_head = r.head()
            progress("retrieving child commits")
            children = [
                entry.commit
                for entry in itertools.takewhile(lambda e: e.commit.id != chosen_id, walker)
            ]
            progress(f"rewriting {len(children)} commits")
            for child in reversed(children):  # oldest first
                assert len(child.parents) < 2, "Merge commit detected"
                child.parents = last_commits
                r.object_store.add_object(child)
                last_commits = [child.id]

            last_commit_id = last_commits[0]
            r.refs.set_if_equals(dw.refs.HEADREF, old_head, last_commit_id)
            progress("pruning")
            _, freed = dw.gc.prune_unreachable_objects(
                r.object_store, r.refs, progress=self._gc_progress(progress)
            )
            progress(f"freed {freed:_} bytes")
            dw.gc.maybe_auto_gc(r, progress=self._gc_progress(progress))

    def list_backups(self) -> list[BackupInfo]:
        with dw.repo.Repo(self._world) as r:
            try:
                return [self._commit_to_backup_info(entry.commit) for entry in r.get_walker()]
            except KeyError:
                return []

    @staticmethod
    def _commit_to_backup_info(commit: dw.objects.Commit) -> BackupInfo:
        backup_info = BackupInfo(
            datetime.datetime.fromtimestamp(
                commit.commit_time,
                datetime.timezone(datetime.timedelta(seconds=commit.commit_timezone)),
            ),
            commit.sha().hexdigest(),
            commit.message,
        )
        return backup_info
