import contextlib
import os
from collections.abc import Callable, Iterable, Iterator
from concurrent.futures import (
    ALL_COMPLETED,
    FIRST_EXCEPTION,
    Executor,
    Future,
    ThreadPoolExecutor,
    wait,
)
from typing import TYPE_CHECKING, Any, ParamSpec, Self, TypeVar, cast

if TYPE_CHECKING:
    from _typeshed import Unused

P = ParamSpec("P")
T = TypeVar("T")


def __cpu_count() -> int | None:  # pragma: no cover
    with contextlib.suppress(AttributeError):
        return cast("int | None", os.process_cpu_count())  # type: ignore[attr-defined]
    # sys.version_info < 3.13
    with contextlib.suppress(AttributeError):
        return len(os.sched_getaffinity(0))
    # not UNIX
    return os.cpu_count()


MAX_WORKERS = __cpu_count() or 1
del __cpu_count

# InterpreterPool is not supported
_DefaultExecutor = ThreadPoolExecutor


class DummyExecutor(Executor):
    def submit(self, fn: Callable[P, T], /, *args: P.args, **kwargs: P.kwargs) -> Future[T]:
        """Runs the function immediately rather than concurrently.

        If an exception occurs, it is raised immediately instead of being passed to the Future.
        """
        future: Future[T] = Future()
        future.set_result(fn(*args, **kwargs))
        return future

    def map(self, fn: Callable[..., T], *iterables: Iterable[Any], **_: object) -> Iterator[T]:
        return map(fn, *iterables)


class ThreadScope:
    __slots__ = ("_owns_threadpool", "_scope_name", "_tasks", "_threadpool")

    def __init__(self, parent: Executor | Self | None, scope_name: str = "") -> None:
        self._owns_threadpool = not parent
        if parent:
            self._threadpool = parent
        elif MAX_WORKERS <= 1:
            self._threadpool = DummyExecutor()
        else:
            self._threadpool = _DefaultExecutor(
                max_workers=MAX_WORKERS,
                thread_name_prefix=f"{scope_name}_scope" if scope_name else "scope",
            )
        if not isinstance(self._threadpool, ThreadScope):
            self._threadpool._inner_submit = self._threadpool.submit  # type: ignore[attr-defined]
        self._tasks: set[Future[Any]] = set()
        self._scope_name = scope_name

    def __enter__(self) -> Self:
        return self

    def _inner_submit(self, fn: Callable[P, T], /, *args: P.args, **kwargs: P.kwargs) -> Future[T]:
        return self._threadpool._inner_submit(fn, *args, **kwargs)  # type: ignore[union-attr]

    def submit(self, fn: Callable[P, T], /, *args: P.args, **kwargs: P.kwargs) -> Future[T]:
        fut = self._inner_submit(fn, *args, **kwargs)
        self._tasks.add(fut)
        return fut

    def mark_handled(self, future: Future[Any]) -> None:
        if not future.done():
            raise ValueError("Cannot mark unfinished future as handled")
        self._tasks.remove(future)

    def __exit__(self, exc_type: "Unused", exc_val: BaseException | None, exc_tb: "Unused") -> None:
        if not exc_val:
            _, not_done = wait(self._tasks, return_when=FIRST_EXCEPTION)
        else:
            not_done = self._tasks.copy()
        # an exception occurred
        for fut in not_done:
            if fut.cancel():
                self._tasks.remove(fut)
        if self._owns_threadpool:
            assert isinstance(self._threadpool, Executor)  # noqa: S101
            # shutdown already waits, no need to wait after.
            self._threadpool.shutdown()
        else:
            wait(self._tasks, return_when=ALL_COMPLETED)
        exceptions = {fut.exception() for fut in self._tasks}
        exceptions.add(exc_val)
        exceptions.discard(None)

        if exceptions:
            error_msg = "Exceptions occurred in scope"
            if self._scope_name:
                error_msg += f' "{self._scope_name}"'
            # noinspection PyUnnecessaryCast
            raise BaseExceptionGroup(
                error_msg, tuple(cast("set[BaseException]", exceptions))
            ) from None
