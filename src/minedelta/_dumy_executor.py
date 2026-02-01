import sys
from collections.abc import Callable, Iterable, Iterator
from concurrent.futures import Executor, Future
from typing import Any, ParamSpec, TypeVar

if sys.version_info >= (3, 12):
    pass
else:
    pass

P = ParamSpec("P")
T = TypeVar("T")


class DummyExecutor(Executor):
    def submit(self, fn: Callable[P, T], /, *args: P.args, **kwargs: P.kwargs) -> Future[T]:
        future: Future[T] = Future()
        try:
            result = fn(*args, **kwargs)
        except BaseException as exc:
            future.set_exception(exc)
        else:
            future.set_result(result)
        return future

    def map(self, fn: Callable[..., T], *iterables: Iterable[Any], **_: object) -> Iterator[T]:
        return map(fn, *iterables)
