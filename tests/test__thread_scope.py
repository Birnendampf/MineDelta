import contextlib
import threading
import time
from collections.abc import Callable
from concurrent.futures import Future, ThreadPoolExecutor
from typing import TYPE_CHECKING, Any, NoReturn, ParamSpec, TypeVar

import pytest

# noinspection PyProtectedMember
from minedelta import _thread_scope

# noinspection PyProtectedMember
from minedelta._thread_scope import ThreadScope

if TYPE_CHECKING:
    from _typeshed import Unused

P = ParamSpec("P")
T = TypeVar("T")


def some_func(arg: int, /, *, kwarg: int) -> int:
    return arg + kwarg


class SomeError(Exception):
    pass


class OtherError(Exception):
    pass


def raises(ex: Exception | type[Exception]) -> NoReturn:
    raise ex


def wrap_barrier(
    barrier: threading.Barrier, fn: Callable[P, T], /, *args: P.args, **kwargs: P.kwargs
) -> T:
    barrier.wait()
    return fn(*args, **kwargs)


class _SlowFunc:
    __slots__ = ("done",)

    def __init__(self) -> None:
        self.done = False

    def __call__(self) -> None:
        # TODO: this sucks, but there is no better way to emulate a task that is still "running"
        #  at the end of a scope
        time.sleep(0.05)
        self.done = True


class TestDummyExecutor:
    def test_submit(self) -> None:
        with _thread_scope.DummyExecutor() as executor:
            submit = executor.submit(some_func, 1, kwarg=2)
            assert submit.result() == 3
            with pytest.raises(SomeError):
                executor.submit(raises, SomeError)

    def test_map(self) -> None:
        with _thread_scope.DummyExecutor() as executor:
            assert tuple(executor.map(sum, zip(range(3), range(3), strict=True))) == (0, 2, 4)


class TestThreadScope:
    @pytest.fixture
    def non_dummy_thread_scope(self) -> ThreadScope:
        return ThreadScope(ThreadPoolExecutor(_thread_scope.MAX_WORKERS))

    @pytest.mark.parametrize("single_threaded", [True, False])
    def test_threadpool_choice(
        self, single_threaded: bool, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setattr(_thread_scope, "MAX_WORKERS", 1 if single_threaded else 2)
        with ThreadScope(None) as scope:
            assert isinstance(
                scope._pool,
                _thread_scope.DummyExecutor if single_threaded else _thread_scope._DefaultExecutor,
            )

    @pytest.mark.parametrize("exception", [True, False])
    @pytest.mark.parametrize("nested", [True, False])
    def test_shutdown_called(
        self, monkeypatch: pytest.MonkeyPatch, exception: bool, nested: bool
    ) -> None:
        shutdown_called = False

        def mock_shutdown(*_: "Unused", **__: "Unused") -> None:
            nonlocal shutdown_called
            shutdown_called = True

        monkeypatch.setattr(_thread_scope, "MAX_WORKERS", 1)
        monkeypatch.setattr(_thread_scope.DummyExecutor, "shutdown", mock_shutdown)
        with ThreadScope(None) if nested else contextlib.nullcontext() as outer:
            try:
                with ThreadScope(outer):
                    if exception:
                        raise SomeError
            except* SomeError:
                pass
            assert shutdown_called != nested

    def test_nested_scope(self, monkeypatch: pytest.MonkeyPatch) -> None:
        submit_called = False

        def mock_submit(fn: Callable[P, T], /, *args: P.args, **kwargs: P.kwargs) -> Future[T]:
            nonlocal submit_called
            submit_called = True
            future: Future[T] = Future()
            future.set_result(fn(*args, **kwargs))
            return future

        with _thread_scope.DummyExecutor() as executor:
            monkeypatch.setattr(executor, "submit", mock_submit)
            with ThreadScope(executor) as s1, ThreadScope(s1) as s2:
                s2.submit(some_func, 1, kwarg=2)
            assert submit_called

    def test_scope_order(self, non_dummy_thread_scope: ThreadScope) -> None:
        slow_func = _SlowFunc()
        with non_dummy_thread_scope as s1:
            with ThreadScope(s1) as s2:
                s2.submit(slow_func)
            assert slow_func.done

    @pytest.mark.parametrize("handle_exception", [True, False])
    def test_exception(self, handle_exception: bool, non_dummy_thread_scope: ThreadScope) -> None:
        to_be_cancelled: Future[Any] = Future()
        slow_func = _SlowFunc()
        errors = (SomeError,) if handle_exception else (SomeError, OtherError)
        with (
            pytest.RaisesGroup(*errors, match=r"^Exceptions occurred in scope$"),
            non_dummy_thread_scope as scope,
        ):
            b = threading.Barrier(3)
            scope._tasks.add(to_be_cancelled)
            scope.submit(wrap_barrier, b, slow_func)
            # noinspection PyTypeChecker
            t1 = scope.submit(wrap_barrier, b, raises, SomeError)
            with pytest.raises(ValueError, match=r"^Cannot mark unfinished future as handled$"):
                scope.mark_handled(t1)
            t2 = scope.submit(raises, OtherError)
            b.wait()
            try:
                t2.result()
            except OtherError:
                if handle_exception:
                    scope.mark_handled(t2)
                else:
                    raise
        assert slow_func.done
        assert to_be_cancelled.cancelled()

    # noinspection PyUnreachableCode
    def test_exception_in_block(self) -> None:
        to_be_cancelled: Future[Any] = Future()
        slow_func = _SlowFunc()
        # noinspection PyTypeChecker
        with (
            pytest.RaisesGroup(
                SomeError, OtherError, match=r'^Exceptions occurred in scope "name"$'
            ),
            ThreadPoolExecutor(max_workers=_thread_scope.MAX_WORKERS) as ex,
            ThreadScope(ex, "name") as scope,
        ):
            scope._tasks.add(to_be_cancelled)
            b = threading.Barrier(3)
            scope.submit(wrap_barrier, b, slow_func)
            # noinspection PyTypeChecker
            scope.submit(wrap_barrier, b, raises, SomeError)
            b.wait()
            raise OtherError
        assert slow_func.done
        assert to_be_cancelled.cancelled()
