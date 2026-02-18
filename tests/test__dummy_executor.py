from typing import NoReturn

import pytest

# noinspection PyProtectedMember
from minedelta import _dummy_executor


def some_func(arg: int, /, *, kwarg: int) -> int:
    return arg + kwarg


class SomeError(Exception):
    pass


def raises() -> NoReturn:
    raise SomeError()


class TestDummyExecutor:
    def test_submit(self) -> None:
        with _dummy_executor.DummyExecutor() as executor:
            submit = executor.submit(some_func, 1, kwarg=2)
            assert submit.result() == 3
            with pytest.raises(SomeError):
                executor.submit(raises)

    def test_map(self) -> None:
        with _dummy_executor.DummyExecutor() as executor:
            assert tuple(executor.map(sum, zip(range(3), range(3), strict=True))) == (0, 2, 4)
