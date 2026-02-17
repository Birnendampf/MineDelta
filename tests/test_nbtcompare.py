import itertools
from typing import Any, Protocol

import pytest
import rapidnbt
from nbtcompare import compare

# noinspection PyProtectedMember
from minedelta.nbt import _py_compare_nbt

pytestmark = pytest.mark.parametrize(
    "compare_func", [_py_compare_nbt, compare], ids=("py_compare", "rust_compare")
)


class CompareFunc(Protocol):
    def __call__(self, left: bytes, right: bytes, exclude_last_update: bool = False) -> bool: ...


@pytest.mark.parametrize("left_fault", [True, False])
class TestExceptions:
    @staticmethod
    def get_exc_note(left_fault: bool) -> str:
        return f"\nOccurred while parsing {'left' if left_fault else 'right'}"

    def test_truncated_nbt(
        self, compare_func: CompareFunc, left_fault: bool, subtests: pytest.Subtests
    ) -> None:
        correct = rapidnbt.CompoundTag(
            {"foo": ["bar"], "baz": rapidnbt.ByteArrayTag([0])}
        ).to_binary_nbt(False)
        for i in range(len(correct)):
            incorrect = correct[:i]
            left, right = (incorrect, correct) if left_fault else (correct, incorrect)
            with (
                subtests.test("truncated tag", cutoff=i),
                pytest.raises(EOFError, match="Unexpected EOF" + self.get_exc_note(left_fault)),
            ):
                compare_func(left, right)

    def test_non_compound_root(self, compare_func: CompareFunc, left_fault: bool) -> None:
        left = rapidnbt.CompoundTag().to_binary_nbt(False)
        right = b"\x09\x00\x00\x00\x00\x00\x00\x00"  # empty list of end tags
        if left_fault:
            left, right = right, left
        with pytest.raises(
            ValueError, match="Root tag is not Compound" + self.get_exc_note(left_fault)
        ):
            compare_func(left, right)

    def test_unknown_tag_in_compound(self, compare_func: CompareFunc, left_fault: bool) -> None:
        left = rapidnbt.CompoundTag().to_binary_nbt(False)
        right = b"\x0a\x00\x00\x0d\x00"
        if left_fault:
            left, right = right, left
        with pytest.raises(
            ValueError, match="Unknown tag id in Compound" + self.get_exc_note(left_fault)
        ):
            compare_func(left, right)

    def test_unknown_tag_in_list(self, compare_func: CompareFunc, left_fault: bool) -> None:
        left = rapidnbt.CompoundTag().to_binary_nbt(False)
        right = b"\x0a\x00\x00\x09\x00\x00\x0d\x00\x00\x00\x00\x00"
        if left_fault:
            left, right = right, left
        with pytest.raises(
            ValueError, match="Unknown tag id in List: 13" + self.get_exc_note(left_fault)
        ):
            compare_func(left, right)


NUMERIC_TAGS = (
    rapidnbt.ByteTag,
    rapidnbt.ShortTag,
    rapidnbt.IntTag,
    rapidnbt.LongTag,
    rapidnbt.FloatTag,
    rapidnbt.DoubleTag,
)
ALL_TAGS = (
    rapidnbt.EndTag,
    rapidnbt.ByteArrayTag,
    rapidnbt.StringTag,
    rapidnbt.ListTag,
    rapidnbt.CompoundTag,
    rapidnbt.IntArrayTag,
    rapidnbt.LongArrayTag,
    *NUMERIC_TAGS,
)


def wrap_in_compound(tag: rapidnbt.Tag) -> bytes:
    return rapidnbt.CompoundTag({"": tag}).to_binary_nbt(False)


def possible_values_id_fn(val: Any) -> str | None:  # noqa: ANN401
    if isinstance(val, tuple):
        return ""
    return None


@pytest.mark.parametrize(
    ("tag_type", "possible_values"),
    itertools.zip_longest(
        ALL_TAGS,
        (
            (),
            *((b"", b"0", b"1", b"11"),) * 2,
            ([], [0], [1], [1, 1]),
            ({}, {"": 0}, {"0": 0}, {"1": 0}, {"": 1}, {"": 0, "0": 0}),
            *(([], [0], [1], [1, 1]),) * 2,
        ),
        fillvalue=(0, 1),
    ),
    ids=possible_values_id_fn,
)
def test_tag_types(
    tag_type: type[rapidnbt.Tag],
    possible_values: tuple[Any, ...],
    compare_func: CompareFunc,
    subtests: pytest.Subtests,
) -> None:
    for value in possible_values:
        with subtests.test(msg="equal", value=value):
            left = right = wrap_in_compound(tag_type(value))  # type: ignore[call-arg]
            assert compare_func(left, right)

    for left_arg, right_arg in itertools.combinations(possible_values, 2):
        with subtests.test(msg="inequal", left=left_arg, right=right_arg):
            left = wrap_in_compound(tag_type(left_arg))  # type: ignore[call-arg]
            right = wrap_in_compound(tag_type(right_arg))  # type: ignore[call-arg]
            assert not compare_func(left, right)

