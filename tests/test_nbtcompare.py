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
            ValueError, match="Unknown tag id in Compound: 13" + self.get_exc_note(left_fault)
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


def wrap_in_compound(tag: rapidnbt.Tag | list[rapidnbt.Tag]) -> bytes:
    return rapidnbt.CompoundTag({"": tag}).to_binary_nbt(False)


@pytest.mark.parametrize("as_list", [True, False], ids=("List", pytest.HIDDEN_PARAM))  # type: ignore[arg-type]
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
    ids=(tag.__name__ for tag in ALL_TAGS),
)
def test_tag_types(
    tag_type: type[rapidnbt.Tag],
    possible_values: tuple[Any, ...],
    as_list: bool,
    compare_func: CompareFunc,
    subtests: pytest.Subtests,
) -> None:
    possible_tags: tuple[list[rapidnbt.Tag], ...] | tuple[rapidnbt.Tag, ...]
    if as_list:
        possible_tags = tuple([tag_type(value)] for value in possible_values)  # type: ignore[call-arg]
    else:
        possible_tags = tuple(tag_type(value) for value in possible_values)  # type: ignore[call-arg]

    for value in possible_tags:
        with subtests.test(msg="equal", value=value.to_snbt(indent=0)):
            left = right = wrap_in_compound(value)
            assert compare_func(left, right)

    for left_arg, right_arg in itertools.combinations(possible_tags, 2):
        with subtests.test(
            msg="inequal", left=left_arg.to_snbt(indent=0), right=right_arg.to_snbt(indent=0)
        ):
            left = wrap_in_compound(left_arg)
            right = wrap_in_compound(right_arg)
            assert not compare_func(left, right)


@pytest.mark.parametrize("is_chunk", [True, False])
def test_exclude_last_update(compare_func: CompareFunc, is_chunk: bool) -> None:
    left = rapidnbt.CompoundTag({"LastUpdate": 1}).to_binary_nbt(False)
    right = rapidnbt.CompoundTag({"LastUpdate": 0}).to_binary_nbt(False)
    assert compare_func(left, right, is_chunk) == is_chunk
