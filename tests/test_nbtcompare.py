from typing import Protocol

import pytest
import rapidnbt
from _pytest.subtests import Subtests

# noinspection PyProtectedMember
from minedelta.nbt import _py_compare_nbt, _rust_compare_nbt  # type: ignore[attr-defined]

pytestmark = pytest.mark.parametrize("compare_func", [_py_compare_nbt, _rust_compare_nbt])


class CompareFunc(Protocol):
    def __call__(self, left: bytes, right: bytes, exclude_last_update: bool = False) -> bool: ...


@pytest.mark.parametrize("left_fault", [True, False])
class TestExceptions:
    @staticmethod
    def get_exc_note(left_fault: bool) -> str:
        return f"\nOccurred while parsing {'left' if left_fault else 'right'}"

    def test_truncated_nbt(
        self, compare_func: CompareFunc, left_fault: bool, subtests: Subtests
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
