import io
import zlib
from pathlib import Path
from typing import TYPE_CHECKING, Protocol

import pytest
import rapidnbt

import minedelta.nbt

if TYPE_CHECKING:
    from _typeshed import StrPath


class CompareFileFunc(Protocol):
    def __call__(
        self,
        left: "StrPath",
        left_comp_type: int,
        right: "StrPath",
        right_comp_type: int,
        exclude_last_update: bool = False,
    ) -> bool: ...


@pytest.mark.parametrize(
    "compare_func",
    [minedelta.nbt._py_compare_nbt_files, minedelta.nbt.compare_nbt_files],
    ids=("py_compare", "rust_compare"),
)
def test_external_nbt(compare_func: CompareFileFunc, tmp_path: Path) -> None:
    tag = rapidnbt.CompoundTag({"hello": "world"})
    left = zlib.compress(tag.to_binary_nbt(False))
    tag["hello"] = "world!"
    right = zlib.compress(tag.to_binary_nbt(False))
    left_file = tmp_path / "left.nbt"
    right_file = tmp_path / "right.nbt"
    left_file.write_bytes(left)
    right_file.write_bytes(right)
    assert not compare_func(left_file, 2, right_file, 2)


@pytest.mark.skipif(not hasattr(minedelta.nbt, "_ZlibReader"), reason="_ZlibReader not available")
class TestZlibReader:
    def test_read(self) -> None:
        f = io.BytesIO(zlib.compress(b"hello world"))
        with minedelta.nbt._ZlibReader(f) as z_f:
            assert z_f.read() == b"hello world"
            assert z_f.read() == b""

    def test_split(self) -> None:
        f = io.BytesIO()
        f.write(zlib.compress(b"hello "))
        f.write(zlib.compress(b"world"))
        f.seek(0)
        with minedelta.nbt._ZlibReader(f) as z_f:
            assert z_f.read() == b"hello world"

    def test_truncated(self) -> None:
        f = io.BytesIO(zlib.compress(b"hello world")[:-1])
        with minedelta.nbt._ZlibReader(f) as z_f, pytest.raises(EOFError):
            z_f.read()
