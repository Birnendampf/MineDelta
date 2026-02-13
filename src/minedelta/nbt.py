"""Contains a special NBT parser used to compare nbt data as quickly as possible."""

import functools
import io
import struct
from collections.abc import Callable
from typing import TypeAlias, cast

RawCompound: TypeAlias = bytes | dict[bytes, "RawCompound"] | list["RawCompound"]
_parse_func_type = Callable[[io.BytesIO], RawCompound]

_U_SHORT = struct.Struct("!H")
_U_INT = struct.Struct("!I")


def _get_raw_numeric(size: int, stream: io.BytesIO) -> bytes:
    return stream.read(size)


def _get_raw_array(size: int, stream: io.BytesIO) -> bytes:
    length = _U_INT.unpack(stream.read(4))[0]
    return stream.read(length * size)


def _get_raw_string(stream: io.BytesIO) -> bytes:
    length = _U_SHORT.unpack(stream.read(2))[0]
    return stream.read(length)


def _get_raw_list(stream: io.BytesIO) -> bytes | list[RawCompound]:
    tag_id = stream.read(1)[0]
    size = _U_INT.unpack(stream.read(4))[0]

    if tag_id < 7:
        tag_size = TAG_SIZE_LUT[tag_id]
        arr_byte_len = tag_size * size
        return stream.read(arr_byte_len)

    # TAG_LUT[tag_id] can't be none at this point but mypy doesn't know that. This is a hot code
    # path so a cast is used instead of assert because it's faster at runtime
    # noinspection PyUnnecessaryCast
    parse_func = cast("_parse_func_type", TAG_LUT[tag_id])
    return [parse_func(stream) for _ in range(size)]


def _get_raw_compound(stream: io.BytesIO) -> dict[bytes, RawCompound]:
    result: dict[bytes, RawCompound] = {}

    while parse_func := TAG_LUT[stream.read(1)[0]]:
        name_len = _U_SHORT.unpack(stream.read(2))[0]
        raw_name = stream.read(name_len)
        result[raw_name] = parse_func(stream)

    return result


TAG_SIZE_LUT = [0, 1, 2, 4, 8, 4, 8]

TAG_LUT: list[_parse_func_type | None] = [None]
TAG_LUT.extend(functools.partial(_get_raw_numeric, size) for size in TAG_SIZE_LUT[1:])
TAG_LUT.extend(
    (
        lambda stream: _get_raw_array(1, stream),  # byte_array
        _get_raw_string,
        _get_raw_list,
        _get_raw_compound,
        lambda stream: _get_raw_array(4, stream),  # int_array
        lambda stream: _get_raw_array(8, stream),  # long_array
    )
)


def load_nbt_raw(data: bytes) -> dict[bytes, RawCompound]:
    """Get the overall structure of a nbt file, while parsing as little of it as possible."""
    if data[0] != 10:
        raise ValueError("Root TAG is not Compound")

    stream = io.BytesIO(data)
    stream.read(1)  # Skip root tag
    name_len = _U_SHORT.unpack(stream.read(2))[0]
    stream.read(name_len)  # Skip root name

    return _get_raw_compound(stream)


def _py_compare_nbt(buffer1: bytes, buffer2: bytes, is_chunk: bool) -> bool:
    """Compare two NBT files."""
    this_nbt = load_nbt_raw(buffer1)
    other_nbt = load_nbt_raw(buffer2)
    if is_chunk:
        other_nbt[b"LastUpdate"] = this_nbt[b"LastUpdate"]  # type: ignore[index]
    return this_nbt == other_nbt


try:
    import rapidnbt
except ImportError:
    compare_nbt = _py_compare_nbt
else:

    def compare_nbt(left: bytes, right: bytes, is_chunk: bool) -> bool:
        """Compare two NBT files."""
        this_nbt = rapidnbt.nbtio.loads(left, rapidnbt.NbtFileFormat.BIG_ENDIAN)
        other_nbt = rapidnbt.nbtio.loads(right, rapidnbt.NbtFileFormat.BIG_ENDIAN)
        if this_nbt is None or other_nbt is None:
            raise ValueError("Failed to load NBT")
        if is_chunk:
            this_nbt.pop("LastUpdate")
            other_nbt.pop("LastUpdate")
        return this_nbt == other_nbt
