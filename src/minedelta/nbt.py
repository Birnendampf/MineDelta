"""Contains a special NBT parser used to compare nbt data as quickly as possible."""

import functools
import io
import struct
from collections.abc import Callable
from typing import TypeAlias

RawCompound: TypeAlias = bytes | dict[bytes, "RawCompound"] | list["RawCompound"]
_parse_func_type: TypeAlias = Callable[[io.BytesIO], RawCompound]

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

    try:
        parse_func = TAG_LUT[tag_id - 1]
    except IndexError:
        raise ValueError(f"Unknown tag id in List: {tag_id}") from None
    return [parse_func(stream) for _ in range(size)]


def _get_raw_compound(stream: io.BytesIO) -> dict[bytes, RawCompound]:
    result: dict[bytes, RawCompound] = {}

    while tag_id := stream.read(1)[0]:
        try:
            parse_func = TAG_LUT[tag_id - 1]
        except IndexError:
            raise ValueError(f"Unknown tag id in Compound: {tag_id}") from None
        name_len = _U_SHORT.unpack(stream.read(2))[0]
        raw_name = stream.read(name_len)
        result[raw_name] = parse_func(stream)

    return result


TAG_SIZE_LUT = [0, 1, 2, 4, 8, 4, 8]

TAG_LUT: list[_parse_func_type] = [
    *(functools.partial(_get_raw_numeric, size) for size in TAG_SIZE_LUT[1:]),
    lambda stream: _get_raw_array(1, stream),  # byte_array
    _get_raw_string,
    _get_raw_list,
    _get_raw_compound,
    lambda stream: _get_raw_array(4, stream),  # int_array
    lambda stream: _get_raw_array(8, stream),  # long_array
]


def load_nbt_raw(data: bytes) -> dict[bytes, RawCompound]:
    """Get the overall structure of a nbt file, while parsing as little of it as possible.

    Raises:
        EOFError: Unexpected end of file.
    """
    stream = io.BytesIO(data)
    try:
        if stream.read(1)[0] != 10:
            raise ValueError("Root tag is not Compound")

        name_len = _U_SHORT.unpack(stream.read(2))[0]
        stream.read(name_len)  # Skip root name

        return _get_raw_compound(stream)
    except (IndexError, struct.error) as exc:
        if not stream.read(1):
            raise EOFError("Unexpected EOF") from exc
        raise exc


def _load_add_exc_note(data: bytes, left: bool) -> dict[bytes, RawCompound]:
    try:
        return load_nbt_raw(data)
    except Exception as exc:
        exc.add_note(f"Occurred while parsing {'left' if left else 'right'}")
        raise exc


def _py_compare_nbt(left: bytes, right: bytes, exclude_last_update: bool = False) -> bool:
    """Compare two NBT files."""
    this_nbt = _load_add_exc_note(left, True)
    other_nbt = _load_add_exc_note(right, False)
    if exclude_last_update:
        this_nbt.pop(b"LastUpdate", None)
        other_nbt.pop(b"LastUpdate", None)
    return this_nbt == other_nbt


try:
    from nbtcompare import compare as _rust_compare_nbt
except ImportError:  # pragma: no cover
    compare_nbt = _py_compare_nbt
else:
    compare_nbt = _rust_compare_nbt
