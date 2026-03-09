"""Contains a special NBT parser used to compare nbt data as quickly as possible."""

import functools
import io
import struct
from collections.abc import Callable
from typing import TYPE_CHECKING, Final, Literal, TypeAlias

if TYPE_CHECKING:
    from _typeshed import StrPath, SupportsRead


RawTag: TypeAlias = bytes | dict[bytes, "RawTag"] | list["RawTag"]
_parse_func_type: TypeAlias = Callable[["SupportsRead[bytes]"], RawTag]

_U_SHORT = struct.Struct("!H")
_U_INT = struct.Struct("!I")


def _get_raw_numeric(size: int, stream: "SupportsRead[bytes]") -> bytes:
    return stream.read(size)


def _get_raw_array(size: int, stream: "SupportsRead[bytes]") -> bytes:
    length = _U_INT.unpack(stream.read(4))[0]
    return stream.read(length * size)


def _get_raw_string(stream: "SupportsRead[bytes]") -> bytes:
    length = _U_SHORT.unpack(stream.read(2))[0]
    return stream.read(length)


def _get_raw_list(stream: "SupportsRead[bytes]") -> bytes | list[RawTag]:
    tag_id = stream.read(1)[0]
    size = _U_INT.unpack(stream.read(4))[0]

    if tag_id < 7:
        tag_size = TAG_SIZE_LUT[tag_id]
        byte_len = tag_size * size
        return stream.read(byte_len)

    try:
        parse_func = TAG_LUT[tag_id - 1]
    except IndexError:
        raise ValueError(f"Unknown tag id in List: {tag_id}") from None
    return [parse_func(stream) for _ in range(size)]


def _get_raw_compound(stream: "SupportsRead[bytes]") -> dict[bytes, RawTag]:
    result: dict[bytes, RawTag] = {}

    while tag_id := stream.read(1)[0]:
        try:
            parse_func = TAG_LUT[tag_id - 1]
        except IndexError:
            raise ValueError(f"Unknown tag id in Compound: {tag_id}") from None
        name_len = _U_SHORT.unpack(stream.read(2))[0]
        name = stream.read(name_len)
        result[name] = parse_func(stream)

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


def load_nbt_raw(stream: "SupportsRead[bytes]") -> dict[bytes, RawTag]:
    """Get the overall structure of a nbt file, while parsing as little of it as possible.

    Raises:
        EOFError: Unexpected end of file.
    """
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


DECOMP_FILE_LUT: Final[dict[int, Callable[["StrPath", Literal["rb"]], io.BufferedIOBase]]] = {
    3: open
}
"""This table mirrors DECOMP_LUT but streams decompression"""

# MCA Selector treats "no data" and "uncompressed" the same, so it is probably correct
DECOMP_FILE_LUT[0] = DECOMP_FILE_LUT[3]

try:
    import gzip
    import zlib
except ImportError:  # pragma: no cover
    pass
else:
    DECOMP_FILE_LUT[1] = gzip.open

    import sys
    from typing import Any, BinaryIO

    if sys.version_info >= (3, 14):  # pragma: no cover
        from compression._common._streams import DecompressReader
    else:
        from _compression import DecompressReader

    BUF_SIZE = 128 * 1024

    # TODO: this can be simplified once 3.11 is EOL using zlib._ZlibDecompressor
    class _ZlibReader(DecompressReader):
        def __init__(self, fp: BinaryIO):
            # copied from super class for better type checking
            self._fp = fp
            self._eof = False
            self._pos = 0  # Current offset in decompressed stream

            # Set to size of decompressed stream once it is known, for SEEK_END
            self._size = -1

            # Save the decompressor factory and arguments.
            # If the file contains multiple compressed streams, each
            # stream will need a separate decompressor object. A new decompressor
            # object is also needed when implementing a backwards seek().
            self._decomp_factory = zlib.decompressobj
            self._decomp_args: dict[str, Any] = {}
            self._decompressor = self._decomp_factory(**self._decomp_args)

        def read(self, size: int = -1) -> bytes:
            if size < 0:
                return self.readall()

            if not size or self._eof:
                return b""
            data: bytes | None = None  # Default if EOF is encountered
            # Depending on the input data, our call to the decompressor may not
            # return any data. In this case, try again after reading another block.
            while not data:
                rawblock = (
                    self._decompressor.unused_data
                    or self._decompressor.unconsumed_tail
                    or self._fp.read(BUF_SIZE)
                )
                if not rawblock:
                    if self._decompressor.eof:
                        break
                    raise EOFError(
                        "Compressed file ended before the end-of-stream marker was reached"
                    )
                if self._decompressor.eof:
                    # Continue to next stream.
                    self._decompressor = self._decomp_factory(**self._decomp_args)
                data = self._decompressor.decompress(rawblock, size)
            if not data:
                self._eof = True
                self._size = self._pos
                return b""
            self._pos += len(data)
            return data

        def close(self) -> None:
            self._fp.close()
            super().close()

    def _zlib_open(path: "StrPath", mode: Literal["rb"]) -> io.BufferedIOBase:
        return io.BufferedReader(_ZlibReader(open(path, mode, 0)), BUF_SIZE)

    DECOMP_FILE_LUT[2] = _zlib_open

try:
    import lz4.frame
except ImportError:  # pragma: no cover
    pass
else:
    DECOMP_FILE_LUT[4] = lz4.frame.open


def _load_add_exc_note(
    stream: "SupportsRead[bytes]", left: bool, exclude_last_update: bool
) -> dict[bytes, RawTag]:
    try:
        raw_nbt = load_nbt_raw(stream)
    except Exception as exc:
        exc.add_note(f"Occurred while parsing {'left' if left else 'right'}")
        raise exc
    if exclude_last_update:
        raw_nbt.pop(b"LastUpdate", None)
    return raw_nbt


def _py_compare_nbt(left: bytes, right: bytes, exclude_last_update: bool = False) -> bool:
    """Compare two NBT buffers."""
    left_nbt = _load_add_exc_note(io.BytesIO(left), True, exclude_last_update)
    right_nbt = _load_add_exc_note(io.BytesIO(right), False, exclude_last_update)
    return left_nbt == right_nbt


def _py_compare_nbt_files(
    left: "StrPath",
    left_comp_type: int,
    right: "StrPath",
    right_comp_type: int,
    exclude_last_update: bool = False,
) -> bool:
    """Compare two NBT files."""
    with (
        DECOMP_FILE_LUT[left_comp_type](left, "rb") as left_f,
        DECOMP_FILE_LUT[right_comp_type](right, "rb") as right_f,
    ):
        left_nbt = _load_add_exc_note(left_f, True, exclude_last_update)
        right_nbt = _load_add_exc_note(right_f, False, exclude_last_update)
        return left_nbt == right_nbt


try:
    from nbtcompare import compare as _rust_compare_nbt
except ImportError:  # pragma: no cover
    compare_nbt = _py_compare_nbt
    compare_nbt_files = _py_compare_nbt_files
else:

    def compare_nbt_files(
        left: "StrPath",
        left_comp_type: int,
        right: "StrPath",
        right_comp_type: int,
        exclude_last_update: bool = False,
    ) -> bool:
        """Compare two NBT files."""
        with (
            DECOMP_FILE_LUT[left_comp_type](left, "rb") as left_f,
            DECOMP_FILE_LUT[right_comp_type](right, "rb") as right_f,
        ):
            left_bytes = left_f.read()
            right_bytes = right_f.read()
        if len(left_bytes) != len(right_bytes):
            return False
        return _rust_compare_nbt(left_bytes, right_bytes, exclude_last_update)

    compare_nbt = _rust_compare_nbt
