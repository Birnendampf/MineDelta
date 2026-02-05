# ruff: noqa: D101
"""Contains rudimentary NBT parsers and classes representing NBT types."""

import abc
import io
import struct
import sys
from abc import ABCMeta
from typing import TYPE_CHECKING, Any, ClassVar, Generic, Self, TypeAlias, TypeVar, cast

if sys.version_info >= (3, 12):
    from typing import override
else:
    from typing_extensions import override

if TYPE_CHECKING:
    from _typeshed import ReadableBuffer, SupportsRead, SupportsWrite


__all__ = [
    "RawCompound",
    "TAG_Byte",
    "TAG_Byte_Array",
    "TAG_Compound",
    "TAG_Double",
    "TAG_Float",
    "TAG_Int",
    "TAG_Int_Array",
    "TAG_List",
    "TAG_Long",
    "TAG_Long_Array",
    "TAG_Short",
    "TAG_String",
    "compare_nbt",
    "load_nbt",
    "load_nbt_raw",
]

T = TypeVar("T")

RawCompound: TypeAlias = bytes | dict[bytes, "RawCompound"] | list["RawCompound"]


class Tag(Generic[T], metaclass=abc.ABCMeta):  # noqa: PLW1641
    """Base class for all NBT tags."""

    __slots__ = ("value",)

    def __init__(self, value: T):
        """Create a new NBT tag with the specified value."""
        self.value = value

    def __eq__(self, other: object) -> bool:
        """Check for equality in the same way dataclass does."""
        if self is other:
            return True
        if other.__class__ is self.__class__:  # this is what dataclass does
            return self.value == other.value  # type: ignore[attr-defined, no-any-return]
        return NotImplemented

    @classmethod
    @abc.abstractmethod
    def load(cls, stream: "SupportsRead[bytes]") -> Self:
        """Load a tag from the specified byte stream."""

    @abc.abstractmethod
    def dumps(self) -> bytes:
        """Return the tag's binary representation."""

    def dump(self, stream: "SupportsWrite[bytes]") -> None:
        """Dump the tag's binary representation to stream."""
        stream.write(self.dumps())

    def __repr__(self) -> str:
        """String representation of the tag. Copied from dataclass. NOT SNBT."""
        return f"{self.__class__.__qualname__}({self.value!r})"

    @abc.abstractmethod
    def _snbt_helper(self, indent_so_far: int, indent: int, depth: int) -> str:
        pass

    def snbt(self, indent: int = 0, depth: int = -1) -> str:
        """Return the SNBT representation of the tag."""
        return self._snbt_helper(0, indent, depth)

    @classmethod
    @abc.abstractmethod
    def get_raw(cls, stream: "SupportsRead[bytes]") -> RawCompound:
        """Get a binary representation of the tag while parsing as little as possible."""


class _Numeric(Tag[int], metaclass=ABCMeta):
    __slots__ = ()
    _size: ClassVar[int]
    _format_char: ClassVar[str]

    @classmethod
    def load(cls, stream: "SupportsRead[bytes]") -> Self:
        return cls(int.from_bytes(stream.read(cls._size), signed=True))

    def dumps(self) -> bytes:
        return self.value.to_bytes(self._size, signed=True)

    def _snbt_helper(self, indent_so_far: int, indent: int, depth: int) -> str:
        return f"{self.value}{self._format_char}"

    @classmethod
    def get_raw(cls, stream: "SupportsRead[bytes]") -> bytes:
        return stream.read(cls._size)


class TAG_Byte(_Numeric):
    __slots__ = ()
    _size = 1
    _format_char = "b"


class TAG_Short(_Numeric):
    __slots__ = ()
    _size = 2
    _format_char = "s"


class TAG_Int(_Numeric):
    __slots__ = ()
    _size = 4
    _format_char = "i"


class TAG_Long(_Numeric):
    __slots__ = ()
    _size = 8
    _format_char = "l"


class TAG_Float(Tag[float]):
    __slots__ = ()
    _struct: ClassVar[struct.Struct] = struct.Struct("!f")

    @override
    @classmethod
    def load(cls, stream: "SupportsRead[bytes]") -> Self:
        return cls(cls._struct.unpack(stream.read(cls._struct.size))[0])

    @override
    def dumps(self) -> bytes:
        return self._struct.pack(self.value)

    def _snbt_helper(self, indent_so_far: int, indent: int, depth: int) -> str:
        return f"{self.value}{self._struct.format[1:]}"

    @override
    @classmethod
    def get_raw(cls, stream: "SupportsRead[bytes]") -> bytes:
        return stream.read(cls._struct.size)


class TAG_Double(TAG_Float):
    __slots__ = ()
    _struct = struct.Struct("!d")


Numeric_T = TypeVar("Numeric_T", bound=_Numeric)


class _Array(Tag[list[Numeric_T]], metaclass=abc.ABCMeta):
    __slots__ = ()
    _type: ClassVar[type[Numeric_T]]

    @classmethod
    def load(cls, stream: "SupportsRead[bytes]") -> Self:
        size = int.from_bytes(stream.read(4), signed=True)
        type_load = cls._type.load
        return cls([type_load(stream) for _ in range(size)])

    def dumps(self) -> bytes:
        length = len(self.value).to_bytes(4, signed=True)
        payload = b"".join(elem.dumps() for elem in self.value)
        return length + payload

    def dump(self, stream: "SupportsWrite[bytes]") -> None:
        stream.write(len(self.value).to_bytes(4, signed=True))
        for elem in self.value:
            elem.dump(stream)

    def _snbt_helper(self, indent_so_far: int, indent: int, depth: int) -> str:
        if not depth:
            return f"['{self._type._format_char.upper()}' x{len(self.value)}]"
        values = ",".join(elem._snbt_helper(indent_so_far, indent, depth) for elem in self.value)
        return f"[{self._type._format_char.upper()};{values}]"

    @classmethod
    def get_raw(cls, stream: "SupportsRead[bytes]") -> bytes:
        length = int.from_bytes(stream.read(4))
        return stream.read(length * cls._type._size)


class TAG_Byte_Array(_Array[TAG_Byte]):
    __slots__ = ()
    _type = TAG_Byte


class TAG_String(Tag[str]):
    __slots__ = ()

    @override
    @classmethod
    def load(cls, stream: "SupportsRead[bytes]") -> Self:
        size = int.from_bytes(stream.read(2))
        return cls((stream.read(size)).decode("utf-8"))

    @override
    def dumps(self) -> bytes:
        size = len(self.value).to_bytes(2)
        return size + self.value.encode("utf-8")

    def _snbt_helper(self, indent_so_far: int, indent: int, depth: int) -> str:
        return repr(self.value)

    @override
    @classmethod
    def get_raw(cls, stream: "SupportsRead[bytes]") -> bytes:
        return stream.read(int.from_bytes(stream.read(2)))


Tag_T = TypeVar("Tag_T", bound=Tag[Any])


class TAG_List(Tag[list[Tag_T]]):
    __slots__ = ("value_type",)

    def __init__(self, value: list[Tag_T], val_type: type[Tag_T] | None):
        """Create a new list TAG with the specified values of the specified type."""
        super().__init__(value)
        self.value_type = val_type

    @override
    @classmethod
    def load(cls, stream: "SupportsRead[bytes]") -> Self:
        tag_id = stream.read(1)[0]
        size = int.from_bytes(stream.read(4))
        # this is possible without a cast, but its ugly and less flexible
        TAG_class = cast("type[Tag_T] | None", TAG_LUT[tag_id])
        if not TAG_class:
            stream.read(size)
            return cls([], None)
        tag_class_load = TAG_class.load
        return cls([tag_class_load(stream) for _ in range(size)], TAG_class)

    @override
    def dumps(self) -> bytes:
        tag_id = TAG_ID_LUT[self.value_type]
        return (
            tag_id.to_bytes(1)
            + len(self.value).to_bytes(4)
            + b"".join(elem.dumps() for elem in self.value)
        )

    @override
    def dump(self, stream: "SupportsWrite[bytes]") -> None:
        tag_id = TAG_ID_LUT[self.value_type]
        stream.write(tag_id.to_bytes(1))
        stream.write(len(self.value).to_bytes(4))
        for elem in self.value:
            elem.dump(stream)

    def _snbt_helper(self, indent_so_far: int, indent: int, depth: int) -> str:
        if self.value_type is None or not self.value:
            return "[]"
        if not depth:
            return f"[<{self.value_type.__qualname__}> x{len(self.value)}]"
        depth -= 1
        next_indent = indent_so_far + indent
        str_array = ["["]
        str_indent = "\n" + " " * next_indent if indent else ""
        for elem in self.value:
            str_array.append(str_indent)
            str_array.append(elem._snbt_helper(next_indent, indent, depth))
            str_array.append(",")
        str_array.pop()
        str_array.append(str_indent[:-indent])
        str_array.append("]")
        return "".join(str_array)

    @override
    @classmethod
    def get_raw(cls, stream: "SupportsRead[bytes]") -> bytes | list[RawCompound]:
        tag_id = stream.read(1)[0]
        size = int.from_bytes(stream.read(4))
        if tag_id < 7:
            tag_size = TAG_SIZE_LUT[tag_id]
            return stream.read(size * tag_size)
        # TAG_LUT[tag_id] can't be none at this point but mypy doesn't know that. This is a hot code
        # path so a cast is used instead of assert because it's faster at runtime
        tag_class_get_raw = cast("type[Tag[Any]]", TAG_LUT[tag_id]).get_raw
        return [tag_class_get_raw(stream) for _ in range(size)]


class TAG_Compound(Tag[dict[str, Tag[Any]]]):
    __slots__ = ()

    @override
    @classmethod
    def load(cls, stream: "SupportsRead[bytes]") -> Self:
        result: dict[str, Tag[Any]] = {}
        stream_read = stream.read
        int_from_bytes = int.from_bytes
        while TAG_class := TAG_LUT[stream_read(1)[0]]:
            name_size = int_from_bytes(stream_read(2))
            name = (stream_read(name_size)).decode("utf-8")
            result[name] = TAG_class.load(stream)

        return cls(result)

    @override
    def dumps(self) -> bytes:
        result: list[bytes] = []
        result_append = result.append
        for key, value in self.value.items():
            result_append(TAG_ID_LUT[type(value)].to_bytes(1))  # tag_id
            result_append(len(key).to_bytes(2))  # name_size
            result_append(key.encode("utf-8"))
            result_append(value.dumps())
        return b"".join(result)

    @override
    def dump(self, stream: "SupportsWrite[bytes]") -> None:
        stream_write = stream.write
        for key, value in self.value.items():
            stream_write(TAG_ID_LUT[type(value)].to_bytes(1))
            stream_write(len(key).to_bytes(2))
            stream_write(key.encode("utf-8"))
            value.dump(stream)
        stream_write(b"\0")

    def _snbt_helper(self, indent_so_far: int, indent: int, depth: int) -> str:
        if not self.value:
            return "{}"
        if not depth:
            return "{...}"
        depth -= 1
        indent_so_far += indent
        str_array = ["{"]
        if indent:
            str_indent = "\n" + " " * indent_so_far
            key_val_sep = ": "
        else:
            str_indent = ""
            key_val_sep = ":"
        for key, value in self.value.items():
            str_array.append(str_indent)
            str_array.append(repr(key))
            str_array.append(key_val_sep)
            str_array.append(value._snbt_helper(indent_so_far, indent, depth))
            str_array.append(",")
        str_array.pop()
        str_array.extend(str_indent[:-indent])
        str_array.append("}")
        return "".join(str_array)

    @override
    @classmethod
    def get_raw(cls, stream: "SupportsRead[bytes]") -> dict[bytes, RawCompound]:
        result: dict[bytes, RawCompound] = {}
        stream_read = stream.read
        int_from_bytes = int.from_bytes
        while TAG_class := TAG_LUT[stream_read(1)[0]]:
            raw_name = stream_read(int_from_bytes(stream_read(2)))
            result[raw_name] = TAG_class.get_raw(stream)
        return result


class TAG_Int_Array(_Array[TAG_Int]):
    _type = TAG_Int
    __slots__ = ()


class TAG_Long_Array(_Array[TAG_Long]):
    _type = TAG_Long
    __slots__ = ()


TAG_LUT = (
    None,
    TAG_Byte,
    TAG_Short,
    TAG_Int,
    TAG_Long,
    TAG_Float,
    TAG_Double,
    TAG_Byte_Array,
    TAG_String,
    TAG_List,
    TAG_Compound,
    TAG_Int_Array,
    TAG_Long_Array,
)

TAG_ID_LUT: dict[type[Tag[Any]] | None, int] = {elem: idx for idx, elem in enumerate(TAG_LUT)}

TAG_SIZE_LUT = [0, 1, 2, 4, 8, 4, 8]


def _ensure_root_compound(stream: "SupportsRead[bytes]") -> None:
    if stream.read(1) != b"\x0a":
        raise ValueError("Root TAG is not Compound")
    name_len = int.from_bytes(stream.read(2))
    stream.read(name_len)


def load_nbt_raw(stream: "SupportsRead[bytes]") -> dict[bytes, RawCompound]:
    """Get the overall structure of a nbt file, while parsing as little of it as possible."""
    _ensure_root_compound(stream)
    return TAG_Compound.get_raw(stream)


def load_nbt(stream: "SupportsRead[bytes]") -> TAG_Compound:
    """Load NBT data from  a byte stream."""
    _ensure_root_compound(stream)
    return TAG_Compound.load(stream)


try:
    import rapidnbt
except ImportError:

    def compare_nbt(buffer1: "ReadableBuffer", buffer2: "ReadableBuffer", is_chunk: bool) -> bool:
        """Compare two NBT files."""
        this_nbt = load_nbt_raw(io.BytesIO(buffer1))
        other_nbt = load_nbt_raw(io.BytesIO(buffer2))
        if is_chunk:
            other_nbt[b"LastUpdate"] = this_nbt[b"LastUpdate"]
        return this_nbt == other_nbt
else:

    def compare_nbt(buffer1: "ReadableBuffer", buffer2: "ReadableBuffer", is_chunk: bool) -> bool:
        """Compare two NBT files."""
        this_nbt = rapidnbt.nbtio.loads(buffer1, rapidnbt.NbtFileFormat.BIG_ENDIAN)
        other_nbt = rapidnbt.nbtio.loads(buffer2, rapidnbt.NbtFileFormat.BIG_ENDIAN)
        if this_nbt is None or other_nbt is None:
            raise ValueError("Failed to load NBT")
        if is_chunk:
            other_nbt["LastUpdate"] = this_nbt["LastUpdate"]
        return this_nbt == other_nbt
