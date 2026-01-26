"""This module parses the region file format as described in https://minecraft.wiki/w/Region_file_format"""

import contextlib
import io
import mmap
import operator
import struct
from collections.abc import Callable, Iterator
from dataclasses import dataclass
from typing import Self, ClassVar, TYPE_CHECKING, Final, NamedTuple

from .nbt import TAG_Compound, load_nbt, load_nbt_raw

if TYPE_CHECKING:
    from _typeshed import ReadableBuffer, WriteableBuffer, StrOrBytesPath
    from types import TracebackType

DECOMP_LUT: Final[dict[int, Callable[["ReadableBuffer"], "ReadableBuffer"]]] = {3: lambda v: v}
"""chunk compression schemes according to https://minecraft.wiki/w/Region_file_format#Payload

Documented but unsupported:
  - 127: Custom compression algorithm
  - x + 128: the compressed data is saved in a file called c.x.z.mcc, where x and z are the chunk's
    coordinates, instead of the usual position.
"""

try:
    import zlib, gzip

    DECOMP_LUT[1] = gzip.decompress
    DECOMP_LUT[2] = zlib.decompress
except ImportError:
    pass

try:
    import lz4.frame  # type: ignore[import, unused-ignore]

    DECOMP_LUT[4] = lz4.frame.decompress
except ImportError:
    pass

SECTOR: Final = 2**12
"""4 KiB"""


class ChunkLoadingError(Exception):
    pass


class ChangesReport(NamedTuple):
    created: list[int]
    deleted: list[int]
    modified: list[int]
    moved: list[int]
    touched: int


@dataclass(slots=True, order=True)
class ChunkHeader:
    """this class represents rows in the 8KiB header of a region file"""

    _table_struct: ClassVar[struct.Struct] = struct.Struct("!I")

    offset: int
    """chunk offset in sectors. 0 if not created, 1 if unmodified"""
    size: int
    """chunk size rounded up, in sectors 0 if not created"""
    mtime: int
    """last modification time in seconds since epoch"""

    @classmethod
    def load(cls, buf: "ReadableBuffer", offset: int) -> Self:
        """load chunk header from a readable ``buf`` starting at ``offset``"""
        return cls(
            *divmod(cls._table_struct.unpack_from(buf, offset)[0], 256),
            *cls._table_struct.unpack_from(buf, offset + SECTOR),
        )

    def dump(self, buf: "WriteableBuffer", offset: int) -> None:
        """dump chunk header to a writeable ``buf`` starting at ``offset``"""
        self._table_struct.pack_into(buf, offset, (self.offset << 8) + self.size)
        self._table_struct.pack_into(buf, offset + SECTOR, self.mtime)

    @property
    def unmodified(self) -> bool:
        """whether this chunk is marked as unmodified.

        Always false for vanilla region files, this can only occur in diffs
        """
        return self.offset == 1 and not self.size

    @unmodified.setter
    def unmodified(self, value: bool) -> None:
        assert value, "Can't be set to False"
        self.offset = 1
        self.size = 0

    @property
    def not_created(self) -> bool:
        return self.offset == self.size == 0

    @not_created.setter
    def not_created(self, value: bool) -> None:
        assert value, "Can't be set to False"
        self.offset = self.size = 0


class RegionFile:
    __slots__ = ("_fd", "_mmap", "_headers", "_headers_changed")

    _chunk_heading_struct: Final = struct.Struct("!iB")

    def __init__(self, fd: int):
        self._fd = fd
        self._mmap: mmap.mmap
        self._headers: list[ChunkHeader] = []
        self._headers_changed = False

    def __enter__(self) -> Self:
        self._mmap = mmap.mmap(self._fd, 0, access=mmap.ACCESS_WRITE)
        if not self._headers:
            self.load_headers()
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: "TracebackType | None",
    ) -> None:
        self.dump_headers()
        self._mmap.close()
        del self._mmap

    def __len__(self) -> int:
        return len(self._mmap)

    @classmethod
    @contextlib.contextmanager
    def open(cls, file: "StrOrBytesPath") -> Iterator[Self]:
        with open(file, "r+b", 0) as f, cls(f.fileno()) as region:
            yield region

    def load_headers(self) -> None:
        # if not len(self._mmap):
        #     self._headers = [ChunkHeader(0, 0, 0) for _ in range(1024)]
        self._headers = [ChunkHeader.load(self._mmap, offset) for offset in range(0, SECTOR, 4)]

    def dump_headers(self) -> None:
        if self._headers_changed:
            for idx, header in enumerate(self._headers):
                header.dump(self._mmap, idx * 4)
        self._headers_changed = False

    def _get_chunk_data(self, header: ChunkHeader) -> io.BytesIO:
        start = header.offset * SECTOR
        assert not (header.not_created or header.unmodified)
        size, comp_type = self._chunk_heading_struct.unpack_from(self._mmap, start)
        start += 5  # actual chunk data starts here
        decompressor = DECOMP_LUT[comp_type]
        with memoryview(self._mmap) as v, v[start : start + size - 1] as view:
            return io.BytesIO(decompressor(view))

    def get_chunk_nbt(self, idx: int) -> TAG_Compound:
        header = self._headers[idx]
        return load_nbt(self._get_chunk_data(header))

    def _check_unchanged(
        self, this_header: ChunkHeader, other: Self, other_header: ChunkHeader, is_chunk: bool
    ) -> bool:
        if this_header.mtime == other_header.mtime:
            return True
        this_data = self._get_chunk_data(this_header)
        other_data = other._get_chunk_data(other_header)
        if len(this_data.getbuffer()) != len(other_data.getbuffer()):
            return False
        this_nbt = load_nbt_raw(this_data)
        other_nbt = load_nbt_raw(other_data)
        if is_chunk:
            other_nbt[b"LastUpdate"] = this_nbt[b"LastUpdate"]
        return this_nbt == other_nbt

    def density(self) -> float:
        return SECTOR * (sum(header.size for header in self._headers) + 2) / len(self)

    def defragment(self) -> None:
        prev_end = 2
        # noinspection PyTypeChecker
        for header in sorted(self._headers):
            if header.not_created or header.unmodified:
                continue
            prev_end = self._move_chunk_back(prev_end, header)
        self._mmap.resize(prev_end * SECTOR)

    def filter_diff_defragment(self, other: Self, is_chunk: bool = False) -> bool:
        """drop all chunks in common with other and defragment the file
        :param other: other region to compare against
        :param is_chunk: if the region stores chunks
        :return: True if the regions are identical
        """
        prev_end = 2
        for this_header, other_header in sorted(
            zip(self._headers, other._headers), key=operator.itemgetter(0)
        ):
            if this_header.not_created or this_header.unmodified:
                continue
            if not (other_header.not_created or other_header.unmodified) and self._check_unchanged(
                this_header, other, other_header, is_chunk
            ):
                this_header.unmodified = True
                self._headers_changed = True
            else:  # defragment
                prev_end = self._move_chunk_back(prev_end, this_header)

        self._mmap.resize(prev_end * SECTOR)
        return prev_end == 2

    def _move_chunk_back(self, prev_end: int, header: ChunkHeader) -> int:
        if header.offset > prev_end:
            self._mmap.move(prev_end * SECTOR, header.offset * SECTOR, header.size * SECTOR)
            header.offset = prev_end
            self._headers_changed = True
        elif header.offset < prev_end:
            raise ChunkLoadingError("overlapping chunks")
        return header.offset + header.size

    def apply_diff(self, other: Self, defragment: bool = False) -> None:
        """apply changes from other to self

        when restoring backups, older should be applied to newer."""
        # TODO: use os.sendfile or os.copy_file_range for improved performance (not
        #  available on windows but winapi probably has something similar)
        to_be_copied: list[tuple[ChunkHeader, ChunkHeader]] = []
        added_size = 0
        self._headers_changed = True
        with memoryview(other._mmap) as other_view:
            for this_header, other_header in zip(self._headers, other._headers):
                this_header.mtime = other_header.mtime
                if other_header.unmodified:
                    continue
                if other_header.not_created:
                    this_header.not_created = True
                elif other_header.size <= this_header.size:
                    # we can fit the new chunk where the old one was
                    self._mmap.seek(this_header.offset * SECTOR)
                    this_header.size = other_header.size
                    self._copy_chunk(other_header, other_view)
                else:
                    # new one will be appended to the end
                    to_be_copied.append((this_header, other_header))
                    added_size += other_header.size * SECTOR
                    this_header.size = this_header.offset = 0  # set to 0 for defrag

            if defragment:
                self.defragment()
            if not added_size:
                # nothing to append
                return
            # this returns none until 3.13
            # otherwise, we could write added_size += self._mmap.seek(0, os.SEEK_END)
            self._mmap.seek(0, 2)
            added_size += len(self)
            self._mmap.resize(added_size)
            for this_header, other_header in to_be_copied:
                this_header.offset = self._mmap.tell() // SECTOR
                this_header.size = other_header.size
                self._copy_chunk(other_header, other_view)

    # noinspection PyUnresolvedReferences
    def _copy_chunk(self, other_header: ChunkHeader, other_view: "memoryview[int]") -> None:
        other_start = other_header.offset
        other_end = other_start + other_header.size
        # uses memcpy under the hood
        with other_view[other_start * SECTOR : other_end * SECTOR] as mv:
            self._mmap.write(mv)

    def report_diff(self, other: Self, is_chunk: bool = False) -> ChangesReport:
        """report changes between self and other"""
        deleted = []
        """present in other but not self"""
        created = []
        """present in self but not other"""
        modified = []
        """changed between self and other"""
        touched = 0
        """ how many chunks were marked as modified but are actually unmodified"""
        moved = []
        """changed offset"""

        for idx, (this_header, other_header) in enumerate(zip(self._headers, other._headers)):
            if this_header.unmodified or other_header.unmodified:
                continue
            if this_header.not_created:
                if not other_header.not_created:
                    deleted.append(idx)
                continue
            if other_header.not_created:
                created.append(idx)
                continue
            if this_header.offset != other_header.offset:
                moved.append(idx)
            if self._check_unchanged(this_header, other, other_header, is_chunk):
                if this_header.mtime != other_header.mtime:
                    touched += 1
                continue
            modified.append(idx)
        return ChangesReport(created, deleted, modified, moved, touched)
