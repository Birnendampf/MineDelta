"""Parse the region file format as described in https://minecraft.wiki/w/Region_file_format.

This module references region files but actually operates on anvil files as well.
The term "chunk" is used rather loosely as entities abd POIs are also stored on a per-chunk basis
"""

import contextlib
import mmap
import operator
import struct
from collections.abc import Callable
from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING, ClassVar, Final, Literal, NamedTuple, Self

from .nbt import compare_nbt, compare_nbt_files

if TYPE_CHECKING:
    import io

    from _typeshed import ReadableBuffer, StrPath, Unused, WriteableBuffer

__all__ = [
    "DECOMP_LUT",
    "SECTOR",
    "ChangesReport",
    "CorruptedRegionError",
    "EmptyRegionError",
    "RegionError",
    "RegionFile",
    "RegionLoadingError",
]

DECOMP_LUT: Final[dict[int, Callable[["ReadableBuffer"], bytes]]] = {3: bytes}
"""chunk compression schemes according to https://minecraft.wiki/w/Region_file_format#Payload

Documented but unsupported:
  - 127: Custom compression algorithm
  - x + 128: the compressed data is saved in a file called c.x.z.mcc, where x and z are the chunk's
    coordinates, instead of the usual position.
"""

# MCA Selector treats "no data" and "uncompressed" the same, so it is probably correct
DECOMP_LUT[0] = DECOMP_LUT[3]

with contextlib.suppress(ImportError):
    import gzip
    import zlib

    DECOMP_LUT[1] = gzip.decompress
    DECOMP_LUT[2] = zlib.decompress

with contextlib.suppress(ImportError):
    import lz4.frame

    DECOMP_LUT[4] = lz4.frame.decompress


SECTOR: Final = 2**12
"""4 KiB"""


class RegionError(Exception):
    """Base class for all region-related errors."""


class RegionLoadingError(RegionError):
    """Something is wrong with the region file."""


class ChunkLoadingError(RegionLoadingError):
    """A chunk in a region file could not be loaded."""


class EmptyRegionError(RegionLoadingError):
    """The region file is empty."""


class CorruptedRegionError(RegionError):
    """The region file appears corrupted."""


class ChangesReport(NamedTuple):
    """Summary of differences between two region files.

    Attributes:
        created: Indices of chunks present in this file but not other.
        deleted: Indices of chunks present in other but not this file.
        modified: Indices of chunks that differ.
        moved: Indices of chunks that have been moved.
        touched: How many chunks were marked as modified but are actually unmodified.
    """

    created: list[int]
    deleted: list[int]
    modified: list[int]
    moved: list[int]
    touched: int


@dataclass(slots=True, order=True)
class ChunkHeader:
    """this class represents rows in the 8KiB header of a region file.

    Attributes:
        offset: Chunk offset in sectors. 0 if not created, 1 if unmodified.
        size: Chunk size in sectors. 0 if not created or unmodified.
        mtime: Last modification time in seconds since epoch.
    """

    _table_struct: ClassVar[struct.Struct] = struct.Struct("!I")

    offset: int
    size: int
    mtime: int
    external: bool = field(default=False, init=False, compare=False)

    @classmethod
    def load(cls, buf: "ReadableBuffer", offset: int) -> Self:
        """Load chunk header from a readable `buf` starting at `offset`."""
        return cls(
            *divmod(cls._table_struct.unpack_from(buf, offset)[0], 256),
            *cls._table_struct.unpack_from(buf, offset + SECTOR),
        )

    def dump(self, buf: "WriteableBuffer", offset: int) -> None:
        """Dump chunk header to a writeable `buf` starting at `offset`."""
        self._table_struct.pack_into(buf, offset, (self.offset << 8) + self.size)
        self._table_struct.pack_into(buf, offset + SECTOR, self.mtime)

    @property
    def unmodified(self) -> bool:
        """Whether this chunk is marked as unmodified.

        Always false for vanilla region files, this can only occur in diffs
        """
        return self.offset == 1 and not self.size

    @unmodified.setter
    def unmodified(self, value: Literal[True]) -> None:
        """Mark chunk as unmodified.

        Cannot be set to False.
        """
        if not value:  # pragma: no cover
            raise ValueError("Can't be set to False")
        self.offset = 1
        self.size = 0

    @property
    def not_created(self) -> bool:
        """Whether this chunk has not been created yet."""
        return self.offset == self.size == 0

    @not_created.setter
    def not_created(self, value: Literal[True]) -> None:
        """Mark chunk as not created.

        Cannot be set to False.
        """
        if not value:  # pragma: no cover
            raise ValueError("Can't be set to False")
        self.offset = self.size = 0


class RegionFile:
    """Contains methods for interacting with files in the anvil/region file format.

    It uses mmap under the hood and can therefor not be used on empty files.

    This class can be used as a reusable context manager,
    """

    __slots__ = ("_file_obj", "_headers", "_headers_changed", "_mmap", "_path", "_region_pos")

    _chunk_heading_struct: Final = struct.Struct("!iB")

    def __init__(self, path: "StrPath"):
        """Create a new `RegionFile` object.

        This does not load any data yet, so most methods will raise `AttributeError`.

        Args:
            path: The region file that will be opened when entering the context.
        """
        self._path = path
        self._mmap: mmap.mmap
        self._headers: list[ChunkHeader] = []
        self._headers_changed = False
        self._file_obj: io.FileIO
        self._region_pos: tuple[int, int]

    def __enter__(self) -> Self:
        """Map the region file into memory and load its headers.

        Returns: The RegionFile object.

        Raises:
            ValueError: The region file is empty.
            ChunkLoadingError: Chunk headers could not be loaded.
            RuntimeError: The context has already been entered.
        """
        if hasattr(self, "_mmap"):
            raise RuntimeError("Already loaded")
        self._file_obj = open(self._path, "r+b", 0)
        try:
            try:
                self._mmap = mmap.mmap(self._file_obj.fileno(), 0, access=mmap.ACCESS_WRITE)
            except ValueError as e:
                raise EmptyRegionError from e
            if not self._headers:
                self.load_headers()
        except Exception:
            self._file_obj.close()
            raise
        return self

    def __exit__(self, *_: "Unused") -> None:
        """Write chunk headers back to the file and release the mapping."""
        self.dump_headers()
        self._mmap.close()
        self._file_obj.close()
        del self._mmap, self._file_obj

    def __len__(self) -> int:
        """The length of the region file."""
        return len(self._mmap)

    def load_headers(self) -> None:
        """Load the region file headers.

        Raises:
            ChunkLoadingError: Chunk headers could not be loaded.
        """
        try:
            self._headers = [ChunkHeader.load(self._mmap, offset) for offset in range(0, SECTOR, 4)]
        except struct.error as e:
            raise RegionLoadingError("Chunk headers appear truncated") from e

    def dump_headers(self) -> None:
        """Write the chunk headers back to the file if they changed."""
        if self._headers_changed:
            for idx, header in enumerate(self._headers):
                header.dump(self._mmap, idx * 4)
        self._headers_changed = False

    def get_mcc(self, idx: int) -> Path:
        """Get the mcc file for the given header index."""
        path = Path(self._path)
        if hasattr(self, "_region_pos"):
            region_x, region_z = self._region_pos
        else:
            _, region_x_str, region_z_str = path.stem.split(".")
            # noinspection PyAttributeOutsideInit
            region_x, region_z = self._region_pos = int(region_x_str) * 32, int(region_z_str) * 32
        chunk_rel_z, chunk_rel_x = divmod(idx, 32)
        return path.with_name(f"c.{chunk_rel_x + region_x}.{chunk_rel_z + region_z}.mcc")

    def _get_start_size_comp_type(self, header: ChunkHeader) -> tuple[int, int, int]:
        start = header.offset * SECTOR
        if header.not_created or header.unmodified:
            raise ChunkLoadingError("Chunk not created or unmodified")
        size, comp_type = self._chunk_heading_struct.unpack_from(self._mmap, start)
        start += 5  # actual chunk data starts here
        return start, size, comp_type

    def _get_inner_chunk_data(self, start: int, size: int, comp_type: int) -> bytes:
        try:
            decompressor = DECOMP_LUT[comp_type]
        except KeyError:
            raise ChunkLoadingError(f"Unknown compression type: {comp_type}") from None
        with memoryview(self._mmap)[start : start + size - 1] as view:
            return decompressor(view)

    def _check_unchanged(
        self,
        this_header: ChunkHeader,
        other: Self,
        other_header: ChunkHeader,
        is_chunk: bool,
        idx: int,
    ) -> bool:
        if this_header.mtime == other_header.mtime:
            return True
        this_start, this_size, this_comp_type = self._get_start_size_comp_type(this_header)
        other_start, other_size, other_comp_type = other._get_start_size_comp_type(other_header)
        external = this_comp_type > 127
        if external != (other_comp_type > 127):
            # if only one of two is external, it means different size and therefor different content
            # This might lead to false negatives when a chunk is right at the limit of being
            # external and the order of nbt tags lead to different compression and consequentially
            # different length
            return False
        this_header.external = external
        if external:
            return compare_nbt_files(
                self.get_mcc(idx),
                this_comp_type - 128,
                other.get_mcc(idx),
                other_comp_type - 128,
                is_chunk,
            )
        this_data = self._get_inner_chunk_data(this_start, this_size, this_comp_type)
        other_data = other._get_inner_chunk_data(other_start, other_size, other_comp_type)
        if len(this_data) != len(other_data):
            return False
        return compare_nbt(this_data, other_data, is_chunk)

    def density(self) -> float:
        """Return the ratio of used space to file size in this region.

        Mainly useful for debugging.
        """
        return SECTOR * (sum(header.size for header in self._headers) + 2) / len(self)

    def defragment(self) -> None:
        """Move chunks back to fill any gaps and truncate the file.

        after this operation, `density()` will return `1.0`.

        Raises:
            ChunkLoadingError: Overlapping chunks were detected.
        """
        prev_end = 2
        # noinspection PyTypeChecker
        for header in sorted(self._headers):
            if header.not_created or header.unmodified:
                continue
            prev_end = self._move_chunk_back(prev_end, header)
        self._mmap.resize(prev_end * SECTOR)

    def filter_diff_defragment(self, other: Self, is_chunk: bool = False) -> bool:
        """Drop all chunks in common with other and defragment the file.

        Args:
            other: other region to compare against.
            is_chunk: if the region stores chunks.

        Returns:
            Whether the regions are identical.

        Raises:
            ChunkLoadingError: Overlapping chunks were detected.
        """
        prev_end = 2
        no_missing_chunks = True
        for this_header, (idx, other_header) in sorted(
            zip(self._headers, enumerate(other._headers), strict=True), key=operator.itemgetter(0)
        ):
            if this_header.unmodified:
                continue
            if this_header.not_created:
                if not other_header.not_created:
                    no_missing_chunks = False
                continue
            if not (other_header.not_created or other_header.unmodified) and self._check_unchanged(
                this_header, other, other_header, is_chunk, idx
            ):
                this_header.unmodified = True
                self._headers_changed = True
            else:  # defragment
                prev_end = self._move_chunk_back(prev_end, this_header)

        self._mmap.resize(prev_end * SECTOR)
        return no_missing_chunks and prev_end == 2

    def _move_chunk_back(self, prev_end: int, header: ChunkHeader) -> int:
        if header.offset > prev_end:
            self._mmap.move(prev_end * SECTOR, header.offset * SECTOR, header.size * SECTOR)
            header.offset = prev_end
            self._headers_changed = True
        elif header.offset < prev_end:
            raise CorruptedRegionError("overlapping chunks")
        return header.offset + header.size

    def apply_diff(self, other: Self, defragment: bool = False) -> None:
        """Apply changes from other to self.

        when restoring backups, older should be applied to newer.

        Args:
            other: Region to apply changes from
            defragment: Whether the file should also be defragmented
        """
        # we could use os.sendfile or os.copy_file_range for improved performance (not available on
        # Windows but winapi probably has something similar)
        to_be_copied: list[tuple[ChunkHeader, ChunkHeader]] = []
        added_size = 0
        self._headers_changed = True
        with memoryview(other._mmap) as other_view:
            for this_header, other_header in zip(self._headers, other._headers, strict=True):
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
    def _copy_chunk(self, other_header: ChunkHeader, other_view: memoryview) -> None:
        other_start = other_header.offset
        other_end = other_start + other_header.size
        # uses memcpy under the hood
        with other_view[other_start * SECTOR : other_end * SECTOR] as mv:
            self._mmap.write(mv)

    def report_diff(self, other: Self, is_chunk: bool = False) -> ChangesReport:  # pragma: no cover
        """Report changes between self and other."""
        deleted = []
        created = []
        modified = []
        touched = 0
        moved = []

        for idx, (this_header, other_header) in enumerate(
            zip(self._headers, other._headers, strict=True)
        ):
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
            if self._check_unchanged(this_header, other, other_header, is_chunk, idx):
                if this_header.mtime != other_header.mtime:
                    touched += 1
                continue
            modified.append(idx)
        return ChangesReport(created, deleted, modified, moved, touched)
