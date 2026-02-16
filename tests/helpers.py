"""Helper functions for tests."""

import enum
import math
from pathlib import Path

import lz4.frame
import rapidnbt

from minedelta.region import SECTOR


class Compression(enum.IntEnum):
    """Compression schemes available in region files.

    See Also: https://minecraft.wiki/w/Region_file_format#Payload
    """

    GZIP = 1
    ZLIB = 2
    UNCOMPRESSED = 3
    LZ4 = 4

    __str__ = enum.Enum.__str__


def generate_bare_region_file(path: Path) -> None:
    """Create a bare region file for testing."""
    with open(path, "wb") as f:
        f.truncate(8192)


def write_nbt_to_region_file(  # noqa: PLR0913
    path: Path,
    idx: int,
    timestamp: int,
    tag: rapidnbt.CompoundTag,
    compression: Compression = Compression.ZLIB,
    external: bool = False,
) -> None:
    """Write a compound tag to region file."""
    payload: bytes
    if compression is Compression.LZ4:
        payload = lz4.frame.compress(
            rapidnbt.nbtio.dumps(
                tag, rapidnbt.NbtFileFormat.BIG_ENDIAN, rapidnbt.NbtCompressionType.NONE
            )
        )
    else:
        nbt_comp_type = rapidnbt.NbtCompressionType(compression % 3)
        payload = rapidnbt.nbtio.dumps(tag, rapidnbt.NbtFileFormat.BIG_ENDIAN, nbt_comp_type)

    if external:
        _, region_x_str, region_z_str = path.stem.split(".")
        chunk_rel_z, chunk_rel_x = divmod(idx, 32)
        chunk_x = chunk_rel_x + int(region_x_str) * 32
        chunk_z = chunk_rel_z + int(region_z_str) * 32
        payload_size = 0
        compression_value = compression + 128
        out_path = path.parent / f"c.{chunk_x}.{chunk_z}.mcc"
    else:
        payload_size = len(payload)
        compression_value = compression
        out_path = path
    sector_count = math.ceil((payload_size + 5) / SECTOR)
    with open(path, "r+b") as f:
        sector_offset = math.ceil(f.seek(0, 2) / SECTOR)
        f.seek(idx * 4)  # location table
        f.write(sector_offset.to_bytes(3))
        f.write(sector_count.to_bytes(1))
        f.seek(idx * 4 + SECTOR)  # timestamp table
        f.write(timestamp.to_bytes(4))
        f.seek(sector_offset * SECTOR)
        f.write((payload_size + 1).to_bytes(4))  # length includes compression type byte
        f.write(compression_value.to_bytes(1))
    with open(out_path, "ab") as f:
        f.write(payload)
    with open(path, "ab") as f:
        f.truncate((sector_offset + sector_count) * SECTOR)
