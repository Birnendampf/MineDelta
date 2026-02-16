import shutil
from pathlib import Path

import pytest
import rapidnbt as nbt

from minedelta import region
from tests import helpers


@pytest.fixture
def bare_region_file(tmp_path: Path) -> Path:
    mca_file = tmp_path / "r.0.0.mca"
    helpers.generate_bare_region_file(mca_file)
    return mca_file


@pytest.fixture
def dummy_region_file(bare_region_file: Path) -> Path:
    """generate a dummy region file with one chunk"""
    helpers.write_nbt_to_region_file(
        bare_region_file, 0, 1, nbt.CompoundTag({"LastUpdate": nbt.LongTag(1)})
    )
    return bare_region_file


# noinspection PyTypeChecker
class TestRegionFile:
    def test_open(self, tmp_path: Path) -> None:
        mca_file = tmp_path / "r.0.0.mca"
        mca_file.touch()
        with pytest.raises(region.EmptyRegionError), region.RegionFile.open(mca_file):
            pass

        with mca_file.open("wb") as f:
            f.truncate(4096)
        with pytest.raises(region.RegionLoadingError), region.RegionFile.open(mca_file):
            pass

    def test_headers_empty(self, bare_region_file: Path) -> None:
        with pytest.raises(RuntimeError), region.RegionFile.open(bare_region_file) as r, r:
            pass
        with region.RegionFile.open(bare_region_file) as r:
            assert len(r._headers) == 1024
            for i, header in enumerate(r._headers):
                assert header.not_created, f"header {i} should be not created"

    @pytest.mark.parametrize("compression", helpers.Compression)
    @pytest.mark.parametrize("external", [True, False])
    def test__get_chunk_data(
        self, compression: helpers.Compression, external: bool, bare_region_file: Path
    ) -> None:
        tag = nbt.CompoundTag({"LastUpdate": 1, "SomeArray": bytearray(range(255))})
        helpers.write_nbt_to_region_file(bare_region_file, 0, 1, tag, compression, external)
        with region.RegionFile.open(bare_region_file) as r:
            data = r._get_chunk_data(r._headers[0])
            assert nbt.nbtio.loads(data, nbt.NbtFileFormat.BIG_ENDIAN) == tag

    @pytest.mark.parametrize(
        ("timestamp", "last_update", "is_chunk"),
        [(1, 1, True), (2, 2, True), (2, 1, False), (2, 2, False)],
    )
    def test__check_unchanged(
        self,
        timestamp: int,
        last_update: int,
        is_chunk: bool,
        dummy_region_file: Path,
        tmp_path: Path,
    ) -> None:
        expected = timestamp == 1 or last_update == 1 or is_chunk

        other_mca = shutil.copyfile(dummy_region_file, tmp_path / "r.0.1.mca")
        helpers.write_nbt_to_region_file(
            other_mca, 0, timestamp, nbt.CompoundTag({"LastUpdate": nbt.LongTag(last_update)})
        )
        with (
            region.RegionFile.open(dummy_region_file) as this,
            region.RegionFile.open(other_mca) as other,
        ):
            assert expected == this._check_unchanged(
                this._headers[0], other, other._headers[0], is_chunk
            )
