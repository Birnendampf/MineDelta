import shutil
from collections.abc import Callable
from pathlib import Path
from typing import TypeAlias

import pytest
import rapidnbt as nbt

from minedelta import region
from tests import helpers

RegionFactory: TypeAlias = Callable[[Path], Path]


@pytest.fixture
def bare_region_file(tmp_path: Path) -> Path:
    mca_file = tmp_path / "r.0.0.mca"
    helpers.generate_bare_region_file(mca_file)
    return mca_file


@pytest.fixture(scope="session")
def dummy_region_file_factory(tmp_path_factory: pytest.TempPathFactory) -> RegionFactory:
    """generate a dummy region file with one chunk"""
    mca_file = tmp_path_factory.getbasetemp() / "r.0.0.mca"
    helpers.generate_bare_region_file(mca_file)
    helpers.write_nbt_to_region_file(mca_file, 0, 1)

    def dummy_region_file(path: Path) -> Path:
        return shutil.copyfile(mca_file, path)

    return dummy_region_file


@pytest.fixture
def dummy_region_file(dummy_region_file_factory: RegionFactory, tmp_path: Path) -> Path:
    return dummy_region_file_factory(tmp_path / "r.0.0.mca")


@pytest.fixture
def other_dummy(tmp_path: Path, dummy_region_file_factory: RegionFactory) -> Path:
    return dummy_region_file_factory(tmp_path / "r.0.1.mca")


# noinspection PyTypeChecker
class TestRegionFile:
    def test_open(self, tmp_path: Path) -> None:
        mca_file = tmp_path / "r.0.0.mca"
        mca_file.touch()
        with pytest.raises(region.EmptyRegionError), region.RegionFile.open(mca_file):
            pass

        with mca_file.open("wb") as f:
            f.truncate(4096)
        with (
            pytest.raises(region.RegionLoadingError, match="Chunk headers appear truncated"),
            region.RegionFile.open(mca_file),
        ):
            pass
        with (
            open(mca_file, "rb", 0) as f,
            pytest.raises(PermissionError),
            region.RegionFile(f.fileno()),
        ):
            pass

    def test_headers_empty(self, bare_region_file: Path) -> None:
        with open(bare_region_file, "r+b", 0) as f:
            with region.RegionFile(f.fileno()) as r, pytest.raises(RuntimeError), r:
                pass
            with r:
                assert len(r._headers) == 1024
                for i, header in enumerate(r._headers):
                    assert header.not_created, f"header {i} should be not created"

    @pytest.mark.parametrize("compression", helpers.Compression)
    @pytest.mark.parametrize(
        "external",
        [
            pytest.param(
                True,
                marks=pytest.mark.xfail(
                    reason=".mcc files not yet supported", raises=region.ChunkLoadingError
                ),
            ),
            False,
        ],
    )
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
        other_dummy: Path,
    ) -> None:
        expected = timestamp == 1 or last_update == 1 or is_chunk
        helpers.write_nbt_to_region_file(
            other_dummy, 0, timestamp, nbt.CompoundTag({"LastUpdate": nbt.LongTag(last_update)})
        )
        with (
            region.RegionFile.open(dummy_region_file) as this,
            region.RegionFile.open(other_dummy) as other,
        ):
            assert expected == this._check_unchanged(
                this._headers[0], other, other._headers[0], is_chunk
            )

    def test_density_defragment(self, dummy_region_file: Path) -> None:
        with region.RegionFile.open(dummy_region_file) as r:
            assert r.density() == 1
        tag = nbt.CompoundTag({"LastUpdate": nbt.LongTag(1), "hello": "world"})
        helpers.write_nbt_to_region_file(dummy_region_file, 0, 1, tag)
        with region.RegionFile.open(dummy_region_file) as r:
            assert r.density() == 0.75
            r.defragment()
            assert r.density() == 1
            data = r._get_chunk_data(r._headers[0])
            assert nbt.nbtio.loads(data, nbt.NbtFileFormat.BIG_ENDIAN) == tag

    def test_overlapping_chunks(self, dummy_region_file: Path) -> None:
        helpers.write_nbt_to_region_file(dummy_region_file, 1, 1)
        with region.RegionFile.open(dummy_region_file) as r:
            r._headers[1].offset = 2
            with pytest.raises(region.CorruptedRegionError):
                r.defragment()
