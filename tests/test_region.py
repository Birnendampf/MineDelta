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


def check_chunk_at_idx_matches(r: region.RegionFile, idx: int, tag: nbt.CompoundTag) -> None:
    # noinspection PyProtectedMember
    data = r._get_chunk_data(r._headers[idx])
    assert nbt.nbtio.loads(data, nbt.NbtFileFormat.BIG_ENDIAN) == tag


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
            open(mca_file, "rb", 0) as file,
            pytest.raises(PermissionError),
            region.RegionFile(file.fileno()),
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
            check_chunk_at_idx_matches(r, 0, tag)

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

    def test__check_unchanged_different_length(
        self, dummy_region_file: Path, other_dummy: Path
    ) -> None:
        helpers.write_nbt_to_region_file(
            other_dummy, 0, 0, nbt.CompoundTag({"asd": nbt.LongTag(1)})
        )
        with (
            region.RegionFile.open(dummy_region_file) as this,
            region.RegionFile.open(other_dummy) as other,
        ):
            assert not this._check_unchanged(this._headers[0], other, other._headers[0], False)

    def test_density_defragment(self, dummy_region_file: Path) -> None:
        with region.RegionFile.open(dummy_region_file) as r:
            assert r.density() == 1
        tag = nbt.CompoundTag({"LastUpdate": nbt.LongTag(1), "hello": "world"})
        helpers.write_nbt_to_region_file(dummy_region_file, 0, 1, tag)
        with region.RegionFile.open(dummy_region_file) as r:
            assert r.density() == 0.75
            r.defragment()
            assert r.density() == 1
            check_chunk_at_idx_matches(r, 0, tag)

    def test_overlapping_chunks(self, dummy_region_file: Path) -> None:
        helpers.write_nbt_to_region_file(dummy_region_file, 1, 1)
        with region.RegionFile.open(dummy_region_file) as r:
            r._headers[1].offset = 2
            with pytest.raises(region.CorruptedRegionError):
                r.defragment()


# noinspection PyTypeChecker
class TestDiffOperations:
    def test_identical(self, dummy_region_file: Path, other_dummy: Path) -> None:
        with (
            region.RegionFile.open(dummy_region_file) as this,
            region.RegionFile.open(other_dummy) as other,
        ):
            assert this.filter_diff_defragment(other)
            assert this._headers[0].unmodified
            with pytest.raises(region.ChunkLoadingError):
                this._get_chunk_data(this._headers[0])
            assert this.density() == 1

    @pytest.mark.parametrize("swap", [True, False])
    def test_not_identical(self, dummy_region_file: Path, other_dummy: Path, swap: bool) -> None:
        tag = nbt.CompoundTag({"LastUpdate": nbt.LongTag(1), "hello": "world"})
        helpers.write_nbt_to_region_file(dummy_region_file, 1, 1, tag)
        with (
            region.RegionFile.open(dummy_region_file) as this,
            region.RegionFile.open(other_dummy) as other,
        ):
            # yes its ugly but leads to nicer test failures
            if swap:
                assert not other.filter_diff_defragment(this)
            else:
                assert not this.filter_diff_defragment(other)
            assert this._headers[0].unmodified
            check_chunk_at_idx_matches(this, 1, tag)
            assert this.density() == 1

    @pytest.mark.parametrize("defragment", [True, False])
    @pytest.mark.parametrize("added_size", [True, False])
    def test_apply_diff(
        self, dummy_region_file: Path, other_dummy: Path, defragment: bool, added_size: bool
    ) -> None:
        kept_tag = nbt.CompoundTag({"kept": True})
        helpers.write_nbt_to_region_file(dummy_region_file, 0, 2, kept_tag)

        overwrite_tag = nbt.CompoundTag({"kept": False, "fits": True})
        helpers.write_nbt_to_region_file(other_dummy, 1, 10, overwrite_tag)  # overwrite + fits
        helpers.write_nbt_to_region_file(dummy_region_file, 1, 2)

        appended_tag = nbt.CompoundTag({"kept": False, "fits": False})
        if added_size:  # overwrite + doesn't fit
            helpers.write_nbt_to_region_file(other_dummy, 2, 10, appended_tag)
        helpers.write_nbt_to_region_file(dummy_region_file, 3, 2)  # overwrite with not created
        with region.RegionFile.open(other_dummy) as other:
            other._headers[0].unmodified = True  # keep first
            other.defragment()
            with region.RegionFile.open(dummy_region_file) as this:
                this.apply_diff(other, defragment)
                assert (this.density() == 1) == defragment
                for header in this._headers[:4]:
                    assert not header.unmodified
                check_chunk_at_idx_matches(this, 0, kept_tag)
                assert this._headers[0].mtime == 1
                check_chunk_at_idx_matches(this, 1, overwrite_tag)
                assert this._headers[1].mtime == 10
                if added_size:
                    check_chunk_at_idx_matches(this, 2, appended_tag)
                    assert this._headers[2].mtime == 10
                else:
                    assert this._headers[2].mtime == 0
                    assert this._headers[2].not_created
                assert this._headers[3].mtime == 0
                assert this._headers[3].not_created
