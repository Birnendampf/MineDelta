import itertools
import shutil
from pathlib import Path
from typing import TypeAlias

import pytest
import rapidnbt

from minedelta.backup import (
    BaseBackupManager,
    DiffBackupManager,
    GitBackupManager,
    HardlinkBackupManager,
)
from tests import helpers

WorldVariations: TypeAlias = tuple[Path, ...]


@pytest.fixture(scope="session")
def _world_0(tmp_path_factory: pytest.TempPathFactory) -> Path:
    return tmp_path_factory.mktemp("world_0")


@pytest.fixture(scope="session")
def _world_1(tmp_path_factory: pytest.TempPathFactory) -> Path:
    world_dir = tmp_path_factory.mktemp("world_1")
    # make non chunk mca file
    poi_region = world_dir / "poi" / "r.0.0.mca"
    poi_region.parent.mkdir()
    helpers.generate_bare_region_file(poi_region)
    helpers.write_nbt_to_region_file(poi_region, 0, 1, rapidnbt.CompoundTag({"poi": "region 0.0"}))
    # chunk mca file
    chunk_region_dir = world_dir / "region"
    chunk_region_dir.mkdir()
    chunk_region = chunk_region_dir / "r.0.0.mca"
    helpers.generate_bare_region_file(chunk_region)
    helpers.write_nbt_to_region_file(
        chunk_region, 0, 1, rapidnbt.CompoundTag({"chunk": "region 0.0", "LastUpdate": 1})
    )
    helpers.write_nbt_to_region_file(
        chunk_region, 1, 1, rapidnbt.CompoundTag({"chunk": "something", "LastUpdate": 1})
    )
    # fake mca file (minecraft does this a lot)
    (chunk_region_dir / "r.0.-1.mca").touch()
    # other file
    (world_dir / "level.dat").write_text("level")
    # ignored files
    datapacks_dir = world_dir / "datapacks"
    datapacks_dir.mkdir()
    (datapacks_dir / "hello").write_text("hello")
    (datapacks_dir / "world").write_text("world")
    (world_dir / "session.lock").touch()
    (world_dir / "icon.png").write_text("icon")
    deep_file = world_dir / "very" / "deeply" / "nested" / "dir" / "deep" / "within"
    deep_file.mkdir(parents=True)
    (deep_file / "hello.dat").write_text("hello")
    return world_dir


@pytest.fixture(scope="session")
def _world_2(tmp_path_factory: pytest.TempPathFactory, _world_1: Path) -> Path:
    world_dir = tmp_path_factory.mktemp("world_2")
    shutil.copytree(_world_1, world_dir, dirs_exist_ok=True)
    (world_dir / "icon.png").write_text("icon_2")
    chunk_region_dir = world_dir / "region"
    region_0_minus_1 = chunk_region_dir / "r.0.-1.mca"
    helpers.generate_bare_region_file(region_0_minus_1)
    helpers.write_nbt_to_region_file(region_0_minus_1, 0, 2)

    helpers.generate_bare_region_file(chunk_region_dir / "r.0.2.mca")
    helpers.write_nbt_to_region_file(chunk_region_dir / "r.0.2.mca", 0, 2)
    (chunk_region_dir / "DistantHorizons.sqlite").write_text("Not real")
    region_0_0 = chunk_region_dir / "r.0.0.mca"
    region_0_0.unlink()
    helpers.generate_bare_region_file(region_0_0)
    helpers.write_nbt_to_region_file(
        region_0_0, 1, 2, rapidnbt.CompoundTag({"chunk": "world_2", "LastUpdate": 2})
    )
    helpers.write_nbt_to_region_file(
        world_dir / "poi" / "r.0.0.mca",
        0,
        2,
        rapidnbt.CompoundTag({"poi": "region 0.0", "version": 2}),
    )
    return world_dir


@pytest.fixture(scope="session")
def _world_3(tmp_path_factory: pytest.TempPathFactory, _world_1: Path) -> Path:
    world_dir = tmp_path_factory.mktemp("world_3")
    shutil.copytree(_world_1, world_dir, dirs_exist_ok=True)
    (world_dir / "level.dat").write_text("level 3")
    chunk_region_dir = world_dir / "region"
    (chunk_region_dir / "r.0.-1.mca").unlink()
    helpers.write_nbt_to_region_file(world_dir / "poi" / "r.0.0.mca", 1, 3)
    deep_ignored = world_dir / "DIM-1" / "data" / "DistantHorizons.sqlite"
    deep_ignored.parent.mkdir(parents=True)
    deep_ignored.write_text("Not a real DB lol")
    helpers.write_nbt_to_region_file(chunk_region_dir / "r.0.0.mca", 2, 3)
    helpers.generate_bare_region_file(chunk_region_dir / "r.0.1.mca")
    return world_dir


@pytest.fixture(
    scope="session",
    params=(*itertools.permutations(range(4), 3), (1, 2, 1), (1, 0, 1), (3, 1, 1), (1, 1, 0)),
)
def world_variations(
    _world_0: Path, _world_1: Path, _world_2: Path, _world_3: Path, request: pytest.FixtureRequest
) -> WorldVariations:
    loc = locals()
    return tuple(loc[f"_world_{i}"] for i in request.param)


@pytest.fixture(scope="class", params=[GitBackupManager, DiffBackupManager, HardlinkBackupManager])
def manager(
    request: pytest.FixtureRequest, tmp_path_factory: pytest.TempPathFactory
) -> BaseBackupManager:
    tmp_path = tmp_path_factory.mktemp(request.param.__name__)
    world_dir = tmp_path / "world"
    backup_dir = tmp_path / "backup"
    world_dir.mkdir(parents=True)
    backup_dir.mkdir(parents=True)
    manager: BaseBackupManager = request.param(world_dir, backup_dir)
    manager.prepare()
    return manager


@pytest.fixture(scope="class")
def load_manager(
    world_variations: WorldVariations, manager: BaseBackupManager
) -> BaseBackupManager:
    # noinspection PyProtectedMember
    orig_world = manager._world
    if isinstance(manager, GitBackupManager):
        for path in world_variations:
            manager._world = path
            manager.prepare()
            manager.create_backup(path.name)
            (path / ".git").unlink()
    else:
        for path in world_variations:
            manager._world = path
            manager.create_backup(path.name)
    manager._world = orig_world
    return manager
