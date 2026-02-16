> [!WARNING]
> This project is still in early stages of development and should not be used just yet

# MineDelta Backup Utility

> Why store everything when you can just store only the chunks that changed?

A differential backup utility for Minecraft worlds.

This tool offers 3 ways to create backups:

- diff (storing only changed chunks),
- git (using dulwich),
- hardlink (deduplicating identical files)

each with their own advantages and drawbacks ([see below](#comparison-between-backup-methods))

## Installation

```shell
pipx install minedelta
```

This will install only the package and its required dependencies.

To be able to use the `diff` backup method on a server that has `region-file-compression = lz4` set
in server.properties, install `minedelta[lz4]`.

To use the `git` backup method, install `minedelta[git]`.
You can, of course, also use both with `minedelta[git,lz4]`.

To use MineDelta as a library, add it to your dependencies.

## Usage

### As a library

All backup methods are implemented as manager classes inheriting from the
`minedelta.backup.BaseBackupManager` abstract class.

#### Example using DiffBackupManager

````python
from pathlib import Path

import minedelta.backup


def main():
    # create a new manager
    manager = minedelta.backup.DiffBackupManager("/path/to/world", Path("/path/to/backup_dir"))

    # create, restore, delete, allow passing in a progress function which will be called with a
    # str repeatedly
    manager.create_backup("description", progress=print)

    infos = manager.list_backups()  # a list of minedelta.backup.BackupInfo
    print(infos)
    oldest_idx = len(infos) - 1

    manager.restore_backup(oldest_idx, print)

    manager.restore_backup(0, print)

    manager.delete_backup(0, print)


if __name__ == "__main__":
    main()
````

#### Changing the number of threads used by `DiffBackupManager`

````python
from minedelta.backup import diff

print(f"{diff.MAX_WORKERS=}")  # set to the number of available cores by default
# run in single threaded mode
diff.MAX_WORKERS = 1
````

#### A note on methods with an `id_` parameter

some methods in (`.restore_backup()` and `.delete_backup()`) take an `id_` parameter to specify
which backup to restore / delete.  
This can either be an index into the list returned by `.list_backups()` or the `BackupInfo.id` of
the chosen backup.  
Which backup manager requires what can be seen via the `Manager.index_by` class attribute, which
is either `"idx"` or `"id"`

**Example: translating between index and id:**

```python
from minedelta.backup import BaseBackupManager, BackupInfo


def restore_by_index(manager: BaseBackupManager, idx: int) -> None:
    if manager.index_by == "idx":
        manager.restore_backup(idx)
    else:
        infos = manager.list_backups()
        manager.restore_backup(infos[idx])


def restore_by_info(manager: BaseBackupManager, info: BackupInfo) -> None:
    if manager.index_by == "id":
        manager.restore_backup(info.id)
    else:
        infos = manager.list_backups()
        manager.restore_backup(infos.index(info))

```

## Comparison between backup methods

| type     | details                                                              | pros                                                                                                     | cons                                                                                                                                                                                                                                              | backup size |
|----------|----------------------------------------------------------------------|----------------------------------------------------------------------------------------------------------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|-------------|
| diff     | newest is full backup, older store only changed chunks               | <ul><li>deleting oldest and restoring newest is fast and O(1)</li><li>best storage efficiency</li></ul>  | <ul><li>creating backups can be very slow</li><li>restoring oldest backup is O(n)</li></ul>                                                                                                                                                       | best        |
| git      | uses git for backups                                                 | <ul><li>faster than diff in most scenarios</li><li>easy off-site backups with remotes</li></ul>          | <ul><li>deleting backups requires rewriting history</li><li>deleting oldest is O(n)</li><li>actually reclaiming disk space from deleted backups requires expensive pruning</li><li>branches and merge commits **will** break the system</li></ul> | decent      |
| hardlink | creates hardlinks between unchanged files, effectively deduplicating | <ul><li>every backup is a valid world, can be manually restored</li><li>fastest</li><li>simple</li></ul> | only slightly more storage efficient than creating raw copies of the world                                                                                                                                                                        | worst       |

---
NOT AN OFFICIAL MINECRAFT PRODUCT. NOT APPROVED BY OR ASSOCIATED WITH MOJANG OR MICROSOFT