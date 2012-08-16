# MBUtil

MBUtil is a utility for importing and exporting the [MBTiles](http://mbtiles.org/) format,
typically created with [MapBox](http://mapbox.com/) [TileMill](http://mapbox.com/tilemill/).

Before exporting tiles to disk, see if there's a [MapBox Hosting plan](http://mapbox.com/plans/)
or an open source [MBTiles server implementation](https://github.com/mapbox/mbtiles-spec/wiki/Implementations)
that works for you - tiles on disk are notoriously difficult to manage.

## Installation

Git checkout (requires git)

    git clone git://github.com/Alpstein/mbutil.git
    cd mbutil
    ./mb-util -h

    # then to install the mb-util command globally:
    sudo python setup.py install
    # then you can run:
    mb-util

Python installation (requires easy_install)

    easy_install mbutil
    mb-util -h

## Usage

    mb-util [command] [options] file|directory [file|directory ...]

    Examples:

    Export an mbtiles database to a directory of files:
    $ mb-util --export world.mbtiles tiles

    Import a directory of tiles into an mbtiles database:
    $ mb-util --import tiles world.mbtiles

    Create an empty mbtiles file:
    $ mb-util --create empty.mbtiles

    Execute commands on all tiles in the mbtiles file:
    $ mb-util --process --execute "COMMAND ARGUMENTS" [--execute "SECOND COMMAND"] world.mbtiles

    Merge two or more mbtiles files (receiver will be the first file):
    $ mb-util --merge receiver.mbtiles file1.mbtiles [file2.mbtiles ...]

    Check if a mbtiles file contains all tiles at a specific zoom level:
    $ mb-util --check --zoom=7 world.mbtiles

    Compact a mbtiles file by eliminating duplicate images:
    $ mb-util --compact world.mbtiles


    Options:
        -h, --help            show this help message and exit

    Commands:
        These are the commands to use on mbtiles databases

        -e, --export        Export an mbtiles database to a directory of files. If
                            the directory exists, any already existing tiles will
                            be overwritten.
        -i, --import        Import a directory of tiles into an mbtiles database.
                            If the mbtiles database already exists, existing tiles
                            will be overwritten with the imported tiles.
        -m, --merge         Merge two or more databases. The receiver will be
                            created if it doesn't yet exist.
        -p, --process       Processes a mbtiles databases. Only usefull together
                            with one or more --execute.
        --check             Check the database for missing tiles.
        --compact           Eliminate duplicate images to reduce mbtiles filesize.
        --create            Create an empty mbtiles database.
        --convert=CONVERT   Convert tile coordinates 'y/x/z' to bounding box
                            'left,bottom,right,top' or vice versa.

    Options:
        --execute=COMMAND   Commands to execute for each tile image. %s will be
                            replaced with the file name. This argument may be
                            repeated several times and can be used together with
                            --import/--export/--merge/--compact/--process.
        --flip-y            Flip the y tile coordinate during
                            --export/--import/--merge/--convert.
        --min-zoom=MIN_ZOOM
                            Minimum zoom level for
                            --export/--import/--merge/--process/--check/--convert.
        --max-zoom=MAX_ZOOM
                            Maximum zoom level for
                            --export/--import/--merge/--process/--check/--convert.
        --zoom=ZOOM         Zoom level for --export/--import/--process/--check/--convert.
                            (Overrides --min-zoom and --max-zoom)
        --no-overwrite      don't overwrite existing tiles during
                            --merge/--import/--export.
        --auto-commit       Enable auto commit for --merge/--import/--process.
        --check-before-merge
                            Runs some basic checks (like --check) on mbtiles
                            before merging them.
        --delete-after-export
                            DANGEROUS!!! After a --merge or --export, this option
                            will delete all the merged/exported tiles from the
                            (sending) database. Only really usefull with --min-
                            zoom/--max-zoom or --zoom since it would remove all
                            tiles from the database otherwise.
        --poolsize=POOLSIZE
                            Pool size for processing tiles with --process/--merge.
                            Default is to use a pool size equal to the number of
                            cpus/cores.
        --vacuum            VACUUM the mbtiles database after
                            --import/--merge/--process/--compact.
        --analyze           ANALYZE the mbtiles database after
                            --import/--merge/--process/--compact.
        -q, --quiet         don't print any status messages to stdout except
                            errors.
        -d, --debug         print debug messages to stdout (exclusive to --quiet).


## Special considerations

* All mbtiles databases must be on the same host as mb-util, due to the WAL locking mode used for SQLite.

## Requirements

* Python `>= 2.6`
* SQLite `>= 3.7.0`

## Metadata

mb-util imports and exports metadata as JSON, in the root of the tile directory, as a file named `metadata.json`.

```javascript
{
    "name": "World Light",
    "description": "A Test Metadata",
    "version": "3"
}
```

## Testing

This project uses [nosetests](http://readthedocs.org/docs/nose/en/latest/) for testing. Install nosetests
and run

    nosetests

## License

BSD - see LICENSE.md

## Authors

- Tom MacWright (tmcw)
- Dane Springmeyer (springmeyer)
- Mathieu Leplatre (leplatrem)
- Thomas Rasch (trasch)
