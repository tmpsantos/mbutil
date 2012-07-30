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
    $ mb-util --execute "COMMAND ARGUMENTS" [--execute "SECOND COMMAND"] world.mbtiles

    Merge two or more mbtiles files (receiver will be the first file):
    $ mb-util --merge receiver.mbtiles file1.mbtiles [file2.mbtiles ...]

    Check if a mbtiles file contains all tiles at a specific zoom level and remove unused tiles:
    $ mb-util --check world.mbtiles

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
        --check             Check the database for missing tiles and remove
                            unnecessary tiles.
        --compact           Eliminate duplicate images to reduce mbtiles filesize.
        --create            Create an empty mbtiles database.
        --execute=COMMAND   Commands to execute for each tile image. %s will be
                            replaced with the file name. This argument may be
                            repeated several times and can also be used together with
                            --import/--export/--merge/--compact.

    Options:
        --flip-y            Flip the y tile coordinate during
                            --export/--import/--merge.
        --min-zoom=MIN_ZOOM
                            Minimum zoom level for
                            --export/--import/--merge/--execute/--check.
        --max-zoom=MAX_ZOOM
                            Maximum zoom level for
                            --export/--import/--merge/--execute/--check.
        --no-overwrite      don't overwrite existing tiles during --merge,
                            --import.
        --no-vacuum         don't VACUUM the mbtiles database after --import,
                            --merge, --execute, --compact.
        --no-analyze        don't ANALYZE the mbtiles database after --import,
                            --merge, --execute, --compact.
        -q, --quiet         don't print any status messages to stdout except
                            errors.
        -d, --debug         print debug messages to stdout (exclusive to --quiet).

## Requirements

* Python `>= 2.6`

## Metadata

MBUtil imports and exports metadata as JSON, in the root of the tile directory, as a file named `metadata.json`.

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
