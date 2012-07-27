# MBUtil

MBUtil is a utility for importing and exporting the [MBTiles](http://mbtiles.org/) format,
typically created with [MapBox](http://mapbox.com/) [TileMill](http://mapbox.com/tilemill/).

Before exporting tiles to disk, see if there's a [MapBox Hosting plan](http://mapbox.com/plans/)
or an open source [MBTiles server implementation](https://github.com/mapbox/mbtiles-spec/wiki/Implementations)
that works for you - tiles on disk are notoriously difficult to manage.

## Installation

Git checkout (requires git)

    git clone git://github.com/mapbox/mbutil.git
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

    Export an mbtiles database to a directory of files:
    $ mb-util --export world.mbtiles tiles

    Import a directory of tiles into an mbtiles database:
    $ mb-util --import tiles world.mbtiles

    Create an empty mbtiles file:
    $ mb-util --create empty.mbtiles

    Execute commands on all tiles in the mbtiles file (similar to mbpipe):
    $ mb-util --execute "COMMAND ARGUMENTS" [--execute "SECOND COMMAND"] world.mbtiles

    Merge two or more mbtiles files (receiver will be the first file in the argument list):
    $ mb-util --merge receiver.mbtiles file1.mbtiles [file2.mbtiles ...]

    Check if a mbtiles file contains all tiles at a specific zoom level and remove unused tiles:
    $ mb-util --check world.mbtiles

    Compact a mbtiles file by eliminating duplicate images:
    $ mb-util --compact world.mbtiles

    See mb-util -h for more options.

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

## See Also

* [node-mbtiles provides mbpipe](https://github.com/mapbox/node-mbtiles/wiki/Post-processing-MBTiles-with-MBPipe), a useful utility.

## License

BSD - see LICENSE.md

## Authors

- Tom MacWright (tmcw)
- Dane Springmeyer (springmeyer)
- Mathieu Leplatre (leplatrem)
