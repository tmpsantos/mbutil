import sqlite3, uuid, sys, logging, time, os, json, zlib, hashlib, tempfile

from util import mbtiles_connect, mbtiles_setup, optimize_connection, optimize_database, execute_commands_on_tile

logger = logging.getLogger(__name__)


def disk_to_mbtiles(directory_path, mbtiles_file, **kwargs):
    logger.debug("Importing from disk to MBTiles: %s --> %s" % (directory_path, mbtiles_file))

    import_into_existing_mbtiles = os.path.isfile(mbtiles_file)
    existing_mbtiles_is_compacted = True

    auto_commit = kwargs.get('auto_commit')

    con = mbtiles_connect(mbtiles_file, auto_commit)
    cur = con.cursor()
    optimize_connection(cur, False)

    if import_into_existing_mbtiles:
        cur.execute("select count(name) from sqlite_master where type='table' AND name='images';")
        res = cur.fetchone()
        existing_mbtiles_is_compacted = (res[0] > 0)
    else:
        mbtiles_setup(cur)

    image_format = 'png'
    try:
        metadata = json.load(open(os.path.join(directory_path, 'metadata.json'), 'r'))
        image_format = metadata.get('format', 'png')

        # Check that the old and new image formats are the same
        if import_into_existing_mbtiles:
            original_format = None
            try:
                original_format = cur.execute("select value from metadata where name='format';").fetchone()[0]
            except:
                pass

            if original_format != None and image_format != original_format:
                sys.stderr.write('The files to merge must use the same image format (png or jpg)\n')
                sys.exit(1)

        if not import_into_existing_mbtiles:
            for name, value in metadata.items():
                cur.execute('insert or ignore into metadata (name, value) values (?, ?)',
                        (name, value))
            con.commit()
            logger.info('metadata from metadata.json restored')

    except IOError:
        logger.warning('metadata.json not found')

    zoom     = kwargs.get('zoom')
    min_zoom = kwargs.get('min_zoom')
    max_zoom = kwargs.get('max_zoom')
    no_overwrite = kwargs.get('no_overwrite')

    if zoom >= 0:
        min_zoom = max_zoom = zoom

    existing_tiles = {}
    if no_overwrite:
        tiles = cur.execute("""select zoom_level, tile_column, tile_row from tiles where zoom_level>=? and zoom_level<=?;""", (min_zoom, max_zoom))
        t = tiles.fetchone()
        while t:
            z = str(t[0])
            x = str(t[1])
            y = str(t[2])

            zoom = existing_tiles.get(z, None)
            if not zoom:
                zoom = {}
                existing_tiles[z] = zoom

            row = zoom.get(y, None)
            if not row:
                row = set()
                zoom[y] = row

            row.add(x)
            t = tiles.fetchone()

    count = 0
    start_time = time.time()
    msg = ""
    for r1, zs, ignore in os.walk(os.path.join(directory_path, "tiles")):
        for z in zs:
            if int(z) < min_zoom or int(z) > max_zoom:
                continue

            for r2, xs, ignore in os.walk(os.path.join(r1, z)):
                for x in xs:
                    for r2, ignore, ys in os.walk(os.path.join(r1, z, x)):
                        for y in ys:
                            y, extension = y.split('.')

                            if no_overwrite:
                                if x in existing_tiles.get(z, {}).get(y, set()):
                                    logging.debug("Ignoring tile (%s, %s, %s)" % (z, x, y))
                                    continue

                            if kwargs.get('flip_y') == True:
                                y = flip_y(z, y)

                            f = open(os.path.join(r1, z, x, y) + '.' + extension, 'rb')
                            tile_data = f.read()
                            f.close()

                            # Execute commands
                            if kwargs['command_list']:
                                tile_data = execute_commands_on_tile(kwargs['command_list'], image_format, tile_data)

                            if existing_mbtiles_is_compacted:
                                m = hashlib.md5()
                                m.update(tile_data)
                                tile_id = m.hexdigest()

                                cur.execute("""insert or ignore into images (tile_id, tile_data) values (?, ?);""",
                                    (tile_id, sqlite3.Binary(tile_data)))

                                cur.execute("""replace into map (zoom_level, tile_column, tile_row, tile_id)
                                    values (?, ?, ?, ?);""",
                                    (z, x, y, tile_id))
                            else:
                                cur.execute("""replace into tiles (zoom_level, tile_column, tile_row, tile_data) values (?, ?, ?, ?);""",
                                    (z, x, y.split('.')[0], sqlite3.Binary(tile_data)))

                            count = count + 1
                            if (count % 100) == 0:
                                for c in msg: sys.stdout.write(chr(8))
                                logger.debug("%s tiles imported (%d tiles/sec)" % (count, count / (time.time() - start_time)))

    logger.debug("%d tiles imported." % (count))
    con.commit()
    con.close()
