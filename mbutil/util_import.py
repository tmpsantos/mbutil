import sqlite3, uuid, sys, logging, time, os, json, zlib, hashlib, tempfile

from util import mbtiles_connect, mbtiles_setup, optimize_connection, optimize_database, execute_commands_on_tile, flip_y, compaction_update

logger = logging.getLogger(__name__)


def disk_to_mbtiles(directory_path, mbtiles_file, **kwargs):
    logger.info("Importing from disk to database: %s --> %s" % (directory_path, mbtiles_file))


    import_into_existing_mbtiles = os.path.isfile(mbtiles_file)
    existing_mbtiles_is_compacted = True

    no_overwrite = kwargs.get('no_overwrite', False)
    auto_commit  = kwargs.get('auto_commit', False)
    wal_journal  = kwargs.get('wal_journal', False)
    synchronous_off = kwargs.get('synchronous_off', False)
    zoom     = kwargs.get('zoom', -1)
    min_zoom = kwargs.get('min_zoom', 0)
    max_zoom = kwargs.get('max_zoom', 18)
    tmp_dir  = kwargs.get('tmp_dir', None)

    if tmp_dir and not os.path.isdir(tmp_dir):
        os.mkdir(tmp_dir)

    if zoom >= 0:
        min_zoom = max_zoom = zoom


    con = mbtiles_connect(mbtiles_file, auto_commit)
    cur = con.cursor()
    optimize_connection(cur, wal_journal, synchronous_off, False)


    if import_into_existing_mbtiles:
        existing_mbtiles_is_compacted = (con.execute("SELECT count(name) FROM sqlite_master WHERE type='table' AND name='images'").fetchone()[0] > 0)
        if existing_mbtiles_is_compacted:
            compaction_update(cur)
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
                original_format = cur.execute("SELECT value FROM metadata WHERE name='format'").fetchone()[0]
            except:
                pass

            if original_format != None and image_format != original_format:
                sys.stderr.write('The files to merge must use the same image format (png or jpg)\n')
                sys.exit(1)

        if not import_into_existing_mbtiles:
            for name, value in metadata.items():
                cur.execute('INSERT OR IGNORE INTO metadata (name, value) VALUES (?, ?)',
                        (name, value))
            con.commit()
            logger.info('metadata from metadata.json restored')

    except IOError:
        logger.warning('metadata.json not found')


    existing_tiles = {}

    if no_overwrite:
        tiles = cur.execute("""SELECT zoom_level, tile_column, tile_row FROM tiles WHERE zoom_level>=? AND zoom_level<=?""",
            (min_zoom, max_zoom))

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

                            if kwargs.get('flip_y', False) == True:
                                y = str(flip_y(z, y))

                            f = open(os.path.join(r1, z, x, y) + '.' + extension, 'rb')
                            tile_data = f.read()
                            f.close()

                            # Execute commands
                            if kwargs.get('command_list'):
                                tile_data = execute_commands_on_tile(kwargs['command_list'], image_format, tile_data, tmp_dir)

                            if existing_mbtiles_is_compacted:
                                m = hashlib.md5()
                                m.update(tile_data)
                                tile_id = m.hexdigest()

                                cur.execute("""INSERT OR IGNORE INTO images (tile_id, tile_data) VALUES (?, ?)""",
                                    (tile_id, sqlite3.Binary(tile_data)))

                                cur.execute("""REPLACE INTO map (zoom_level, tile_column, tile_row, tile_id, updated_at) VALUES (?, ?, ?, ?, ?)""",
                                    (z, x, y, tile_id, int(time.time())))
                            else:
                                cur.execute("""REPLACE INTO tiles (zoom_level, tile_column, tile_row, tile_data) VALUES (?, ?, ?, ?)""",
                                    (z, x, y.split('.')[0], sqlite3.Binary(tile_data)))


                            count = count + 1
                            if (count % 100) == 0:
                                logger.debug("%s tiles imported (%d tiles/sec)" %
                                    (count, count / (time.time() - start_time)))


    logger.info("%d tiles imported." % (count))

    con.commit()
    con.close()
