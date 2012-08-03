import sqlite3, uuid, sys, logging, time, os, json, zlib, hashlib, tempfile

from util import mbtiles_connect, mbtiles_setup, optimize_connection, optimize_database, execute_commands_on_tile, flip_y
from util_check import check_mbtiles

logger = logging.getLogger(__name__)


def merge_mbtiles(mbtiles_file1, mbtiles_file2, **kwargs):
    logger.info("Merging MBTiles databases: %s --> %s" % (mbtiles_file2, mbtiles_file1))

    check_before_merge = kwargs.get('check_before_merge')
    if check_before_merge and not check_mbtiles(mbtiles_file2, **kwargs):
        sys.stderr.write("The pre-merge check on %s failed\n" % (mbtiles_file2))
        sys.exit(1)

    auto_commit = kwargs.get('auto_commit')
    delete_after_export = kwargs.get('delete_after_export')

    con1 = mbtiles_connect(mbtiles_file1, auto_commit)
    cur1 = con1.cursor()
    optimize_connection(cur1, False)

    con2 = mbtiles_connect(mbtiles_file2)
    cur2 = con2.cursor()
    optimize_connection(cur2)

    receiving_mbtiles_is_compacted = (cur1.execute("select count(name) from sqlite_master where type='table' AND name='images';").fetchone()[0] > 0)
    sending_mbtiles_is_compacted = (cur2.execute("select count(name) from sqlite_master where type='table' AND name='images';").fetchone()[0] > 0)
    if not receiving_mbtiles_is_compacted:
        con1.close()
        con2.close()
        sys.stderr.write('To merge two MBTiles, the receiver must already be compacted\n')
        sys.exit(1)

    # Check that the old and new image formats are the same
    original_format = new_format = None
    try:
        original_format = cur1.execute("select value from metadata where name='format';").fetchone()[0]
        new_format = cur2.execute("select value from metadata where name='format';").fetchone()[0]
    except:
        pass

    if original_format != None and new_format != None and new_format != original_format:
        sys.stderr.write('The files to merge must use the same image format (png or jpg)\n')
        sys.exit(1)

    if original_format == None and new_format != None:
        cur1.execute("""insert or ignore into metadata (name, value) values ("format", ?)""", [new_format])
        con1.commit()

    zoom     = kwargs.get('zoom')
    min_zoom = kwargs.get('min_zoom')
    max_zoom = kwargs.get('max_zoom')
    no_overwrite = kwargs.get('no_overwrite')

    if zoom >= 0:
        min_zoom = max_zoom = zoom

    existing_tiles = {}
    if no_overwrite:
        tiles = cur1.execute("""select zoom_level, tile_column, tile_row from tiles where zoom_level>=? and zoom_level<=?;""", (min_zoom, max_zoom))
        t = tiles.fetchone()
        while t:
            z = t[0]
            x = t[1]
            y = t[2]

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

    total_tiles = (cur2.execute("""select count(*) from tiles where zoom_level>=? and zoom_level<=?""", (min_zoom, max_zoom)).fetchone()[0])

    if sending_mbtiles_is_compacted:
        known_tile_ids = {}

        # First: Merge images
        tiles = cur2.execute("""select map.zoom_level, map.tile_column, map.tile_row, images.tile_id, images.tile_data from images, map where map.zoom_level>=? and map.zoom_level<=? and images.tile_id=map.tile_id;""", (min_zoom, max_zoom))
        t = tiles.fetchone()
        while t:
            z = t[0]
            x = t[1]
            y = t[2]
            tile_id = t[3]
            tile_data = t[4]

            if no_overwrite:
                if x in existing_tiles.get(z, {}).get(y, set()):
                    logging.debug("Ignoring tile (%d, %d, %d)" % (z, x, y))
                    t = tiles.fetchone()
                    continue

            if kwargs.get('flip_y') == True:
                y = flip_y(z, y)

            new_tile_id = known_tile_ids.get(tile_id)
            if new_tile_id is None:
                # Execute commands
                if kwargs['command_list']:
                    tile_data = execute_commands_on_tile(kwargs['command_list'], new_format, tile_data)

                m = hashlib.md5()
                m.update(tile_data)
                new_tile_id = m.hexdigest()
                known_tile_ids[tile_id] = new_tile_id

                cur1.execute("""replace into images (tile_id, tile_data) values (?, ?);""",
                    (new_tile_id, sqlite3.Binary(tile_data)))

            cur1.execute("""replace into map (zoom_level, tile_column, tile_row, tile_id) values (?, ?, ?, ?);""",
                (z, x, y, new_tile_id))

            count = count + 1
            if (count % 100) == 0:
                logger.debug("%s tiles merged (%.1f%% %.1f tiles/sec)" % (count, (float(count) / float(total_tiles)) * 100.0, count / (time.time() - start_time)))

            t = tiles.fetchone()

    else:
        known_tile_ids = set()

        tiles = cur2.execute("""select zoom_level, tile_column, tile_row, tile_data from tiles where zoom_level>=? and zoom_level<=?;""", (min_zoom, max_zoom))
        t = tiles.fetchone()
        while t:
            z = t[0]
            x = t[1]
            y = t[2]
            tile_data = t[3]

            if no_overwrite:
                if x in existing_tiles.get(z, {}).get(y, set()):
                    logging.debug("Ignoring tile (%d, %d, %d)" % (z, x, y))
                    t = tiles.fetchone()
                    continue

            if kwargs.get('flip_y') == True:
                y = flip_y(z, y)

            # Execute commands
            if kwargs['command_list']:
                tile_data = execute_commands_on_tile(kwargs['command_list'], new_format, tile_data)

            m = hashlib.md5()
            m.update(tile_data)
            tile_id = m.hexdigest()

            if tile_id not in known_tile_ids:
                cur1.execute("""replace into images (tile_id, tile_data) values (?, ?);""",
                    (tile_id, sqlite3.Binary(tile_data)))

            cur1.execute("""replace into map (zoom_level, tile_column, tile_row, tile_id) values (?, ?, ?, ?);""",
                (z, x, y, tile_id))

            known_tile_ids.add(tile_id)
            count = count + 1
            if (count % 100) == 0:
                logger.debug("%s tiles merged (%.1f%%, %.1f tiles/sec)" % (count, (float(count) / float(total_tiles)) * 100.0, count / (time.time() - start_time)))

            t = tiles.fetchone()

    logger.info("%s tiles merged (100.0%%, %.1f tiles/sec)" % (count, count / (time.time() - start_time)))

    if delete_after_export:
        logger.debug("WARNING: Removing merged tiles from %s" % (mbtiles_file2))

        if sending_mbtiles_is_compacted:
            cur2.execute("""delete from images where tile_id in (select tile_id from map where zoom_level>=? and zoom_level<=?);""", (min_zoom, max_zoom))
            cur2.execute("""delete from map where zoom_level>=? and zoom_level<=?;""", (min_zoom, max_zoom))
        else:
            cur2.execute("""delete from tiles where zoom_level>=? and zoom_level<=?;""", (min_zoom, max_zoom))

        optimize_database(cur2, kwargs.get('skip_analyze', False), kwargs.get('skip_vacuum', False))
        con2.commit()

    con1.commit()
    con1.close()
    con2.close()
