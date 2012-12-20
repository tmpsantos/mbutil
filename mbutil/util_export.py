import sqlite3, uuid, sys, logging, time, os, json, zlib, hashlib, tempfile

from util import mbtiles_connect, optimize_connection, optimize_database, compaction_update, execute_commands_on_tile, flip_y

logger = logging.getLogger(__name__)


def mbtiles_to_disk(mbtiles_file, directory_path, **kwargs):
    logger.info("Exporting database to disk: %s --> %s" % (mbtiles_file, directory_path))


    delete_after_export = kwargs.get('delete_after_export', False)
    no_overwrite        = kwargs.get('no_overwrite', False)
    wal_journal         = kwargs.get('wal_journal', False)

    zoom     = kwargs.get('zoom', -1)
    min_zoom = kwargs.get('min_zoom', 0)
    max_zoom = kwargs.get('max_zoom', 18)
    tmp_dir  = kwargs.get('tmp_dir', None)
    print_progress = kwargs.get('progress', False)
    min_timestamp  = kwargs.get('min_timestamp', 0)
    max_timestamp  = kwargs.get('max_timestamp', 0)

    if tmp_dir and not os.path.isdir(tmp_dir):
        os.mkdir(tmp_dir)

    if zoom >= 0:
        min_zoom = max_zoom = zoom


    con = mbtiles_connect(mbtiles_file)
    cur = con.cursor()
    optimize_connection(cur, wal_journal)


    if not os.path.isdir(directory_path):
        os.mkdir(directory_path)
    base_path = os.path.join(directory_path, "tiles")
    if not os.path.isdir(base_path):
        os.makedirs(base_path)


    metadata = dict(con.execute('SELECT name, value FROM metadata').fetchall())
    json.dump(metadata, open(os.path.join(directory_path, 'metadata.json'), 'w'), indent=4)

    count = 0
    start_time = time.time()
    image_format = metadata.get('format', 'png')
    sending_mbtiles_is_compacted = (con.execute("SELECT count(name) FROM sqlite_master WHERE type='table' AND name='images'").fetchone()[0] > 0)

    total_tiles = 0
    if min_timestamp > 0 and max_timestamp > 0:
        total_tiles = con.execute("""SELECT count(zoom_level) FROM map WHERE zoom_level>=? AND zoom_level<=? AND updated_at>? AND updated_at<?""",
            (min_zoom, max_zoom, min_timestamp, max_timestamp)).fetchone()[0]
    elif min_timestamp > 0:
        total_tiles = con.execute("""SELECT count(zoom_level) FROM map WHERE zoom_level>=? AND zoom_level<=? AND updated_at>?""",
            (min_zoom, max_zoom, min_timestamp)).fetchone()[0]
    elif max_timestamp > 0:
        total_tiles = con.execute("""SELECT count(zoom_level) FROM map WHERE zoom_level>=? AND zoom_level<=? AND updated_at<?""",
            (min_zoom, max_zoom, max_timestamp)).fetchone()[0]
    else:
        total_tiles = con.execute("""SELECT count(zoom_level) FROM map WHERE zoom_level>=? AND zoom_level<=?""",
            (min_zoom, max_zoom)).fetchone()[0]


    if not sending_mbtiles_is_compacted and (min_timestamp != 0 or max_timestamp != 0):
        con.close()
        sys.stderr.write('min-timestamp/max-timestamp can only be used with compacted databases.\n')
        sys.exit(1)

    if sending_mbtiles_is_compacted:
        compaction_update(cur)


    logger.debug("%d tiles to export" % (total_tiles))
    if print_progress:
        sys.stdout.write("%d tiles to export\n" % (total_tiles))
        sys.stdout.write("%d / %d tiles exported (0%% @ 0 tiles/sec)" % (count, total_tiles))
        sys.stdout.flush()

    tiles = None
    if min_timestamp > 0 and max_timestamp > 0:
        tiles = cur.execute("""SELECT zoom_level, tile_column, tile_row, tile_data FROM tiles WHERE zoom_level>=? AND zoom_level<=? AND updated_at>? AND updated_at<?""",
            (min_zoom, max_zoom, min_timestamp, max_timestamp))
    elif min_timestamp > 0:
        tiles = cur.execute("""SELECT zoom_level, tile_column, tile_row, tile_data FROM tiles WHERE zoom_level>=? AND zoom_level<=? AND updated_at>?""",
            (min_zoom, max_zoom, min_timestamp))
    elif max_timestamp > 0:
        tiles = cur.execute("""SELECT zoom_level, tile_column, tile_row, tile_data FROM tiles WHERE zoom_level>=? AND zoom_level<=? AND updated_at<?""",
            (min_zoom, max_zoom, max_timestamp))
    else:
        tiles = cur.execute("""SELECT zoom_level, tile_column, tile_row, tile_data FROM tiles WHERE zoom_level>=? AND zoom_level<=?""",
            (min_zoom, max_zoom))

    t = tiles.fetchone()
    while t:
        z = t[0]
        x = t[1]
        y = t[2]
        tile_data = t[3]

        # Execute commands
        if kwargs.get('command_list'):
            tile_data = execute_commands_on_tile(kwargs['command_list'], image_format, tile_data, tmp_dir)

        if kwargs.get('flip_y', False) == True:
          y = flip_y(z, y)

        tile_dir = os.path.join(base_path, str(z), str(x))
        if not os.path.isdir(tile_dir):
            os.makedirs(tile_dir)

        tile_file = os.path.join(tile_dir, '%s.%s' % (y, metadata.get('format', 'png')))

        if no_overwrite == False or not os.path.isfile(tile_file):
            f = open(tile_file, 'wb')
            f.write(tile_data)
            f.close()


        count = count + 1
        if (count % 100) == 0:
            logger.debug("%d / %d tiles exported (%.1f%% @ %.1f tiles/sec)" %
                (count, total_tiles, (float(count) / float(total_tiles)) * 100.0, count / (time.time() - start_time)))
            if print_progress:
                sys.stdout.write("\r%d / %d tiles exported (%.1f%% @ %.1f tiles/sec)" %
                    (count, total_tiles, (float(count) / float(total_tiles)) * 100.0, count / (time.time() - start_time)))
                sys.stdout.flush()

        t = tiles.fetchone()


    if print_progress:
        sys.stdout.write('\n')

    logger.info("%d / %d tiles exported (100.0%% @ %.1f tiles/sec)" % (count, total_tiles, count / (time.time() - start_time)))
    if print_progress:
        sys.stdout.write("%d / %d tiles exported (100.0%% @ %.1f tiles/sec)\n" % (count, total_tiles, count / (time.time() - start_time)))
        sys.stdout.flush()


    if delete_after_export:
        logger.debug("WARNING: Removing exported tiles from %s" % (mbtiles_file))

        if sending_mbtiles_is_compacted:
            if min_timestamp > 0 and max_timestamp > 0:
                cur.execute("""DELETE FROM images WHERE tile_id IN (SELECT tile_id FROM map WHERE zoom_level>=? AND zoom_level<=? AND updated_at>? AND updated_at<?)""",
                    (min_zoom, max_zoom, min_timestamp, max_timestamp))
                cur.execute("""DELETE FROM map WHERE zoom_level>=? AND zoom_level<=? AND updated_at>? AND updated_at<?""", (min_zoom, max_zoom, min_timestamp, max_timestamp))
            elif min_timestamp > 0:
                cur.execute("""DELETE FROM images WHERE tile_id IN (SELECT tile_id FROM map WHERE zoom_level>=? AND zoom_level<=? AND updated_at>?)""",
                    (min_zoom, max_zoom, min_timestamp))
                cur.execute("""DELETE FROM map WHERE zoom_level>=? AND zoom_level<=? AND updated_at>?""", (min_zoom, max_zoom, min_timestamp))
            elif max_timestamp > 0:
                cur.execute("""DELETE FROM images WHERE tile_id IN (SELECT tile_id FROM map WHERE zoom_level>=? AND zoom_level<=? AND updated_at<?)""",
                    (min_zoom, max_zoom, max_timestamp))
                cur.execute("""DELETE FROM map WHERE zoom_level>=? AND zoom_level<=? AND updated_at<?""", (min_zoom, max_zoom, max_timestamp))
            else:
                cur.execute("""DELETE FROM images WHERE tile_id IN (SELECT tile_id FROM map WHERE zoom_level>=? AND zoom_level<=?)""",
                    (min_zoom, max_zoom))
                cur.execute("""DELETE FROM map WHERE zoom_level>=? AND zoom_level<=?""", (min_zoom, max_zoom))
        else:
            cur.execute("""DELETE FROM tiles WHERE zoom_level>=? AND zoom_level<=?""", (min_zoom, max_zoom))

        optimize_database(cur, kwargs.get('skip_analyze', False), kwargs.get('skip_vacuum', False))
        con.commit()


    con.close()

