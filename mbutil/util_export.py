import sqlite3, uuid, sys, logging, time, os, json, zlib, hashlib, tempfile

from util import mbtiles_connect, optimize_connection, optimize_database, execute_commands_on_tile

logger = logging.getLogger(__name__)


def mbtiles_to_disk(mbtiles_file, directory_path, **kwargs):
    logger.info("Exporting MBTiles to disk: %s --> %s" % (mbtiles_file, directory_path))

    delete_after_export = kwargs.get('delete_after_export')

    con = mbtiles_connect(mbtiles_file)
    cur = con.cursor()
    optimize_connection(cur)

    zoom     = kwargs.get('zoom')
    min_zoom = kwargs.get('min_zoom')
    max_zoom = kwargs.get('max_zoom')

    if zoom >= 0:
        min_zoom = max_zoom = zoom

    if not os.path.isdir(directory_path):
        os.mkdir(directory_path)
    metadata = dict(con.execute('select name, value from metadata;').fetchall())
    json.dump(metadata, open(os.path.join(directory_path, 'metadata.json'), 'w'), indent=4)

    image_format = metadata.get('format', 'png')

    total_tiles = con.execute("""select count(zoom_level) from tiles where zoom_level>=? and zoom_level<=?;""", (min_zoom, max_zoom)).fetchone()[0]
    count = 0
    start_time = time.time()

    base_path = os.path.join(directory_path, "tiles")
    if not os.path.isdir(base_path):
        os.makedirs(base_path)

    sending_mbtiles_is_compacted = (cur.execute("select count(name) from sqlite_master where type='table' AND name='images';").fetchone()[0] > 0)
    no_overwrite = kwargs.get('no_overwrite')

    tiles = cur.execute("""select zoom_level, tile_column, tile_row, tile_data from tiles where zoom_level>=? and zoom_level<=?;""", (min_zoom, max_zoom))
    t = tiles.fetchone()
    while t:
        z = t[0]
        x = t[1]
        y = t[2]
        tile_data = t[3]

        # Execute commands
        if kwargs['command_list']:
            tile_data = execute_commands_on_tile(kwargs['command_list'], image_format, tile_data)

        if kwargs.get('flip_y') == True:
          y = flip_y(z, y)

        tile_dir = os.path.join(base_path, str(z), str(x))
        if not os.path.isdir(tile_dir):
            os.makedirs(tile_dir)

        tile_file = os.path.join(tile_dir,'%s.%s' % (y, metadata.get('format', 'png')))
        if no_overwrite == False or not os.path.isfile(tile_file):
            f = open(tile_file, 'wb')
            f.write(tile_data)
            f.close()

        count = count + 1
        if (count % 100) == 0:
            logger.debug("%s / %s tiles exported (%.1f%%, %.1f tiles/sec)" % (count, total_tiles, (float(count) / float(total_tiles)) * 100.0, count / (time.time() - start_time)))
        t = tiles.fetchone()

    logger.info("%s / %s tiles exported (100.0%%, %.1f tiles/sec)" % (count, total_tiles, count / (time.time() - start_time)))

    if delete_after_export:
        logger.debug("WARNING: Removing exported tiles from %s" % (mbtiles_file))

        if sending_mbtiles_is_compacted:
            cur.execute("""delete from images where tile_id in (select tile_id from map where zoom_level>=? and zoom_level<=?);""", (min_zoom, max_zoom))
            cur.execute("""delete from map where zoom_level>=? and zoom_level<=?;""", (min_zoom, max_zoom))
        else:
            cur.execute("""delete from tiles where zoom_level>=? and zoom_level<=?;""", (min_zoom, max_zoom))

        optimize_database(cur, kwargs.get('skip_analyze', False), kwargs.get('skip_vacuum', False))
        con.commit()

    con.close()

