import sqlite3, uuid, sys, logging, time, os, json, zlib, hashlib, tempfile

from util import mbtiles_connect, mbtiles_setup, optimize_connection, optimize_database, execute_commands_on_tile

logger = logging.getLogger(__name__)


def execute_commands_on_mbtiles(mbtiles_file, **kwargs):
    logger.info("Executing commands on database %s" % (mbtiles_file))

    if kwargs['command_list'] == None or len(kwargs['command_list']) == 0:
        return

    auto_commit = kwargs.get('auto_commit')

    con = mbtiles_connect(mbtiles_file, auto_commit)
    cur = con.cursor()
    optimize_connection(cur)

    existing_mbtiles_is_compacted = (cur.execute("select count(name) from sqlite_master where type='table' AND name='images';").fetchone()[0] > 0)
    if not existing_mbtiles_is_compacted:
        logger.info("The mbtiles file must be compacted, exiting...")
        return

    image_format = 'png'
    try:
        image_format = cur.execute("select value from metadata where name='format';").fetchone()[0]
    except:
        pass

    count = 0
    duplicates = 0
    chunk = 100
    start_time = time.time()
    processed_tile_ids = set()

    zoom     = kwargs.get('zoom')
    min_zoom = kwargs.get('min_zoom')
    max_zoom = kwargs.get('max_zoom')

    if zoom >= 0:
        min_zoom = max_zoom = zoom

    total_tiles = (cur.execute("""select count(tile_id) from map where zoom_level>=? and zoom_level<=?""", (min_zoom, max_zoom)).fetchone()[0])
    max_rowid = (cur.execute("select max(rowid) from map").fetchone()[0])

    logging.debug("%d total tiles to process" % (total_tiles))

    for i in range((max_rowid / chunk) + 1):
        cur.execute("""select images.tile_id, images.tile_data, map.zoom_level, map.tile_column, map.tile_row from map, images where (map.rowid > ? and map.rowid <= ?) and (map.zoom_level>=? and map.zoom_level<=?) and (images.tile_id == map.tile_id)""",
            ((i * chunk), ((i + 1) * chunk), min_zoom, max_zoom))
        rows = cur.fetchall()
        for r in rows:
            tile_id = r[0]
            tile_data = r[1]
            # tile_z = r[2]
            # tile_x = r[3]
            # tile_y = r[4]
            # logging.debug("Working on tile (%d, %d, %d)" % (tile_z, tile_x, tile_y))

            if tile_id in processed_tile_ids:
                duplicates = duplicates + 1
            else:
                processed_tile_ids.add(tile_id)

                # Execute commands
                tile_data = execute_commands_on_tile(kwargs['command_list'], image_format, tile_data)
                if tile_data and len(tile_data) > 0:
                    m = hashlib.md5()
                    m.update(tile_data)
                    new_tile_id = m.hexdigest()

                    cur.execute("""insert or ignore into images (tile_id, tile_data) values (?, ?);""",
                        (new_tile_id, sqlite3.Binary(tile_data)))
                    cur.execute("""update map set tile_id=? where tile_id=?;""",
                        (new_tile_id, tile_id))
                    if tile_id != new_tile_id:
                        cur.execute("""delete from images where tile_id=?;""",
                            [tile_id])

            count = count + 1
            if (count % 100) == 0:
                logger.debug("%s tiles finished (%.1f%%, %.1f tiles/sec)" % (count, (float(count) / float(total_tiles)) * 100.0, count / (time.time() - start_time)))

    logger.info("%s tiles finished, %d duplicates ignored (100.0%%, %.1f tiles/sec)" % (count, duplicates, count / (time.time() - start_time)))

    con.commit()
    con.close()
