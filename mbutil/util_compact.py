import sqlite3, uuid, sys, logging, time, os, json, zlib, hashlib, tempfile

logger = logging.getLogger(__name__)

from util import mbtiles_connect, optimize_connection, optimize_database, execute_commands_on_tile, compaction_prepare, compaction_finalize


def compact_mbtiles(mbtiles_file):
    logger.info("Compacting MBTiles database %s" % (mbtiles_file))

    con = mbtiles_connect(mbtiles_file)
    cur = con.cursor()
    optimize_connection(cur)

    existing_mbtiles_is_compacted = (cur.execute("select count(name) from sqlite_master where type='table' AND name='images';").fetchone()[0] > 0)
    if existing_mbtiles_is_compacted:
        logger.info("The mbtiles file is already compacted")
        return

    total_tiles = cur.execute("select count(zoom_level) from tiles").fetchone()[0]
    max_rowid = cur.execute("select max(rowid) from tiles").fetchone()[0]

    overlapping = 0
    unique = 0
    count = 0
    chunk = 100
    start_time = time.time()

    logging.debug("%d total tiles" % total_tiles)

    compaction_prepare(cur)

    for i in range((max_rowid / chunk) + 1):
        cur.execute("""select zoom_level, tile_column, tile_row, tile_data from tiles where rowid > ? and rowid <= ?""",
            ((i * chunk), ((i + 1) * chunk)))

        rows = cur.fetchall()
        for r in rows:
            z = r[0]
            x = r[1]
            y = r[2]
            tile_data = r[3]

            # Execute commands
            if kwargs['command_list']:
                tile_data = execute_commands_on_tile(kwargs['command_list'], "png", tile_data)

            m = hashlib.md5()
            m.update(tile_data)
            tile_id = m.hexdigest()

            try:
                cur.execute("""insert into images (tile_id, tile_data) values (?, ?);""",
                    (tile_id, sqlite3.Binary(tile_data)))
            except:
                overlapping = overlapping + 1
            else:
                unique = unique + 1

            cur.execute("""replace into map (zoom_level, tile_column, tile_row, tile_id)
                values (?, ?, ?, ?);""",
                (z, x, y, tile_id))

            count = count + 1
            if (count % 100) == 0:
                logger.debug("%s tiles finished, %d unique, %d duplicates (%.1f%%, %.1f tiles/sec)" % (count, unique, overlapping, (float(count) / float(total_tiles)) * 100.0, count / (time.time() - start_time)))

    logger.info("%s tiles finished, %d unique, %d duplicates (100.0%%, %.1f tiles/sec)" % (count, unique, overlapping, count / (time.time() - start_time)))

    compaction_finalize(cur)
    con.commit()
    con.close()
