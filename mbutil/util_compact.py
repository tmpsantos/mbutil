import sqlite3, uuid, sys, logging, time, os, json, zlib, hashlib, tempfile

logger = logging.getLogger(__name__)

from util import mbtiles_connect, optimize_connection, optimize_database, execute_commands_on_tile, compaction_prepare, compaction_finalize


def compact_mbtiles(mbtiles_file, **kwargs):
    logger.info("Compacting database %s" % (mbtiles_file))


    wal_journal = kwargs.get('wal_journal', False)
    synchronous_off = kwargs.get('synchronous_off', False)
    tmp_dir = kwargs.get('tmp_dir', None)
    print_progress = kwargs.get('progress', False)

    if tmp_dir and not os.path.isdir(tmp_dir):
        os.mkdir(tmp_dir)


    con = mbtiles_connect(mbtiles_file)
    cur = con.cursor()
    optimize_connection(cur, wal_journal, synchronous_off)

    existing_mbtiles_is_compacted = (con.execute("SELECT count(name) FROM sqlite_master WHERE type='table' AND name='images'").fetchone()[0] > 0)
    if existing_mbtiles_is_compacted:
        logger.info("The mbtiles file is already compacted")
        return


    overlapping = 0
    unique = 0
    count = 0
    chunk = 100
    start_time = time.time()
    total_tiles = con.execute("SELECT count(zoom_level) FROM tiles").fetchone()[0]
    max_rowid = con.execute("SELECT max(rowid) FROM tiles").fetchone()[0]


    logger.debug("%d total tiles" % total_tiles)
    if print_progress:
        sys.stdout.write("%d tiles tiles\n" % (total_tiles))
        sys.stdout.write("0 tiles finished, 0 unique, 0 duplicates (0% @ 0 tiles/sec)")
        sys.stdout.flush()


    compaction_prepare(cur)

    for i in range((max_rowid / chunk) + 1):
        cur.execute("""SELECT zoom_level, tile_column, tile_row, tile_data FROM tiles WHERE rowid > ? AND rowid <= ?""",
            ((i * chunk), ((i + 1) * chunk)))

        rows = cur.fetchall()
        for r in rows:
            z = r[0]
            x = r[1]
            y = r[2]
            tile_data = r[3]

            # Execute commands
            if kwargs.get('command_list'):
                tile_data = execute_commands_on_tile(kwargs['command_list'], "png", tile_data, tmp_dir)

            m = hashlib.md5()
            m.update(tile_data)
            tile_id = m.hexdigest()

            try:
                cur.execute("""INSERT INTO images (tile_id, tile_data) VALUES (?, ?)""",
                    (tile_id, sqlite3.Binary(tile_data)))
            except:
                overlapping = overlapping + 1
            else:
                unique = unique + 1

            cur.execute("""REPLACE INTO map (zoom_level, tile_column, tile_row, tile_id, updated_at) VALUES (?, ?, ?, ?, ?)""",
                (z, x, y, tile_id, int(time.time())))


            count = count + 1
            if (count % 100) == 0:
                logger.debug("%d tiles finished, %d unique, %d duplicates (%.1f%% @ %.1f tiles/sec)" %
                    (count, unique, overlapping, (float(count) / float(total_tiles)) * 100.0, count / (time.time() - start_time)))
                if print_progress:
                    sys.stdout.write("\r%d tiles finished, %d unique, %d duplicates (%.1f%% @ %.1f tiles/sec)" %
                        (count, unique, overlapping, (float(count) / float(total_tiles)) * 100.0, count / (time.time() - start_time)))
                    sys.stdout.flush()


    if print_progress:
        sys.stdout.write('\n')

    logger.info("%d tiles finished, %d unique, %d duplicates (100.0%% @ %.1f tiles/sec)" % (count, unique, overlapping, count / (time.time() - start_time)))
    if print_progress:
        sys.stdout.write("%d tiles finished, %d unique, %d duplicates (100.0%% @ %.1f tiles/sec)\n" % (count, unique, overlapping, count / (time.time() - start_time)))
        sys.stdout.flush()

    compaction_finalize(cur)
    con.commit()
    con.close()
