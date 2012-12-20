import sqlite3, uuid, sys, logging, time, os, json, zlib, hashlib, tempfile, multiprocessing

from util import mbtiles_connect, mbtiles_setup, compaction_update, optimize_connection, optimize_database, execute_commands_on_tile, process_tile, flip_y
from util_check import check_mbtiles
from multiprocessing import Pool

logger = logging.getLogger(__name__)


def merge_mbtiles(mbtiles_file1, mbtiles_file2, **kwargs):
    logger.info("Merging databases: %s --> %s" % (mbtiles_file2, mbtiles_file1))


    zoom          = kwargs.get('zoom', -1)
    min_zoom      = kwargs.get('min_zoom', 0)
    max_zoom      = kwargs.get('max_zoom', 18)

    tmp_dir         = kwargs.get('tmp_dir', None)
    no_overwrite    = kwargs.get('no_overwrite', False)
    auto_commit     = kwargs.get('auto_commit', False)
    wal_journal     = kwargs.get('wal_journal', False)
    synchronous_off = kwargs.get('synchronous_off', False)

    min_timestamp = kwargs.get('min_timestamp', 0)
    max_timestamp = kwargs.get('max_timestamp', 0)

    delete_after_export   = kwargs.get('delete_after_export', False)
    print_progress        = kwargs.get('progress', False)
    delete_vanished_tiles = kwargs.get('delete_vanished_tiles', False)

    if tmp_dir and not os.path.isdir(tmp_dir):
        os.mkdir(tmp_dir)

    if zoom >= 0:
        min_zoom = max_zoom = zoom


    check_before_merge = kwargs.get('check_before_merge', False)
    if check_before_merge and not check_mbtiles(mbtiles_file2, **kwargs):
        sys.stderr.write("The pre-merge check on %s failed\n" % (mbtiles_file2))
        sys.exit(1)


    con1 = mbtiles_connect(mbtiles_file1, auto_commit)
    cur1 = con1.cursor()
    optimize_connection(cur1, wal_journal, synchronous_off, False)

    con2 = mbtiles_connect(mbtiles_file2)
    cur2 = con2.cursor()
    optimize_connection(cur2)


    receiving_mbtiles_is_compacted = (con1.execute("SELECT count(name) FROM sqlite_master WHERE type='table' AND name='images'").fetchone()[0] > 0)
    sending_mbtiles_is_compacted = (con2.execute("SELECT count(name) FROM sqlite_master WHERE type='table' AND name='images'").fetchone()[0] > 0)
    if not receiving_mbtiles_is_compacted:
        con1.close()
        con2.close()
        sys.stderr.write('To merge two mbtiles databases, the receiver must already be compacted\n')
        sys.exit(1)

    if not sending_mbtiles_is_compacted and (min_timestamp != 0 or max_timestamp != 0):
        con.close()
        sys.stderr.write('min-timestamp/max-timestamp can only be used with compacted databases.\n')
        sys.exit(1)

    if receiving_mbtiles_is_compacted:
        compaction_update(cur1)


    # Check that the old and new image formats are the same
    original_format = new_format = None
    try:
        original_format = con1.execute("SELECT value FROM metadata WHERE name='format'").fetchone()[0]
    except:
        pass

    try:
        new_format = con2.execute("SELECT value FROM metadata WHERE name='format'").fetchone()[0]
    except:
        pass

    if new_format == None:
        logger.info("No image format found in the sending database, assuming 'png'")
        new_format = "png"

    if original_format != None and new_format != original_format:
        sys.stderr.write('The files to merge must use the same image format (png or jpg)\n')
        sys.exit(1)

    if original_format == None and new_format != None:
        con1.execute("""insert or ignore into metadata (name, value) values ("format", ?)""", [new_format])
        con1.commit()

    if new_format == None:
        new_format = original_format


    existing_tiles = {}

    if no_overwrite:
        tiles = cur1.execute("""SELECT zoom_level, tile_column, tile_row FROM tiles WHERE zoom_level>=? AND zoom_level<=?""",
            (min_zoom, max_zoom))

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
    chunk = 1000

    total_tiles = 0
    if min_timestamp > 0 and max_timestamp > 0:
        total_tiles = cur2.execute("""SELECT count(zoom_level) FROM map WHERE zoom_level>=? AND zoom_level<=? AND updated_at>? AND updated_at<?""",
            (min_zoom, max_zoom, min_timestamp, max_timestamp)).fetchone()[0]
    elif min_timestamp > 0:
        total_tiles = cur2.execute("""SELECT count(zoom_level) FROM map WHERE zoom_level>=? AND zoom_level<=? AND updated_at>?""",
            (min_zoom, max_zoom, min_timestamp)).fetchone()[0]
    elif max_timestamp > 0:
        total_tiles = cur2.execute("""SELECT count(zoom_level) FROM map WHERE zoom_level>=? AND zoom_level<=? AND updated_at<?""",
            (min_zoom, max_zoom, max_timestamp)).fetchone()[0]
    else:
        total_tiles = cur2.execute("""SELECT count(zoom_level) FROM map WHERE zoom_level>=? AND zoom_level<=?""",
            (min_zoom, max_zoom)).fetchone()[0]

    if total_tiles == 0:
        sys.stderr.write('No tiles to merge, exiting...\n')
        sys.exit(1)

    logger.debug("%d tiles to merge" % (total_tiles))
    if print_progress:
        sys.stdout.write("%d tiles to merge\n" % (total_tiles))
        sys.stdout.write("0 tiles merged (0% @ 0 tiles/sec)")
        sys.stdout.flush()


    # merge and process (--merge --execute)
    if sending_mbtiles_is_compacted and kwargs['command_list']:
        default_pool_size = kwargs.get('poolsize', -1)
        if default_pool_size < 1:
            default_pool_size = None
            logger.debug("Using default pool size")
        else:
            logger.debug("Using pool size = %d" % (default_pool_size))

        pool = Pool(default_pool_size)
        multiprocessing.log_to_stderr(logger.level)


        tiles_to_process = []
        known_tile_ids = {}
        max_rowid = (con2.execute("SELECT max(rowid) FROM map").fetchone()[0])


        # First: Merge images
        for i in range((max_rowid / chunk) + 1):
            if min_timestamp > 0 and max_timestamp > 0:
                cur2.execute("""SELECT map.zoom_level, map.tile_column, map.tile_row, images.tile_id, images.tile_data FROM images, map WHERE (map.rowid > ? AND map.rowid <= ?) AND (map.zoom_level>=? AND map.zoom_level<=?) AND (map.updated_at>? AND map.updated_at<?) AND (images.tile_id == map.tile_id)""",
                    ((i * chunk), ((i + 1) * chunk), min_zoom, max_zoom, min_timestamp, max_timestamp))
            elif min_timestamp > 0:
                cur2.execute("""SELECT map.zoom_level, map.tile_column, map.tile_row, images.tile_id, images.tile_data FROM images, map WHERE (map.rowid > ? AND map.rowid <= ?) AND (map.zoom_level>=? AND map.zoom_level<=?) AND (map.updated_at>?) AND (images.tile_id == map.tile_id)""",
                    ((i * chunk), ((i + 1) * chunk), min_zoom, max_zoom, min_timestamp))
            elif max_timestamp > 0:
                cur2.execute("""SELECT map.zoom_level, map.tile_column, map.tile_row, images.tile_id, images.tile_data FROM images, map WHERE (map.rowid > ? AND map.rowid <= ?) AND (map.zoom_level>=? AND map.zoom_level<=?) AND (map.updated_at<?) AND (images.tile_id == map.tile_id)""",
                    ((i * chunk), ((i + 1) * chunk), min_zoom, max_zoom, max_timestamp))
            else:
                cur2.execute("""SELECT map.zoom_level, map.tile_column, map.tile_row, images.tile_id, images.tile_data FROM images, map WHERE (map.rowid > ? AND map.rowid <= ?) AND (map.zoom_level>=? AND map.zoom_level<=?) AND (images.tile_id == map.tile_id)""",
                    ((i * chunk), ((i + 1) * chunk), min_zoom, max_zoom))

            rows = cur2.fetchall()
            for t in rows:
                z = t[0]
                x = t[1]
                y = t[2]
                tile_id = t[3]
                tile_data = t[4]

                if kwargs.get('flip_y', False) == True:
                    y = flip_y(z, y)

                if no_overwrite:
                    if x in existing_tiles.get(z, {}).get(y, set()):
                        logging.debug("Ignoring tile (%d, %d, %d)" % (z, x, y))
                        continue


                new_tile_id = known_tile_ids.get(tile_id)
                if new_tile_id is None:
                    tmp_file_fd, tmp_file_name = tempfile.mkstemp(suffix=".%s" % (new_format), prefix="tile_", dir=tmp_dir)
                    tmp_file = os.fdopen(tmp_file_fd, "w")
                    tmp_file.write(tile_data)
                    tmp_file.close()

                    tiles_to_process.append({
                        'tile_id':tile_id,
                        'filename':tmp_file_name,
                        'format':new_format,
                        'size':len(tile_data),
                        'command_list':kwargs['command_list'],
                        'x':x,
                        'y':y,
                        'z':z
                    })
                else:
                    cur1.execute("""REPLACE INTO map (zoom_level, tile_column, tile_row, tile_id, updated_at) VALUES (?, ?, ?, ?, ?)""",
                        (z, x, y, new_tile_id, int(time.time())))

                    count = count + 1
                    if (count % 100) == 0:
                        logger.debug("%d tiles merged (%.1f%% @ %.1f tiles/sec)" % (count, (float(count) / float(total_tiles)) * 100.0, count / (time.time() - start_time)))
                        if print_progress:
                            sys.stdout.write("\r%d tiles merged (%.1f%% @ %.1f tiles/sec)" % (count, (float(count) / float(total_tiles)) * 100.0, count / (time.time() - start_time)))
                            sys.stdout.flush()


            if len(tiles_to_process) == 0:
                continue

            # Execute commands
            processed_tiles = pool.map(process_tile, tiles_to_process)


            for next_tile in processed_tiles:
                tile_data = None
                tile_id, tile_file_path, original_size, x, y, z = next_tile['tile_id'], next_tile['filename'], next_tile['size'], next_tile['x'], next_tile['y'], next_tile['z']

                if not os.path.isfile(tile_file_path):
                    if delete_vanished_tiles:
                        logger.debug("Skipping vanished tile %s" % (tile_id, ))
                    else:
                        logger.error("tile %s vanished!" % (tile_id, ))
                    continue
                else:
                    tmp_file = open(tile_file_path, "r")
                    tile_data = tmp_file.read()
                    tmp_file.close()

                    os.remove(tile_file_path)

                if tile_data and len(tile_data) > 0:
                    m = hashlib.md5()
                    m.update(tile_data)
                    new_tile_id = m.hexdigest()
                    known_tile_ids[tile_id] = new_tile_id

                    cur1.execute("""REPLACE INTO images (tile_id, tile_data) VALUES (?, ?)""",
                        (new_tile_id, sqlite3.Binary(tile_data)))

                    cur1.execute("""REPLACE INTO map (zoom_level, tile_column, tile_row, tile_id, updated_at) VALUES (?, ?, ?, ?, ?)""",
                        (z, x, y, new_tile_id, int(time.time())))

                count = count + 1
                if (count % 100) == 0:
                    logger.debug("%d tiles merged (%.1f%% @ %.1f tiles/sec)" % (count, (float(count) / float(total_tiles)) * 100.0, count / (time.time() - start_time)))
                    if print_progress:
                        sys.stdout.write("\r%d tiles merged (%.1f%% @ %.1f tiles/sec)" % (count, (float(count) / float(total_tiles)) * 100.0, count / (time.time() - start_time)))
                        sys.stdout.flush()


            tiles_to_process = []
            processed_tiles = []


    # merge from a compacted database (--merge)
    elif sending_mbtiles_is_compacted:
        known_tile_ids = {}

        # First: Merge images
        if min_timestamp > 0 and max_timestamp > 0:
            tiles = cur2.execute("""SELECT map.zoom_level, map.tile_column, map.tile_row, images.tile_id, images.tile_data FROM images, map WHERE map.zoom_level>=? AND map.zoom_level<=? AND map.updated_at>? AND map.updated_at<? AND images.tile_id=map.tile_id""",
                (min_zoom, max_zoom, min_timestamp, max_timestamp))
        elif min_timestamp > 0:
            tiles = cur2.execute("""SELECT map.zoom_level, map.tile_column, map.tile_row, images.tile_id, images.tile_data FROM images, map WHERE map.zoom_level>=? AND map.zoom_level<=? AND map.updated_at>? AND images.tile_id=map.tile_id""",
                (min_zoom, max_zoom, min_timestamp))
        elif max_timestamp > 0:
            tiles = cur2.execute("""SELECT map.zoom_level, map.tile_column, map.tile_row, images.tile_id, images.tile_data FROM images, map WHERE map.zoom_level>=? AND map.zoom_level<=? AND map.updated_at<? AND images.tile_id=map.tile_id""",
                (min_zoom, max_zoom, max_timestamp))
        else:
            tiles = cur2.execute("""SELECT map.zoom_level, map.tile_column, map.tile_row, images.tile_id, images.tile_data FROM images, map WHERE map.zoom_level>=? AND map.zoom_level<=? AND images.tile_id=map.tile_id""",
                (min_zoom, max_zoom))

        t = tiles.fetchone()
        while t:
            z = t[0]
            x = t[1]
            y = t[2]
            tile_id = t[3]
            tile_data = t[4]

            if kwargs.get('flip_y', False) == True:
                y = flip_y(z, y)

            if no_overwrite:
                if x in existing_tiles.get(z, {}).get(y, set()):
                    logging.debug("Ignoring tile (%d, %d, %d)" % (z, x, y))
                    t = tiles.fetchone()
                    continue


            new_tile_id = known_tile_ids.get(tile_id)
            if new_tile_id is None:
                # Execute commands
                if kwargs.get('command_list'):
                    tile_data = execute_commands_on_tile(kwargs['command_list'], new_format, tile_data, tmp_dir)

                m = hashlib.md5()
                m.update(tile_data)
                new_tile_id = m.hexdigest()
                known_tile_ids[tile_id] = new_tile_id

                cur1.execute("""REPLACE INTO images (tile_id, tile_data) VALUES (?, ?)""",
                    (new_tile_id, sqlite3.Binary(tile_data)))


            cur1.execute("""REPLACE INTO map (zoom_level, tile_column, tile_row, tile_id, updated_at) VALUES (?, ?, ?, ?, ?)""",
                (z, x, y, new_tile_id, int(time.time())))

            count = count + 1
            if (count % 100) == 0:
                logger.debug("%d tiles merged (%.1f%% @ %.1f tiles/sec)" % (count, (float(count) / float(total_tiles)) * 100.0, count / (time.time() - start_time)))
                if print_progress:
                    sys.stdout.write("\r%d tiles merged (%.1f%% @ %.1f tiles/sec)" % (count, (float(count) / float(total_tiles)) * 100.0, count / (time.time() - start_time)))
                    sys.stdout.flush()

            t = tiles.fetchone()


    # merge an uncompacted database (--merge)
    else:
        known_tile_ids = set()

        tiles = cur2.execute("""SELECT zoom_level, tile_column, tile_row, tile_data FROM tiles WHERE zoom_level>=? AND zoom_level<=?""",
            (min_zoom, max_zoom))

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

            if kwargs.get('flip_y', False) == True:
                y = flip_y(z, y)


            # Execute commands
            if kwargs.get('command_list'):
                tile_data = execute_commands_on_tile(kwargs['command_list'], new_format, tile_data, tmp_dir)

            m = hashlib.md5()
            m.update(tile_data)
            tile_id = m.hexdigest()

            if tile_id not in known_tile_ids:
                cur1.execute("""REPLACE INTO images (tile_id, tile_data) VALUES (?, ?)""",
                    (tile_id, sqlite3.Binary(tile_data)))

            cur1.execute("""REPLACE INTO map (zoom_level, tile_column, tile_row, tile_id, updated_at) VALUES (?, ?, ?, ?, ?)""",
                (z, x, y, tile_id, int(time.time())))

            known_tile_ids.add(tile_id)

            count = count + 1
            if (count % 100) == 0:
                logger.debug("%d tiles merged (%.1f%% @ %.1f tiles/sec)" % (count, (float(count) / float(total_tiles)) * 100.0, count / (time.time() - start_time)))
                if print_progress:
                    sys.stdout.write("\r%d tiles merged (%.1f%% @ %.1f tiles/sec)" % (count, (float(count) / float(total_tiles)) * 100.0, count / (time.time() - start_time)))
                    sys.stdout.flush()

            t = tiles.fetchone()


    if print_progress:
        sys.stdout.write('\n')

    logger.info("%d tiles merged (100.0%% @ %.1f tiles/sec)" % (count, count / (time.time() - start_time)))
    if print_progress:
        sys.stdout.write("%d tiles merged (100.0%% @ %.1f tiles/sec)\n" % (count, count / (time.time() - start_time)))
        sys.stdout.flush()


    if delete_after_export:
        logger.debug("WARNING: Removing merged tiles from %s" % (mbtiles_file2))

        if sending_mbtiles_is_compacted:
            if min_timestamp > 0 and max_timestamp > 0:
                cur2.execute("""DELETE FROM images WHERE tile_id IN (SELECT tile_id FROM map WHERE zoom_level>=? AND zoom_level<=? AND updated_at>? AND updated_at<?)""",
                    (min_zoom, max_zoom, min_timestamp, max_timestamp))
                cur2.execute("""DELETE FROM map WHERE zoom_level>=? AND zoom_level<=? AND updated_at>? AND updated_at<?""", (min_zoom, max_zoom, min_timestamp, max_timestamp))
            elif min_timestamp > 0:
                cur2.execute("""DELETE FROM images WHERE tile_id IN (SELECT tile_id FROM map WHERE zoom_level>=? AND zoom_level<=? AND updated_at>?)""",
                    (min_zoom, max_zoom, min_timestamp))
                cur2.execute("""DELETE FROM map WHERE zoom_level>=? AND zoom_level<=? AND updated_at>?""", (min_zoom, max_zoom, min_timestamp))
            elif max_timestamp > 0:
                cur2.execute("""DELETE FROM images WHERE tile_id IN (SELECT tile_id FROM map WHERE zoom_level>=? AND zoom_level<=? AND updated_at<?)""",
                    (min_zoom, max_zoom, max_timestamp))
                cur2.execute("""DELETE FROM map WHERE zoom_level>=? AND zoom_level<=? AND updated_at<?""", (min_zoom, max_zoom, max_timestamp))
            else:
                cur2.execute("""DELETE FROM images WHERE tile_id IN (SELECT tile_id FROM map WHERE zoom_level>=? AND zoom_level<=?)""",
                    (min_zoom, max_zoom))
                cur2.execute("""DELETE FROM map WHERE zoom_level>=? AND zoom_level<=?""", (min_zoom, max_zoom))
        else:
            cur2.execute("""DELETE FROM tiles WHERE zoom_level>=? AND zoom_level<=?""",
                (min_zoom, max_zoom))

        optimize_database(cur2, kwargs.get('skip_analyze', False), kwargs.get('skip_vacuum', False))
        con2.commit()


    con1.commit()
    con1.close()
    con2.close()
