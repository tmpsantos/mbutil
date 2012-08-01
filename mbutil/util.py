#!/usr/bin/env python

# MBUtil: a tool for MBTiles files
# Supports importing, exporting, and more
#
# (c) Development Seed 2012
# Licensed under BSD

import sqlite3, uuid, sys, logging, time, os, json, zlib, hashlib, tempfile

logger = logging.getLogger(__name__)

def flip_y(zoom, y):
    return (2**zoom-1) - y


def mbtiles_connect(mbtiles_file, auto_commit=False):
    try:
        con = sqlite3.connect(mbtiles_file)
        if auto_commit:
            con.isolation_level = None
        return con
    except Exception, e:
        logger.error("Could not connect to database")
        logger.exception(e)
        sys.exit(1)


def optimize_connection(cur, exclusive_lock=True):
    cur.execute("""PRAGMA synchronous=0""")
    cur.execute("""PRAGMA journal_mode=DELETE""")
    if exclusive_lock:
        cur.execute("""PRAGMA locking_mode=EXCLUSIVE""")


def compaction_prepare(cur):
    cur.execute("""
        CREATE TABLE if not exists images (
        tile_data blob,
        tile_id VARCHAR(256));""")
    cur.execute("""
        CREATE TABLE if not exists map (
        zoom_level integer,
        tile_column integer,
        tile_row integer,
        tile_id VARCHAR(256));""")
    cur.execute("""
        CREATE TABLE if not exists metadata (
        name text, value text);""")
    cur.execute("""
        CREATE UNIQUE INDEX name ON metadata (name);""")


def compaction_finalize(cur):
    try:
        cur.execute("""drop table tiles;""")
    except sqlite3.OperationalError:
        pass
    cur.execute("""create view tiles as
        select map.zoom_level as zoom_level,
        map.tile_column as tile_column,
        map.tile_row as tile_row,
        images.tile_data as tile_data FROM
        map JOIN images on images.tile_id = map.tile_id;""")
    cur.execute("""
          CREATE UNIQUE INDEX map_index on map
            (zoom_level, tile_column, tile_row);""")
    cur.execute("""
          CREATE UNIQUE INDEX images_id on images
            (tile_id);""")


def mbtiles_setup(cur):
    compaction_prepare(cur)
    compaction_finalize(cur)


def optimize_database(cur, skip_analyze, skip_vacuum):
    if not skip_analyze:
        logger.debug('analyzing db')
        cur.execute("""ANALYZE;""")

    if not skip_vacuum:
        logger.debug('cleaning db')
        cur.execute("""VACUUM;""")


def optimize_database_file(mbtiles_file, skip_analyze, skip_vacuum):
    con = mbtiles_connect(mbtiles_file)
    cur = con.cursor()
    optimize_connection(cur)
    optimize_database(cur, skip_analyze, skip_vacuum)
    con.commit()
    con.close()


def compact_mbtiles(mbtiles_file):
    logger.debug("Compacting MBTiles database %s" % (mbtiles_file))

    con = mbtiles_connect(mbtiles_file)
    cur = con.cursor()
    optimize_connection(cur)

    cur.execute("select count(name) from sqlite_master where type='table' AND name='images';")
    res = cur.fetchone()
    existing_mbtiles_is_compacted = (res[0] > 0)

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
                logger.debug("%s tiles finished, %d unique, %d duplicates (%.1f tiles/sec)" % (count, unique, overlapping, count / (time.time() - start_time)))

    logger.debug("%s tiles finished, %d unique, %d duplicates (%.1f tiles/sec)" % (count, unique, overlapping, count / (time.time() - start_time)))

    compaction_finalize(cur)
    con.commit()
    con.close()


def mbtiles_create(mbtiles_file, **kwargs):
    logger.info("Creating empty MBTiles database %s" % mbtiles_file)
    con = mbtiles_connect(mbtiles_file)
    cur = con.cursor()
    optimize_connection(cur)
    mbtiles_setup(cur)
    con.commit()
    con.close()


def execute_commands_on_tile(command_list, image_format, tile_data):
    if command_list == None or tile_data == None:
        return tile_data

    tmp_file_fd, tmp_file_name = tempfile.mkstemp(suffix=".%s" % (image_format), prefix="tile_")
    tmp_file = os.fdopen(tmp_file_fd, "w")
    tmp_file.write(tile_data)
    tmp_file.close()

    for command in command_list:
        logger.debug("Executing command: %s" % command)
        os.system(command % (tmp_file_name))

    tmp_file = open(tmp_file_name, "r")
    new_tile_data = tmp_file.read()
    tmp_file.close()

    os.remove(tmp_file_name)

    return new_tile_data


def execute_commands_on_mbtiles(mbtiles_file, **kwargs):
    logger.debug("Executing commands on MBTiles database %s" % (mbtiles_file))

    if kwargs['command_list'] == None or len(kwargs['command_list']) == 0:
        return

    auto_commit = kwargs.get('auto_commit')

    con = mbtiles_connect(mbtiles_file, auto_commit)
    cur = con.cursor()
    optimize_connection(cur)

    count = 0
    chunk = 100
    start_time = time.time()

    min_zoom = kwargs.get('min_zoom')
    max_zoom = kwargs.get('max_zoom')

    cur.execute("""select count(tile_id) from map where zoom_level>=? and zoom_level<=?""", (min_zoom, max_zoom))
    res = cur.fetchone()
    total_tiles = res[0]
    logging.debug("%d total tiles" % total_tiles)

    cur.execute("select max(rowid) from map")
    res = cur.fetchone()
    max_rowid = res[0]

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

            # Execute commands
            tile_data = execute_commands_on_tile(kwargs['command_list'], "png", tile_data)
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
                logger.debug("%s tiles finished (%.1f tiles/sec)" % (count, count / (time.time() - start_time)))

        logging.debug("%d / %d rounds done" % (i+1, (max_rowid / chunk)+1))

    logger.debug("%s tiles finished (%.1f tiles/sec)" % (count, count / (time.time() - start_time)))
    con.commit()
    con.close()


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

    min_zoom = kwargs.get('min_zoom')
    max_zoom = kwargs.get('max_zoom')
    no_overwrite = kwargs.get('no_overwrite')

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
                                tile_data = execute_commands_on_tile(kwargs['command_list'], "png", tile_data)

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
                                cur.execute("""replace into tiles (zoom_level,
                                    tile_column, tile_row, tile_data) values
                                    (?, ?, ?, ?);""",
                                    (z, x, y.split('.')[0], sqlite3.Binary(tile_data)))

                            count = count + 1
                            if (count % 100) == 0:
                                for c in msg: sys.stdout.write(chr(8))
                                logger.debug("%s tiles inserted (%d tiles/sec)" % (count, count / (time.time() - start_time)))

    logger.debug('tiles inserted.')
    con.commit()
    con.close()


def merge_mbtiles(mbtiles_file1, mbtiles_file2, **kwargs):
    logger.debug("Merging MBTiles databases: %s --> %s" % (mbtiles_file2, mbtiles_file1))

    auto_commit = kwargs.get('auto_commit')

    con1 = mbtiles_connect(mbtiles_file1, auto_commit)
    cur1 = con1.cursor()
    optimize_connection(cur1, False)

    cur1.execute("select count(name) from sqlite_master where type='table' AND name='images';")
    res = cur1.fetchone()
    existing_mbtiles_is_compacted = (res[0] > 0)

    if not existing_mbtiles_is_compacted:
        sys.stderr.write('To merge two MBTiles, the receiver must already be compacted\n')
        sys.exit(1)

    con2 = mbtiles_connect(mbtiles_file2)
    cur2 = con2.cursor()
    optimize_connection(cur2)

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

    min_zoom = kwargs.get('min_zoom')
    max_zoom = kwargs.get('max_zoom')
    no_overwrite = kwargs.get('no_overwrite')

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
            tile_data = execute_commands_on_tile(kwargs['command_list'], "png", tile_data)

        m = hashlib.md5()
        m.update(tile_data)
        tile_id = m.hexdigest()

        if tile_id not in known_tile_ids:
            cur1.execute("""replace into images (tile_id, tile_data) values (?, ?);""",
                (tile_id, sqlite3.Binary(tile_data)))

        cur1.execute("""replace into map (zoom_level, tile_column, tile_row, tile_id)
            values (?, ?, ?, ?);""",
            (z, x, y, tile_id))

        known_tile_ids.add(tile_id)
        count = count + 1
        if (count % 100) == 0:
            logger.debug("%s tiles merged (%.1f tiles/sec)" % (count, count / (time.time() - start_time)))

        t = tiles.fetchone()

    logger.debug("%s tiles merged (%.1f tiles/sec)" % (count, count / (time.time() - start_time)))
    con1.commit()
    con1.close()
    con2.close()


def mbtiles_to_disk(mbtiles_file, directory_path, **kwargs):
    logger.debug("Exporting MBTiles to disk: %s --> %s" % (mbtiles_file, directory_path))

    con = mbtiles_connect(mbtiles_file)

    os.mkdir("%s" % directory_path)
    metadata = dict(con.execute('select name, value from metadata;').fetchall())
    json.dump(metadata, open(os.path.join(directory_path, 'metadata.json'), 'w'), indent=4)

    count = con.execute('select count(zoom_level) from tiles;').fetchone()[0]
    done = 0

    base_path = os.path.join(directory_path, "tiles")
    if not os.path.isdir(base_path):
        os.makedirs(base_path)

    min_zoom = kwargs.get('min_zoom')
    max_zoom = kwargs.get('max_zoom')

    tiles = con.execute("""select zoom_level, tile_column, tile_row, tile_data from tiles where zoom_level>=? and zoom_level<=?;""", (min_zoom, max_zoom))
    t = tiles.fetchone()
    while t:
        z = t[0]
        x = t[1]
        y = t[2]
        tile_data = t[3]

        # Execute commands
        if kwargs['command_list']:
            tile_data = execute_commands_on_tile(kwargs['command_list'], "png", tile_data)

        if kwargs.get('flip_y') == True:
          y = flip_y(z, y)

        tile_dir = os.path.join(base_path, str(z), str(x))
        if not os.path.isdir(tile_dir):
            os.makedirs(tile_dir)

        tile = os.path.join(tile_dir,'%s.%s' % (y, metadata.get('format', 'png')))
        f = open(tile, 'wb')
        f.write(tile_data)
        f.close()

        done = done + 1
        logger.debug('%s / %s tiles exported' % (done, count))
        t = tiles.fetchone()

    con.close()


def check_mbtiles(mbtiles_file, **kwargs):
    logger.debug("Checking MBTiles database %s" % (mbtiles_file))

    result = True
    min_zoom = kwargs.get('min_zoom')
    max_zoom = kwargs.get('max_zoom')

    con = mbtiles_connect(mbtiles_file)
    cur = con.cursor()
    optimize_connection(cur)

    zoom_levels = [int(x[0]) for x in cur.execute("select distinct(zoom_level) from tiles;").fetchall()]
    missing_tiles = []

    for current_zoom_level in zoom_levels:
        if current_zoom_level < min_zoom or current_zoom_level > max_zoom:
            continue

        logger.debug("Starting zoom level %d" % (current_zoom_level))

        t = cur.execute("""select min(tile_column), max(tile_column),
                           min(tile_row), max(tile_row)
                           from tiles where zoom_level = ?""", [current_zoom_level]).fetchone()

        minX, maxX, minY, maxY = t[0], t[1], t[2], t[3]

        logger.debug(" - Checking zoom level %d, x: %d - %d, y: %d - %d" % (current_zoom_level, minX, maxX, minY, maxY))

        for current_row in range(minY, maxY+1):
            logger.debug("   - Row: %d" % (current_row))
            mbtiles_columns = set([int(x[0]) for x in cur.execute("""select tile_column from tiles where zoom_level=? and tile_row=?""", (current_zoom_level, current_row)).fetchall()])
            for current_column in range(minX, maxX+1):
                if current_column not in mbtiles_columns:
                    missing_tiles.append([current_zoom_level, current_column, current_row])

    if len(missing_tiles) > 0:
        result = False
        logger.error("(zoom, x, y)")
        for current_tile in missing_tiles:
            logger.error(current_tile)

    return result
