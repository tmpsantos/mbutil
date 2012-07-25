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


def mbtiles_connect(mbtiles_file):
    try:
        con = sqlite3.connect(mbtiles_file)
        return con
    except Exception, e:
        logger.error("Could not connect to database")
        logger.exception(e)
        sys.exit(1)


def optimize_connection(cur):
    cur.execute("""PRAGMA synchronous=0""")
    cur.execute("""PRAGMA locking_mode=EXCLUSIVE""")
    cur.execute("""PRAGMA journal_mode=DELETE""")


def compression_prepare(cur):
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


def compression_finalize(cur):
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
    cur.execute("""vacuum;""")
    cur.execute("""analyze;""")


def mbtiles_setup(cur):
    compression_prepare(cur)
    compression_finalize(cur)


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


def compression_do(cur, con, chunk):
    overlapping = 0
    unique = 0
    total = 0
    cur.execute("select count(zoom_level) from tiles")
    res = cur.fetchone()
    total_tiles = res[0]
    logging.debug("%d total tiles to fetch" % total_tiles)

    for i in range(total_tiles / chunk):
        logging.debug("%d / %d rounds done" % (i, (total_tiles / chunk)))
        ids = []
        files = []
        start = time.time()
        cur.execute("""select zoom_level, tile_column, tile_row, tile_data
            from tiles where rowid > ? and rowid <= ?""", ((i * chunk), ((i + 1) * chunk)))
        logger.debug("select: %s" % (time.time() - start))
        rows = cur.fetchall()
        for r in rows:
            total = total + 1
            if r[3] in files:
                overlapping = overlapping + 1
                start = time.time()
                query = """insert into map
                    (zoom_level, tile_column, tile_row, tile_id)
                    values (?, ?, ?, ?)"""
                logger.debug("insert: %s" % (time.time() - start))
                cur.execute(query, (r[0], r[1], r[2], ids[files.index(r[3])]))
            else:
                unique = unique + 1
                id = str(uuid.uuid4())

                ids.append(id)
                files.append(r[3])

                start = time.time()
                query = """insert into images
                    (tile_id, tile_data)
                    values (?, ?)"""
                cur.execute(query, (str(id), sqlite3.Binary(r[3])))
                logger.debug("insert into images: %s" % (time.time() - start))
                start = time.time()
                query = """replace into map
                    (zoom_level, tile_column, tile_row, tile_id)
                    values (?, ?, ?, ?)"""
                cur.execute(query, (r[0], r[1], r[2], id))
                logger.debug("insert into map: %s" % (time.time() - start))
        con.commit()


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
        os.system(command % (tmp_file_name))

    tmp_file = open(tmp_file_name, "r")
    new_tile_data = tmp_file.read()
    tmp_file.close()

    os.remove(tmp_file_name)

    return new_tile_data


def execute_commands_on_mbtiles(mbtiles_file, **kwargs):
    logger.debug("Executing commands on MBTiles %s" % (mbtiles_file))

    if kwargs['command_list'] == None or len(kwargs['command_list']) == 0:
        return

    con = mbtiles_connect(mbtiles_file)
    cur = con.cursor()
    optimize_connection(cur)

    count = 0
    chunk = 100
    start_time = time.time()

    cur.execute("select count(tile_id) from images")
    res = cur.fetchone()
    total_tiles = res[0]
    logging.debug("%d total tiles" % total_tiles)

    cur.execute("select max(rowid) from images")
    res = cur.fetchone()
    max_rowid = res[0]

    for i in range((max_rowid / chunk) + 1):
        cur.execute("""select tile_id, tile_data from images where rowid > ? and rowid <= ?""",
            ((i * chunk), ((i + 1) * chunk)))
        rows = cur.fetchall()
        for r in rows:
            tile_id = r[0]
            tile_data = r[1]

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
    logger.debug("Importing disk to MBTiles")
    logger.debug("%s --> %s" % (directory_path, mbtiles_file))

    import_into_existing_mbtiles = os.path.isfile(mbtiles_file)
    existing_mbtiles_is_compacted = True

    con = mbtiles_connect(mbtiles_file)
    cur = con.cursor()
    optimize_connection(cur)

    if import_into_existing_mbtiles:
        cur.execute("select count(name) from sqlite_master where type='table' AND name='images';")
        res = cur.fetchone()
        existing_mbtiles_is_compacted = (res[0] > 0)
    else:
        mbtiles_setup(cur)

    image_format = 'png'
    grid_warning = True
    try:
        metadata = json.load(open(os.path.join(directory_path, 'metadata.json'), 'r'))
        image_format = metadata.get('format', 'png')
        # TODO: Check that the old and new image formats are the same
        if not import_into_existing_mbtiles:
            for name, value in metadata.items():
                cur.execute('insert into metadata (name, value) values (?, ?)',
                        (name, value))
            logger.info('metadata from metadata.json restored')
    except IOError:
        logger.warning('metadata.json not found')

    count = 0
    start_time = time.time()
    msg = ""
    for r1, zs, ignore in os.walk(os.path.join(directory_path, "tiles")):
        for z in zs:
            for r2, xs, ignore in os.walk(os.path.join(r1, z)):
                for x in xs:
                    for r2, ignore, ys in os.walk(os.path.join(r1, z, x)):
                        for y in ys:
                            if kwargs.get('flip_y') == True:
                                y = flip_y(z, y)

                            f = open(os.path.join(r1, z, x, y), 'rb')
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
                                    (z, x, y.split('.')[0], tile_id))
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
    optimize_database(con, False, import_into_existing_mbtiles)
    con.commit()
    con.close()


def merge_mbtiles(mbtiles_file1, mbtiles_file2, **kwargs):
    logger.debug("Merging MBTiles")
    logger.debug("%s --> %s" % (mbtiles_file2, mbtiles_file1))

    con1 = mbtiles_connect(mbtiles_file1)
    cur1 = con1.cursor()
    optimize_connection(cur1)

    cur1.execute("select count(name) from sqlite_master where type='table' AND name='images';")
    res = cur1.fetchone()
    existing_mbtiles_is_compacted = (res[0] > 0)

    if not existing_mbtiles_is_compacted:
        sys.stderr.write('To merge two MBTiles, the receiver must already be compacted\n')
        sys.exit(1)

    con2 = mbtiles_connect(mbtiles_file2)
    cur2 = con2.cursor()
    optimize_connection(cur2)

    # TODO: Check that the old and new image formats are the same

    cur1.execute('insert or ignore into metadata (name, value) values ("format", "png")')

    count = 0
    start_time = time.time()
    tiles = cur2.execute('select zoom_level, tile_column, tile_row, tile_data from tiles;')
    t = tiles.fetchone()
    while t:
        z = t[0]
        x = t[1]
        y = t[2]
        tile_data = t[3]

        if kwargs.get('flip_y') == True:
          y = flip_y(z, y)

        m = hashlib.md5()
        m.update(tile_data)
        tile_id = m.hexdigest()

        # Execute commands
        if kwargs['command_list']:
            tile_data = execute_commands_on_tile(kwargs['command_list'], "png", tile_data)

        # Update duplicates

        cur1.execute("""replace into images (tile_id, tile_data) values (?, ?);""",
            (tile_id, sqlite3.Binary(tile_data)))

        cur1.execute("""replace into map (zoom_level, tile_column, tile_row, tile_id)
            values (?, ?, ?, ?);""",
            (z, x, y, tile_id))

        count = count + 1
        if (count % 100) == 0:
            logger.debug("%s tiles merged (%.1f tiles/sec)" % (count, count / (time.time() - start_time)))

        t = tiles.fetchone()

    logger.debug("%s tiles merged (%.1f tiles/sec)" % (count, count / (time.time() - start_time)))
    con1.commit()
    con1.close()
    con2.close()


def mbtiles_to_disk(mbtiles_file, directory_path, **kwargs):
    logger.debug("Exporting MBTiles to disk")
    logger.debug("%s --> %s" % (mbtiles_file, directory_path))

    con = mbtiles_connect(mbtiles_file)

    os.mkdir("%s" % directory_path)
    metadata = dict(con.execute('select name, value from metadata;').fetchall())
    json.dump(metadata, open(os.path.join(directory_path, 'metadata.json'), 'w'), indent=4)

    count = con.execute('select count(zoom_level) from tiles;').fetchone()[0]
    done = 0

    base_path = os.path.join(directory_path, "tiles")
    if not os.path.isdir(base_path):
        os.makedirs(base_path)

    tiles = con.execute('select zoom_level, tile_column, tile_row, tile_data from tiles;')
    t = tiles.fetchone()
    while t:
        z = t[0]
        x = t[1]
        y = t[2]

        if kwargs.get('flip_y') == True:
          y = flip_y(z, y)

        tile_dir = os.path.join(base_path, str(z), str(x))
        if not os.path.isdir(tile_dir):
            os.makedirs(tile_dir)

        tile = os.path.join(tile_dir,'%s.%s' % (y,metadata.get('format', 'png')))
        f = open(tile, 'wb')
        f.write(t[3])
        f.close()

        done = done + 1
        logger.debug('%s / %s tiles exported' % (done, count))
        t = tiles.fetchone()

    con.close()


def check_mbtiles(mbtiles_file, zoom_level, **kwargs):
    logger.debug("Checking MBTiles file %s at zoom level %d" % (mbtiles_file, zoom_level))
    tiles_count = (2**zoom_level)

    logger.info("This does not work yet.")
