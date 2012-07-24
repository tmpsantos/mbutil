#!/usr/bin/env python

# MBUtil: a tool for MBTiles files
# Supports importing, exporting, and more
#
# (c) Development Seed 2012
# Licensed under BSD

import sqlite3, uuid, sys, logging, time, os, json, zlib, hashlib

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
        tile_id VARCHAR(256));
    """)
    cur.execute("""
      CREATE TABLE if not exists map (
        zoom_level integer,
        tile_column integer,
        tile_row integer,
        tile_id VARCHAR(256));
    """)

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

def optimize_database(cur, skip_vacuum):
    logger.debug('analyzing db')
    cur.execute("""ANALYZE;""")

    if not skip_vacuum:
        logger.debug('cleaning db')
        cur.execute("""VACUUM;""")

def optimize_database_file(mbtiles_file):
    con = mbtiles_connect(mbtiles_file)
    cur = con.cursor()
    optimize_connection(cur)
    optimize_database(cur, False)

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
                query = """insert into map
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

def disk_to_mbtiles(directory_path, mbtiles_file, **kwargs):
    logger.info("Importing disk to MBTiles")
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
    for r1, zs, ignore in os.walk(directory_path):
        for z in zs:
            for r2, xs, ignore in os.walk(os.path.join(r1, z)):
                for x in xs:
                    for r2, ignore, ys in os.walk(os.path.join(r1, z, x)):
                        for y in ys:
                            if (y.endswith(image_format)):
                                f = open(os.path.join(r1, z, x, y), 'rb')

                                if existing_mbtiles_is_compacted:
                                    tile_data = f.read()
                                    m = hashlib.md5()
                                    m.update(tile_data)
                                    tile_id = m.hexdigest()

                                    try:
                                        cur.execute("""insert into images (tile_id, tile_data) values (?, ?);""",
                                            (tile_id, sqlite3.Binary(tile_data)))
                                    except sqlite3.IntegrityError:
                                        pass

                                    try:
                                        cur.execute("""insert into map (zoom_level, tile_column, tile_row, tile_id)
                                            values (?, ?, ?, ?);""",
                                            (z, x, y.split('.')[0], tile_id))
                                    except sqlite3.IntegrityError:
                                        pass
                                else:
                                    cur.execute("""insert into tiles (zoom_level,
                                        tile_column, tile_row, tile_data) values
                                        (?, ?, ?, ?);""",
                                        (z, x, y.split('.')[0], sqlite3.Binary(f.read())))

                                f.close()
                                count = count + 1
                                if (count % 100) == 0:
                                    for c in msg: sys.stdout.write(chr(8))
                                    logger.debug("%s tiles inserted (%d tiles/sec)" % (count, count / (time.time() - start_time)))
                            elif (y.endswith('grid.json')):
                                if grid_warning:
                                    logger.warning('grid.json interactivity import not yet supported\n')
                                    grid_warning= False
    logger.debug('tiles inserted.')
    optimize_database(con, import_into_existing_mbtiles)

def merge_mbtiles(mbtiles_file1, mbtiles_file2, **kwargs):
    logger.info("Merging MBTiles")
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

    count = 0
    start_time = time.time()
    tiles = cur2.execute('select zoom_level, tile_column, tile_row, tile_data from tiles;')
    t = tiles.fetchone()
    while t:
        z = t[0]
        x = t[1]
        y = t[2]

        tile_data = t[3]
        m = hashlib.md5()
        m.update(tile_data)
        tile_id = m.hexdigest()

        try:
            cur1.execute("""insert into images (tile_id, tile_data) values (?, ?);""",
                (tile_id, sqlite3.Binary(tile_data)))
        except sqlite3.IntegrityError:
            pass

        try:
            cur1.execute("""insert into map (zoom_level, tile_column, tile_row, tile_id)
                values (?, ?, ?, ?);""",
                (z, x, y, tile_id))
        except sqlite3.IntegrityError:
            pass

        count = count + 1
        if (count % 100) == 0:
            logger.debug("%s tiles merged (%d tiles/sec)" % (count, count / (time.time() - start_time)))

        t = tiles.fetchone()

    logger.debug("%s tiles merged (%d tiles/sec)" % (count, count / (time.time() - start_time)))
    con1.commit()
    con1.close()

def mbtiles_to_disk(mbtiles_file, directory_path, **kwargs):
    logger.debug("Exporting MBTiles to disk")
    logger.debug("%s --> %s" % (mbtiles_file, directory_path))
    con = mbtiles_connect(mbtiles_file)
    os.mkdir("%s" % directory_path)
    metadata = dict(con.execute('select name, value from metadata;').fetchall())
    json.dump(metadata, open(os.path.join(directory_path, 'metadata.json'), 'w'), indent=4)
    count = con.execute('select count(zoom_level) from tiles;').fetchone()[0]
    done = 0
    msg = ''
    service_version = metadata.get('version', '1.0.0')
    base_path = os.path.join(directory_path,
                                service_version,
                                metadata.get('name', 'layer')
                            )
    if not os.path.isdir(base_path):
        os.makedirs(base_path)

    # if interactivity
    formatter = metadata.get('formatter')
    if formatter:
        layer_json = os.path.join(base_path,'layer.json')
        formatter_json = {"formatter":formatter}
        open(layer_json,'w').write('grid(' + json.dumps(formatter_json) + ')')

    tiles = con.execute('select zoom_level, tile_column, tile_row, tile_data from tiles;')
    t = tiles.fetchone()
    while t:
        z = t[0]
        x = t[1]
        y = t[2]
        if kwargs.get('scheme') == 'xyz':
          y = flip_y(z,y)
          print 'flipping'
        tile_dir = os.path.join(base_path, str(z), str(x))
        if not os.path.isdir(tile_dir):
            os.makedirs(tile_dir)
        tile = os.path.join(tile_dir,'%s.%s' % (y,metadata.get('format', 'png')))
        f = open(tile, 'wb')
        f.write(t[3])
        f.close()
        done = done + 1
        for c in msg: sys.stdout.write(chr(8))
        logger.info('%s / %s tiles exported' % (done, count))
        t = tiles.fetchone()

    # grids
    done = 0
    msg = ''
    try:
        count = con.execute('select count(zoom_level) from grids;').fetchone()[0]
        grids = con.execute('select zoom_level, tile_column, tile_row, grid from grids;')
        g = grids.fetchone()
    except sqlite3.OperationalError:
        g = None # no grids table
    while g:
        zoom_level = g[0] # z
        tile_column = g[1] # x
        y = g[2] # y
        grid_data_cursor = con.execute('''select key_name, key_json FROM
            grid_data WHERE
            zoom_level = %(zoom_level)d and
            tile_column = %(tile_column)d and
            tile_row = %(y)d;''' % locals() )
        if kwargs.get('scheme') == 'xyz':
          y = flip_y(zoom_level,y)
        grid_dir = os.path.join(base_path, str(zoom_level), str(tile_column))
        if not os.path.isdir(grid_dir):
            os.makedirs(grid_dir)
        grid = os.path.join(grid_dir,'%s.grid.json' % (y))
        f = open(grid, 'w')
        grid_json = json.loads(zlib.decompress(g[3]))
        # join up with the grid 'data' which is in pieces when stored in mbtiles file
        grid_data = grid_data_cursor.fetchone()
        data = {}
        while grid_data:
            data[grid_data[0]] = json.loads(grid_data[1])
            grid_data = grid_data_cursor.fetchone()
        grid_json['data'] = data
        f.write('grid(' + json.dumps(grid_json) + ')')
        f.close()
        done = done + 1
        for c in msg: sys.stdout.write(chr(8))
        logger.info('%s / %s grids exported' % (done, count))
        g = grids.fetchone()
