import sqlite3, uuid, sys, logging, time, os, json, zlib, hashlib, tempfile, math

logger = logging.getLogger(__name__)


def flip_y(zoom, y):
    return (2**int(zoom)-1) - int(y)


def coordinate_to_tile(longitude, latitude, zoom):
    latitude_rad = math.radians(latitude)
    n = 2.0 ** zoom
    tileX = int((longitude + 180.0) / 360.0 * n)
    tileY = int((1.0 - math.log(math.tan(latitude_rad) + (1 / math.cos(latitude_rad))) / math.pi) / 2.0 * n)
    return (tileX, tileY)


def tile_to_coordinate(tileX, tileY, zoom):
    n = 2.0 ** zoom
    longitude = (tileX + 0.5) / n * 360.0 - 180.0
    latitude_rad = math.atan(math.sinh(math.pi * (1 - 2 * (tileY + 0.5) / n)))
    latitude = math.degrees(latitude_rad)
    return (longitude, latitude)


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


def optimize_connection(cur, wal_journal=False, synchronous_off=False, exclusive_lock=True):
    cur.execute("PRAGMA cache_size = 40000")
    cur.execute("PRAGMA temp_store = memory")

    if wal_journal:
        cur.execute("PRAGMA journal_mode = WAL")
    else:
        try:
            cur.execute("PRAGMA journal_mode = DELETE")
        except sqlite3.OperationalError:
            pass

    if exclusive_lock:
        cur.execute("PRAGMA locking_mode = EXCLUSIVE")

    if synchronous_off:
        cur.execute("PRAGMA synchronous = OFF")


def compaction_prepare(cur):
    cur.execute("PRAGMA page_size = 4096")

    cur.execute("""
        CREATE TABLE IF NOT EXISTS images (
        tile_data BLOB,
        tile_id VARCHAR(256))""")
    cur.execute("""
        CREATE TABLE IF NOT EXISTS map (
        zoom_level INTEGER,
        tile_column INTEGER,
        tile_row INTEGER,
        tile_id VARCHAR(256),
        updated_at INTEGER)""")
    cur.execute("""
        CREATE TABLE IF NOT EXISTS metadata (
        name TEXT,
        value TEXT)""")
    cur.execute("""
        CREATE UNIQUE INDEX name ON metadata (name)""")


def compaction_finalize(cur):
    try:
        cur.execute("""DROP VIEW tiles""")
    except sqlite3.OperationalError:
        pass
    cur.execute("""
        CREATE VIEW tiles AS
        SELECT map.zoom_level AS zoom_level,
        map.tile_column AS tile_column,
        map.tile_row AS tile_row,
        images.tile_data AS tile_data,
        map.updated_at AS updated_at FROM
        map JOIN images ON images.tile_id = map.tile_id""")
    cur.execute("""
        CREATE UNIQUE INDEX IF NOT EXISTS map_index ON map
        (zoom_level, tile_column, tile_row, updated_at)""")
    cur.execute("""
          CREATE UNIQUE INDEX IF NOT EXISTS images_id ON images (tile_id)""")


def compaction_update(cur):
    try:
        cur.execute("""
            ALTER TABLE map ADD COLUMN
            updated_at INTEGER""")
        # Will only drop and recreate the index the first time
        cur.execute("""DROP INDEX map_index""")
    except sqlite3.OperationalError:
        pass

    try:
        compaction_finalize(cur)
    except sqlite3.OperationalError:
        pass


def mbtiles_setup(cur):
    compaction_prepare(cur)
    compaction_finalize(cur)


def optimize_database(cur, skip_analyze, skip_vacuum):
    if not skip_analyze:
        logger.info('analyzing db')
        cur.execute("""ANALYZE""")

    if not skip_vacuum:
        logger.info('cleaning db')
        cur.execute("""VACUUM""")


def optimize_database_file(mbtiles_file, skip_analyze, skip_vacuum, wal_journal=False):
    con = mbtiles_connect(mbtiles_file)
    cur = con.cursor()
    optimize_connection(cur, wal_journal)
    optimize_database(cur, skip_analyze, skip_vacuum)
    con.commit()
    con.close()


def mbtiles_create(mbtiles_file, **kwargs):
    logger.info("Creating empty database %s" % (mbtiles_file))
    con = mbtiles_connect(mbtiles_file)
    cur = con.cursor()
    optimize_connection(cur)
    mbtiles_setup(cur)
    con.commit()
    con.close()


def execute_commands_on_tile(command_list, image_format, tile_data, tmp_dir=None):
    if command_list == None or tile_data == None:
        return tile_data

    tmp_file_fd, tmp_file_name = tempfile.mkstemp(suffix=".%s" % (image_format), prefix="tile_", dir=tmp_dir)
    tmp_file = os.fdopen(tmp_file_fd, "w")
    tmp_file.write(tile_data)
    tmp_file.close()

    for command in command_list:
        # logger.debug("Executing command: %s" % command)
        os.system(command % (tmp_file_name))

    tmp_file = open(tmp_file_name, "r")
    new_tile_data = tmp_file.read()
    tmp_file.close()

    os.remove(tmp_file_name)

    return new_tile_data


def execute_commands_on_file(command_list, image_format, image_file_path):
    if command_list == None or image_file_path == None or not os.path.isfile(image_file_path):
        return False

    for command in command_list:
        # logger.debug("Executing command: %s" % command)
        os.system(command % (image_file_path))

    return True


def process_tile(next_tile):
    tile_id, tile_file_path, image_format, command_list = next_tile['tile_id'], next_tile['filename'], next_tile['format'], next_tile['command_list']
    # sys.stderr.write("%s (%s) -> %s\n" % (tile_id, image_format, tile_file_path))

    tile_data = execute_commands_on_file(command_list, image_format, tile_file_path)

    return next_tile
