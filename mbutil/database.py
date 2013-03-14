import psycopg2, sqlite3, uuid, sys, logging, time, os, json, zlib, hashlib, tempfile, math

logger = logging.getLogger(__name__)


def database_connect(connect_string, auto_commit=False, journal_mode='wal', synchronous_off=False, exclusive_lock=False, check_if_exists=False):
    """Connect to a database
    """

    if connect_string.endswith(".mbtiles"):
        return MBTilesSQLite(connect_string, auto_commit, journal_mode, synchronous_off, exclusive_lock, check_if_exists)
    elif connect_string.find("dbname") >= 0 or connect_string.startswith("pg:"):
        return MBTilesPostgres(connect_string, auto_commit, journal_mode, synchronous_off, exclusive_lock, check_if_exists)
    else:
        logger.error("Unknown database connection string")
        sys.exit(1)
  


class MBTilesDatabase:

    def __init__(self, connect_string, auto_commit=False, journal_mode='wal', synchronous_off=False, exclusive_lock=False, check_if_exists=False):
        self.connect_string = connect_string
        self.con = None
        self.cur = None
        self.database_is_compacted = None

    def close(self):
        self.con.commit()
        self.con.close()

    def mbtiles_setup(self):
        raise Exception("Not implemented.")

    def optimize_database(self, skip_analyze, skip_vacuum):
        if not skip_analyze:
            logger.info('analyzing db')
            self.cur.execute("""ANALYZE""")

        if not skip_vacuum:
            logger.info('cleaning db')
            self.cur.execute("""VACUUM""")

    def create_map_tile_index(self):
        raise Exception("Not implemented.")

    def drop_map_tile_index(self):
        raise Exception("Not implemented.")

    def execute(self, sql):
        self.cur.execute(sql)

    def is_compacted(self):
        return True

    def max_timestamp(self):
        raise Exception("Not implemented.")

    # Returns an array with numbers, e.g. [5, 6, 7]
    def zoom_levels(self):
        raise Exception("Not implemented.")

    def tiles_count(self, min_zoom, max_zoom, min_timestamp, max_timestamp):
        raise Exception("Not implemented.")

    # Yields [x, y]
    def columns_and_rows_for_zoom_level(self, zoom_level):
        raise Exception("Not implemented.")

    # Yields [x]
    def columns_for_zoom_level_and_row(self, zoom_level, row):
        raise Exception("Not implemented.")

    # Yields [z, x, y, data]
    def tiles(self, min_zoom, max_zoom, min_timestamp, max_timestamp):
        raise Exception("Not implemented.")

    # Yields [z, x, y, data, tile_id]
    def tiles_with_tile_id(self, min_zoom, max_zoom, min_timestamp, max_timestamp):
        raise Exception("Not implemented.")

    def delete_tiles(self, min_zoom, max_zoom, min_timestamp, max_timestamp):
        raise Exception("Not implemented.")

    # Returns minX, maxX, minY, maxY
    def bounding_box_for_zoom_level(self, zoom_level):
        raise Exception("Not implemented.")

    def delete_tile_with_id(self, tile_id):
        raise Exception("Not implemented.")

    def insert_tile_to_images(self, tile_id, tile_data):
        raise Exception("Not implemented.")

    # tile_list must be an array of (tile_id, tile_data)
    def insert_tiles_to_images(self, tile_list):
        raise Exception("Not implemented.")

    def insert_tile_to_map(self, zoom_level, tile_column, tile_row, tile_id, replace_existing=True):
        raise Exception("Not implemented.")

    # tile_list must be an array of (z, x, y, tile_id, timestamp)
    def insert_tiles_to_map(self, tile_list):
        raise Exception("Not implemented.")

    def update_tile(self, old_tile_id, new_tile_id, tile_data):
        raise Exception("Not implemented.")

    def metadata(self):
        raise Exception("Not implemented.")

    def update_metadata(self, key, value):
        raise Exception("Not implemented.")



class MBTilesSQLite(MBTilesDatabase):

    def __init__(self, connect_string, auto_commit=False, journal_mode='wal', synchronous_off=False, exclusive_lock=False, check_if_exists=False):
        self.connect_string = connect_string
        self.database_is_compacted = None

        if check_if_exists and not os.path.isfile(connect_string):
            sys.stderr.write('The mbtiles database must exist.\n')
            sys.exit(1)

        try:

            self.con = sqlite3.connect(connect_string)
            if auto_commit:
                self.con.isolation_level = None

            self.cur = self.con.cursor()

            self.cur.execute("PRAGMA cache_size = 100000")
            self.cur.execute("PRAGMA temp_store = memory")
            self.cur.execute("PRAGMA count_changes = OFF")
            self.cur.execute("PRAGMA synchronous = NORMAL")

            try:
                self.cur.execute("PRAGMA journal_mode = '%s'" % (journal_mode))
            except sqlite3.OperationalError:
                logger.error("Could not set journal_mode='%s'" % (journal_mode))
                pass

            if exclusive_lock:
                self.cur.execute("PRAGMA locking_mode = EXCLUSIVE")

            if synchronous_off:
                self.cur.execute("PRAGMA synchronous = OFF")

        except Exception, e:
            logger.error("Could not connect to the SQLite database:")
            logger.error(e)
            sys.exit(1)


    def mbtiles_setup(self):
        self.cur.execute("PRAGMA page_size = 4096")

        self.cur.execute("""
            CREATE TABLE IF NOT EXISTS images (
            tile_id VARCHAR(256),
            tile_data BLOB )""")
        self.cur.execute("""
            CREATE TABLE IF NOT EXISTS map (
            zoom_level INTEGER,
            tile_column INTEGER,
            tile_row INTEGER,
            tile_id VARCHAR(256),
            updated_at INTEGER )""")
        self.cur.execute("""
            CREATE TABLE IF NOT EXISTS metadata (
            name VARCHAR(256),
            value TEXT )""")
        self.cur.execute("""
            CREATE UNIQUE INDEX IF NOT EXISTS name ON metadata (name)""")

        try:
            self.cur.execute("""
                ALTER TABLE map ADD COLUMN
                updated_at INTEGER""")
        except sqlite3.OperationalError:
            pass

        try:
            self.cur.execute("""DROP VIEW tiles""")
        except sqlite3.OperationalError:
            pass

        self.cur.execute("""
            CREATE VIEW tiles AS
            SELECT map.zoom_level AS zoom_level,
            map.tile_column AS tile_column,
            map.tile_row AS tile_row,
            images.tile_data AS tile_data,
            map.updated_at AS updated_at
            FROM map
            JOIN images ON images.tile_id = map.tile_id""")
        self.cur.execute("""
            CREATE UNIQUE INDEX IF NOT EXISTS map_index ON map
            (zoom_level, tile_column, tile_row)""")
        self.cur.execute("""
            CREATE UNIQUE INDEX IF NOT EXISTS images_id ON images (tile_id)""")


    def create_map_tile_index(self):
        self.cur.execute("""CREATE INDEX IF NOT EXISTS map_tile_id_index ON map (tile_id)""")


    def drop_map_tile_index(self):
        self.cur.execute("""DROP INDEX map_tile_id_index""")


    def is_compacted(self):
        if self.database_is_compacted == None:
            self.database_is_compacted = (self.cur.execute("SELECT count(name) FROM sqlite_master WHERE type='table' AND (name='images' OR name='map')").fetchone()[0] == 2)
        return self.database_is_compacted


    def max_timestamp(self):
        try:
            return self.cur.execute("SELECT max(updated_at) FROM map").fetchone()[0]
        except:
            return 0


    def zoom_levels(self):
        return [int(x[0]) for x in self.cur.execute("SELECT distinct(zoom_level) FROM tiles").fetchall()]


    def tiles_count(self, min_zoom, max_zoom, min_timestamp, max_timestamp):
        total_tiles = 0

        if self.is_compacted():
            if min_timestamp > 0 and max_timestamp > 0:
                total_tiles = self.cur.execute("""SELECT count(zoom_level) FROM map WHERE zoom_level>=? AND zoom_level<=? AND updated_at>? AND updated_at<?""",
                    (min_zoom, max_zoom, min_timestamp, max_timestamp)).fetchone()[0]
            elif min_timestamp > 0:
                total_tiles = self.cur.execute("""SELECT count(zoom_level) FROM map WHERE zoom_level>=? AND zoom_level<=? AND updated_at>?""",
                    (min_zoom, max_zoom, min_timestamp)).fetchone()[0]
            elif max_timestamp > 0:
                total_tiles = self.cur.execute("""SELECT count(zoom_level) FROM map WHERE zoom_level>=? AND zoom_level<=? AND updated_at<?""",
                    (min_zoom, max_zoom, max_timestamp)).fetchone()[0]
            else:
                total_tiles = self.cur.execute("""SELECT count(zoom_level) FROM map WHERE zoom_level>=? AND zoom_level<=?""",
                    (min_zoom, max_zoom)).fetchone()[0]
        else:
            total_tiles = self.cur.execute("""SELECT count(zoom_level) FROM map WHERE zoom_level>=? AND zoom_level<=?""",
                (min_zoom, max_zoom)).fetchone()[0]

        return total_tiles


    def columns_and_rows_for_zoom_level(self, zoom_level):
        tiles_cur = self.con.cursor()

        tiles = tiles_cur.execute("""SELECT tile_column, tile_row FROM map WHERE zoom_level = ?""",
            [zoom_level])

        t = tiles.fetchone()
        while t:
            yield t
            t = tiles.fetchone()

        tiles_cur.close()


    def columns_for_zoom_level_and_row(self, zoom_level, row):
        return set([int(x[0]) for x in self.cur.execute("""SELECT tile_column FROM tiles WHERE zoom_level=? AND tile_row=?""",
            (zoom_level, row)).fetchall()])


    def tiles_with_tile_id(self, min_zoom, max_zoom, min_timestamp, max_timestamp):
        tiles_cur = self.con.cursor()

        chunk = 10000

        if min_timestamp > 0 and max_timestamp > 0:
            tiles_cur.execute("""SELECT map.zoom_level, map.tile_column, map.tile_row, images.tile_data, images.tile_id FROM map, images WHERE (map.zoom_level>=? and map.zoom_level<=? AND map.updated_at>? AND map.updated_at<?) AND (images.tile_id == map.tile_id)""",
                (min_zoom, max_zoom, min_timestamp, max_timestamp))
        elif min_timestamp > 0:
            tiles_cur.execute("""SELECT map.zoom_level, map.tile_column, map.tile_row, images.tile_data, images.tile_id FROM map, images WHERE (map.zoom_level>=? and map.zoom_level<=? AND map.updated_at>?) AND (images.tile_id == map.tile_id)""",
                (min_zoom, max_zoom, min_timestamp))
        elif max_timestamp > 0:
            tiles_cur.execute("""SELECT map.zoom_level, map.tile_column, map.tile_row, images.tile_data, images.tile_id FROM map, images WHERE (map.zoom_level>=? and map.zoom_level<=? AND map.updated_at<?) AND (images.tile_id == map.tile_id)""",
                (min_zoom, max_zoom, max_timestamp))
        else:
            tiles_cur.execute("""SELECT map.zoom_level, map.tile_column, map.tile_row, images.tile_data, images.tile_id FROM map, images WHERE (map.zoom_level>=? and map.zoom_level<=?) AND (images.tile_id == map.tile_id)""",
                (min_zoom, max_zoom))

        rows = tiles_cur.fetchmany(chunk)
        while rows:
            for t in rows:
                yield t
            rows = tiles_cur.fetchmany(chunk)

        tiles_cur.close()


    def tiles(self, min_zoom, max_zoom, min_timestamp, max_timestamp):
        tiles_cur = self.con.cursor()

        chunk = 10000
    
        if self.is_compacted():
            if min_timestamp > 0 and max_timestamp > 0:
                tiles_cur.execute("""SELECT zoom_level, tile_column, tile_row, tile_data FROM tiles WHERE zoom_level>=? AND zoom_level<=? AND updated_at>? AND updated_at<?""",
                    (min_zoom, max_zoom, min_timestamp, max_timestamp))
            elif min_timestamp > 0:
                tiles_cur.execute("""SELECT zoom_level, tile_column, tile_row, tile_data FROM tiles WHERE zoom_level>=? AND zoom_level<=? AND updated_at>?""",
                    (min_zoom, max_zoom, min_timestamp))
            elif max_timestamp > 0:
                tiles_cur.execute("""SELECT zoom_level, tile_column, tile_row, tile_data FROM tiles WHERE zoom_level>=? AND zoom_level<=? AND updated_at<?""",
                    (min_zoom, max_zoom, max_timestamp))
            else:
                tiles_cur.execute("""SELECT zoom_level, tile_column, tile_row, tile_data FROM tiles WHERE zoom_level>=? AND zoom_level<=?""",
                    (min_zoom, max_zoom))
        else:
            tiles_cur.execute("""SELECT zoom_level, tile_column, tile_row, tile_data FROM tiles WHERE zoom_level>=? AND zoom_level<=?""",
                (min_zoom, max_zoom))

        rows = tiles_cur.fetchmany(chunk)
        while rows:
            for t in rows:
                yield t
            rows = tiles_cur.fetchmany(chunk)

        tiles_cur.close()


    def delete_tiles(self, min_zoom, max_zoom, min_timestamp, max_timestamp):
        if self.is_compacted():
            if min_timestamp > 0 and max_timestamp > 0:
                self.cur.execute("""DELETE FROM images WHERE tile_id IN (SELECT tile_id FROM map WHERE zoom_level>=? AND zoom_level<=? AND updated_at>? AND updated_at<?)""",
                    (min_zoom, max_zoom, min_timestamp, max_timestamp))
                self.cur.execute("""DELETE FROM map WHERE zoom_level>=? AND zoom_level<=? AND updated_at>? AND updated_at<?""", (min_zoom, max_zoom, min_timestamp, max_timestamp))
            elif min_timestamp > 0:
                self.cur.execute("""DELETE FROM images WHERE tile_id IN (SELECT tile_id FROM map WHERE zoom_level>=? AND zoom_level<=? AND updated_at>?)""",
                    (min_zoom, max_zoom, min_timestamp))
                self.cur.execute("""DELETE FROM map WHERE zoom_level>=? AND zoom_level<=? AND updated_at>?""", (min_zoom, max_zoom, min_timestamp))
            elif max_timestamp > 0:
                self.cur.execute("""DELETE FROM images WHERE tile_id IN (SELECT tile_id FROM map WHERE zoom_level>=? AND zoom_level<=? AND updated_at<?)""",
                    (min_zoom, max_zoom, max_timestamp))
                self.cur.execute("""DELETE FROM map WHERE zoom_level>=? AND zoom_level<=? AND updated_at<?""", (min_zoom, max_zoom, max_timestamp))
            else:
                self.cur.execute("""DELETE FROM images WHERE tile_id IN (SELECT tile_id FROM map WHERE zoom_level>=? AND zoom_level<=?)""",
                    (min_zoom, max_zoom))
                self.cur.execute("""DELETE FROM map WHERE zoom_level>=? AND zoom_level<=?""", (min_zoom, max_zoom))
        else:
            self.cur.execute("""DELETE FROM tiles WHERE zoom_level>=? AND zoom_level<=?""", (min_zoom, max_zoom))


    def bounding_box_for_zoom_level(self, zoom_level):
        return self.cur.execute("""SELECT min(tile_column), max(tile_column), min(tile_row), max(tile_row) FROM tiles WHERE zoom_level = ?""",
            [zoom_level]).fetchone()


    def delete_tile_with_id(self, tile_id):
        self.cur.execute("""DELETE FROM map WHERE tile_id=?""", (tile_id, ));
        self.cur.execute("""DELETE FROM images WHERE tile_id=?""", (tile_id, ))


    def insert_tile_to_images(self, tile_id, tile_data):
        self.cur.execute("""INSERT OR IGNORE INTO images (tile_id, tile_data) VALUES (?, ?)""",
            (tile_id, sqlite3.Binary(tile_data)))


    def insert_tiles_to_images(self, tile_list):
        self.cur.executemany("""INSERT OR IGNORE INTO images (tile_id, tile_data) VALUES (?, ?)""", [(t[0], sqlite3.Binary(t[1])) for t in tile_list])


    def insert_tile_to_map(self, zoom_level, tile_column, tile_row, tile_id, replace_existing=True):
        if replace_existing:
            self.cur.execute("""REPLACE INTO map (zoom_level, tile_column, tile_row, tile_id, updated_at) VALUES (?, ?, ?, ?, ?)""",
                (zoom_level, tile_column, tile_row, tile_id, int(time.time())))
        else:
            self.cur.execute("""INSERT OR IGNORE INTO map (zoom_level, tile_column, tile_row, tile_id, updated_at) VALUES (?, ?, ?, ?, ?)""",
                (zoom_level, tile_column, tile_row, tile_id, int(time.time())))


    def insert_tiles_to_map(self, tile_list):
        self.cur.executemany("""REPLACE INTO map (zoom_level, tile_column, tile_row, tile_id, updated_at) VALUES (?, ?, ?, ?, ?)""", tile_list)


    def update_tile(self, old_tile_id, new_tile_id, tile_data):
        self.cur.execute("""INSERT OR IGNORE INTO images (tile_id, tile_data) VALUES (?, ?)""",
            (new_tile_id, sqlite3.Binary(tile_data)))
        self.cur.execute("""UPDATE map SET tile_id=?, updated_at=? WHERE tile_id=?""",
            (new_tile_id, int(time.time()), old_tile_id))

        if old_tile_id != new_tile_id:
            self.cur.execute("""DELETE FROM images WHERE tile_id=?""",
                [old_tile_id])


    def metadata(self):
        try:
            return dict(self.cur.execute('SELECT name, value FROM metadata').fetchall())
        except Exception, e:
            return None


    def update_metadata(self, key, value):
        self.cur.execute('REPLACE INTO metadata (name, value) VALUES (?, ?)',
            (key, value))



class MBTilesPostgres(MBTilesDatabase):

    def __init__(self, connect_string, auto_commit=False, journal_mode='wal', synchronous_off=False, exclusive_lock=False, check_if_exists=False):
        try:

            if connect_string.startswith("pg:"):
                if os.path.isfile("/etc/mb-util.conf"):
                    config = {}

                    with open("/etc/mb-util.conf") as f:
                        for line in f:
                            try:
                                for (key, value) in (line.strip().split(":", 1),):
                                    config[key.strip()] = value.strip()
                            except:
                                pass

                    key = connect_string.split(':')[1]
                    if len(key):
                        if config.get(key, None) != None:
                            connect_string = config.get(key, connect_string)
                        else:
                            logger.error("Could not find '%s' in /etc/mb-util.conf." % (key))
                            sys.exit(1)

            self.connect_string = connect_string
            self.con = psycopg2.connect(connect_string)

            # autocommit is always enabled in PostgreSQL since it makes lots of things easier
            # until Postgres has an upsert method
            # if auto_commit:
            self.con.autocommit = True

            self.cur = self.con.cursor()

            if check_if_exists:
                self.cur.execute("SELECT count(*) FROM pg_tables WHERE tablename = 'map'")
                if self.cur.fetchone()[0] == 0:
                    self.close()
                    sys.stderr.write('The mbtiles database and tables must exist.\n')
                    sys.exit(1)

        except Exception, e:
            logger.error("Could not connect to the PostgreSQL database '%s':" % (connect_string))
            logger.error(e)
            sys.exit(1)


    def mbtiles_setup(self):
        self.cur.execute("""
            CREATE TABLE IF NOT EXISTS images (
            tile_id VARCHAR(256),
            tile_data BYTEA )""")
        self.cur.execute("""
            CREATE TABLE IF NOT EXISTS map (
            zoom_level SMALLINT,
            tile_column INTEGER,
            tile_row INTEGER,
            tile_id VARCHAR(256),
            updated_at INTEGER )""")
        self.cur.execute("""
            CREATE TABLE IF NOT EXISTS metadata (
            name VARCHAR(256),
            value TEXT )""")

        self.cur.execute("SELECT count(*) FROM pg_class WHERE relname = 'metadata_name_index'")
        if self.cur.fetchone()[0] == 0:
            self.cur.execute("""CREATE UNIQUE INDEX metadata_name_index ON metadata (name)""")

        self.cur.execute("""
            CREATE OR REPLACE VIEW tiles AS
            SELECT map.zoom_level AS zoom_level,
            map.tile_column AS tile_column,
            map.tile_row AS tile_row,
            images.tile_data AS tile_data,
            map.updated_at AS updated_at
            FROM map
            JOIN images ON images.tile_id = map.tile_id""")

        self.cur.execute("SELECT count(*) FROM pg_class WHERE relname = 'map_coordinate_index'")
        if self.cur.fetchone()[0] == 0:
            self.cur.execute("""CREATE UNIQUE INDEX map_coordinate_index ON map (zoom_level, tile_column, tile_row)""")

        self.cur.execute("SELECT count(*) FROM pg_class WHERE relname = 'images_id_index'")
        if self.cur.fetchone()[0] == 0:
            self.cur.execute("""CREATE UNIQUE INDEX images_id_index ON images (tile_id)""")


    def create_map_tile_index(self):
        self.cur.execute("SELECT count(*) FROM pg_class WHERE relname = 'map_tile_id_index'")
        if self.cur.fetchone()[0] == 0:
            self.cur.execute("""CREATE INDEX map_tile_id_index ON map (tile_id)""")


    def drop_map_tile_index(self):
        self.cur.execute("""DROP INDEX map_tile_id_index""")


    def max_timestamp(self):
        self.cur.execute("SELECT max(updated_at) FROM map")
        result = self.cur.fetchone()

        if not result:
            return 0

        return result[0]


    def zoom_levels(self):
        self.cur.execute("SELECT distinct(zoom_level) FROM tiles")
        return [int(x[0]) for x in self.cur.fetchall()]


    def tiles_count(self, min_zoom, max_zoom, min_timestamp, max_timestamp):
        total_tiles = 0

        if min_timestamp > 0 and max_timestamp > 0:
            self.cur.execute("""SELECT count(zoom_level) FROM map WHERE zoom_level>=%s AND zoom_level<=%s AND updated_at>%s AND updated_at<%s""",
                (min_zoom, max_zoom, min_timestamp, max_timestamp))
        elif min_timestamp > 0:
            self.cur.execute("""SELECT count(zoom_level) FROM map WHERE zoom_level>=%s AND zoom_level<=%s AND updated_at>%s""",
                (min_zoom, max_zoom, min_timestamp))
        elif max_timestamp > 0:
            self.cur.execute("""SELECT count(zoom_level) FROM map WHERE zoom_level>=%s AND zoom_level<=%s AND updated_at<%s""",
                (min_zoom, max_zoom, max_timestamp))
        else:
            self.cur.execute("""SELECT count(zoom_level) FROM map WHERE zoom_level>=%s AND zoom_level<=%s""",
                (min_zoom, max_zoom))

        return self.cur.fetchone()[0]


    def columns_and_rows_for_zoom_level(self, zoom_level):
        tiles_cur = self.con.cursor()

        tiles_cur.execute("""SELECT tile_column, tile_row FROM map WHERE zoom_level = %s ORDER BY tile_column, tile_row""",
            [zoom_level])

        t = tiles_cur.fetchone()
        while t:
            yield t
            t = tiles_cur.fetchone()

        tiles_cur.close()


    def columns_for_zoom_level_and_row(self, zoom_level, row):
        self.cur.execute("""SELECT tile_column FROM tiles WHERE zoom_level=%s AND tile_row=%s""",
            (zoom_level, row))
        return set([int(x[0]) for x in self.cur.fetchall()])


    def tiles_with_tile_id(self, min_zoom, max_zoom, min_timestamp, max_timestamp):
        # Second connection to the database, for the named cursor
        iter_con = psycopg2.connect(self.connect_string)

        tiles_cur = iter_con.cursor("tiles_with_tile_id_cursor")

        if min_timestamp > 0 and max_timestamp > 0:
            tiles_cur.execute("""SELECT map.zoom_level, map.tile_column, map.tile_row, images.tile_data, images.tile_id FROM map, images WHERE (map.zoom_level>=%s and map.zoom_level<=%s AND map.updated_at>%s AND map.updated_at<%s) AND (images.tile_id = map.tile_id)""",
                (min_zoom, max_zoom, min_timestamp, max_timestamp))
        elif min_timestamp > 0:
            tiles_cur.execute("""SELECT map.zoom_level, map.tile_column, map.tile_row, images.tile_data, images.tile_id FROM map, images WHERE (map.zoom_level>=%s and map.zoom_level<=%s AND map.updated_at>%s) AND (images.tile_id = map.tile_id)""",
                (min_zoom, max_zoom, min_timestamp))
        elif max_timestamp > 0:
            tiles_cur.execute("""SELECT map.zoom_level, map.tile_column, map.tile_row, images.tile_data, images.tile_id FROM map, images WHERE (map.zoom_level>=%s and map.zoom_level<=%s AND map.updated_at<%s) AND (images.tile_id = map.tile_id)""",
                (min_zoom, max_zoom, max_timestamp))
        else:
            tiles_cur.execute("""SELECT map.zoom_level, map.tile_column, map.tile_row, images.tile_data, images.tile_id FROM map, images WHERE (map.zoom_level>=%s and map.zoom_level<=%s) AND (images.tile_id = map.tile_id)""",
                (min_zoom, max_zoom))

        t = tiles_cur.fetchone()
        while t:
            yield t
            t = tiles_cur.fetchone()

        tiles_cur.close()
        iter_con.close()


    def tiles(self, min_zoom, max_zoom, min_timestamp, max_timestamp):
        # Second connection to the database, for the named cursor
        iter_con = psycopg2.connect(self.connect_string)

        tiles_cur = iter_con.cursor("tiles_cursor")
    
        if min_timestamp > 0 and max_timestamp > 0:
            tiles_cur.execute("""SELECT zoom_level, tile_column, tile_row, tile_data FROM tiles WHERE zoom_level>=%s AND zoom_level<=%s AND updated_at>%s AND updated_at<%s OFFSET %s LIMIT %s""",
                (min_zoom, max_zoom, min_timestamp, max_timestamp, offset, chunk))
        elif min_timestamp > 0:
            tiles_cur.execute("""SELECT zoom_level, tile_column, tile_row, tile_data FROM tiles WHERE zoom_level>=%s AND zoom_level<=%s AND updated_at>%s OFFSET %s LIMIT %s""",
                (min_zoom, max_zoom, min_timestamp, offset, chunk))
        elif max_timestamp > 0:
            tiles_cur.execute("""SELECT zoom_level, tile_column, tile_row, tile_data FROM tiles WHERE zoom_level>=%s AND zoom_level<=%s AND updated_at<%s OFFSET %s LIMIT %s""",
                (min_zoom, max_zoom, max_timestamp, offset, chunk))
        else:
            tiles_cur.execute("""SELECT zoom_level, tile_column, tile_row, tile_data FROM tiles WHERE zoom_level>=%s AND zoom_level<=%s OFFSET %s LIMIT %s""",
                (min_zoom, max_zoom, offset, chunk))

        t = tiles_cur.fetchone()
        while t:
            yield t
            t = tiles_cur.fetchone()

        tiles_cur.close()
        iter_con.close()


    def delete_tiles(self, min_zoom, max_zoom, min_timestamp, max_timestamp):
        if min_timestamp > 0 and max_timestamp > 0:
            self.cur.execute("""DELETE FROM images WHERE tile_id IN (SELECT tile_id FROM map WHERE zoom_level>=%s AND zoom_level<=%s AND updated_at>%s AND updated_at<%s)""",
                (min_zoom, max_zoom, min_timestamp, max_timestamp))
            self.cur.execute("""DELETE FROM map WHERE zoom_level>=%s AND zoom_level<=%s AND updated_at>%s AND updated_at<%s""", (min_zoom, max_zoom, min_timestamp, max_timestamp))
        elif min_timestamp > 0:
            self.cur.execute("""DELETE FROM images WHERE tile_id IN (SELECT tile_id FROM map WHERE zoom_level>=%s AND zoom_level<=%s AND updated_at>%s)""",
                (min_zoom, max_zoom, min_timestamp))
            self.cur.execute("""DELETE FROM map WHERE zoom_level>=%s AND zoom_level<=%s AND updated_at>%s""", (min_zoom, max_zoom, min_timestamp))
        elif max_timestamp > 0:
            self.cur.execute("""DELETE FROM images WHERE tile_id IN (SELECT tile_id FROM map WHERE zoom_level>=%s AND zoom_level<=%s AND updated_at<%s)""",
                (min_zoom, max_zoom, max_timestamp))
            self.cur.execute("""DELETE FROM map WHERE zoom_level>=%s AND zoom_level<=%s AND updated_at<%s""", (min_zoom, max_zoom, max_timestamp))
        else:
            self.cur.execute("""DELETE FROM images WHERE tile_id IN (SELECT tile_id FROM map WHERE zoom_level>=%s AND zoom_level<=%s)""",
                (min_zoom, max_zoom))
            self.cur.execute("""DELETE FROM map WHERE zoom_level>=%s AND zoom_level<=%s""", (min_zoom, max_zoom))


    def bounding_box_for_zoom_level(self, zoom_level):
        self.cur.execute("""SELECT min(tile_column), max(tile_column), min(tile_row), max(tile_row) FROM tiles WHERE zoom_level = %s""",
            [zoom_level])
        return self.cur.fetchone()


    def delete_tile_with_id(self, tile_id):
        self.cur.execute("""DELETE FROM map WHERE tile_id=%s""", (tile_id,));
        self.cur.execute("""DELETE FROM images WHERE tile_id=%s""", (tile_id,))


    def insert_tile_to_images(self, tile_id, tile_data):
        try:
            self.cur.execute("""INSERT INTO images (tile_id, tile_data) VALUES (%s, %s)""",
                (tile_id, psycopg2.Binary(tile_data)))
        except:
            pass


    def insert_tiles_to_images(self, tile_list):
        for t in tile_list:
            self.insert_tile_to_images(t[0], t[1])


    def insert_tile_to_map(self, zoom_level, tile_column, tile_row, tile_id, replace_existing=True):
        try:
            self.cur.execute("""INSERT INTO map (zoom_level, tile_column, tile_row, tile_id, updated_at) VALUES (%s, %s, %s, %s, %s)""",
                (zoom_level, tile_column, tile_row, tile_id, int(time.time())))
        except:
            if replace_existing:
                self.cur.execute("""UPDATE map SET tile_id=%s, updated_at=%s WHERE zoom_level=%s AND tile_column=%s AND tile_row=%s""",
                    (tile_id, int(time.time()), zoom_level, tile_column, tile_row))


    def insert_tiles_to_map(self, tile_list):
        for t in tile_list:
            self.insert_tile_to_map(t[0], t[1], t[2], t[3])


    def update_tile(self, old_tile_id, new_tile_id, tile_data):
        try:
            self.cur.execute("""INSERT INTO images (tile_id, tile_data) VALUES(%s, %s)""",
                (new_tile_id, sqlite3.Binary(tile_data), new_tile_id))
        except:
            pass

        self.cur.execute("""UPDATE map SET tile_id=%s, updated_at=%s WHERE tile_id=%s""",
            (new_tile_id, int(time.time()), old_tile_id))

        if old_tile_id != new_tile_id:
            self.cur.execute("""DELETE FROM images WHERE tile_id=%s""",
                [old_tile_id])


    def metadata(self):
        try:
            self.cur.execute('SELECT name, value FROM metadata')
            return dict(self.cur.fetchall())
        except Exception, e:
            return None


    def update_metadata(self, key, value):
        try:
            self.cur.execute("""INSERT INTO metadata (name, value) VALUES (%s, %s)""",
                (key, value))
        except:
            self.cur.execute("""UPDATE metadata SET value=%s WHERE name=%s""",
                (value, key))

