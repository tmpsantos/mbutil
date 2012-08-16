import sqlite3, uuid, sys, logging, time, os, json, zlib, hashlib, tempfile

from util import mbtiles_connect, optimize_connection, optimize_database, execute_commands_on_tile

logger = logging.getLogger(__name__)


def check_mbtiles(mbtiles_file, **kwargs):
    logger.info("Checking database %s" % (mbtiles_file))

    result = True


    zoom     = kwargs.get('zoom', -1)
    min_zoom = kwargs.get('min_zoom', 0)
    max_zoom = kwargs.get('max_zoom', 18)

    if zoom >= 0:
        min_zoom = max_zoom = zoom

    con = mbtiles_connect(mbtiles_file)
    cur = con.cursor()
    optimize_connection(cur)

    logger.debug("Loading zoom levels")

    zoom_levels = [int(x[0]) for x in cur.execute("SELECT distinct(zoom_level) FROM tiles").fetchall()]
    missing_tiles = []

    for current_zoom_level in zoom_levels:
        if current_zoom_level < min_zoom or current_zoom_level > max_zoom:
            continue

        logger.debug("Starting zoom level %d" % (current_zoom_level))

        t = cur.execute("""SELECT min(tile_column), max(tile_column), min(tile_row), max(tile_row) FROM tiles WHERE zoom_level = ?""",
            [current_zoom_level]).fetchone()

        minX, maxX, minY, maxY = t[0], t[1], t[2], t[3]

        logger.debug(" - Checking zoom level %d, x: %d - %d, y: %d - %d" % (current_zoom_level, minX, maxX, minY, maxY))

        for current_row in range(minY, maxY+1):
            logger.debug("   - Row: %d (%.1f%%)" %
                (current_row, (float(current_row - minY) / float(maxY - minY)) * 100.0) if minY != maxY else 100.0)

            mbtiles_columns = set([int(x[0]) for x in cur.execute("""SELECT tile_column FROM tiles WHERE zoom_level=? AND tile_row=?""",
                (current_zoom_level, current_row)).fetchall()])

            for current_column in range(minX, maxX+1):
                if current_column not in mbtiles_columns:
                    missing_tiles.append([current_zoom_level, current_column, current_row])


    if len(missing_tiles) > 0:
        result = False
        logger.error("(zoom, x, y)")
        for current_tile in missing_tiles:
            logger.error(current_tile)


    return result
