import sqlite3, uuid, sys, logging, time, os, json, zlib, hashlib, tempfile

from util import mbtiles_connect, execute_commands_on_tile, prettify_connect_string

logger = logging.getLogger(__name__)


def check_mbtiles(mbtiles_file, **kwargs):

    result = True

    zoom     = kwargs.get('zoom', -1)
    min_zoom = kwargs.get('min_zoom', 0)
    max_zoom = kwargs.get('max_zoom', 18)

    auto_commit     = kwargs.get('auto_commit', False)
    journal_mode    = kwargs.get('journal_mode', 'wal')
    synchronous_off = kwargs.get('synchronous_off', False)

    if zoom >= 0:
        min_zoom = max_zoom = zoom


    con = mbtiles_connect(mbtiles_file, auto_commit, journal_mode, synchronous_off, False, True)


    zoom_level_string = None

    if min_zoom == max_zoom:
        zoom_level_string = "zoom level %d" % (min_zoom)
    else:
        zoom_level_string = "zoom levels %d -> %d" % (min_zoom, max_zoom)

    logger.info("Checking %s (%s)" % (prettify_connect_string(con.connect_string), zoom_level_string))


    logger.debug("Loading zoom levels")

    zoom_levels = con.zoom_levels()
    missing_tiles = []

    for current_zoom_level in zoom_levels:
        if current_zoom_level < min_zoom or current_zoom_level > max_zoom:
            continue

        logger.debug("Starting zoom level %d" % (current_zoom_level))

        t = con.bounding_box_for_zoom_level(current_zoom_level)

        minX, maxX, minY, maxY = t[0], t[1], t[2], t[3]

        logger.debug(" - Checking zoom level %d, x: %d - %d, y: %d - %d" % (current_zoom_level, minX, maxX, minY, maxY))

        for current_row in range(minY, maxY+1):
            logger.debug("   - Row: %d (%.1f%%)" %
                (current_row, (float(current_row - minY) / float(maxY - minY)) * 100.0) if minY != maxY else 100.0)

            mbtiles_columns = con.columns_for_zoom_level_and_row(current_zoom_level, current_row)

            for current_column in range(minX, maxX+1):
                if current_column not in mbtiles_columns:
                    missing_tiles.append([current_zoom_level, current_column, current_row])


    if len(missing_tiles) > 0:
        result = False
        logger.error("(zoom, x, y)")
        for current_tile in missing_tiles:
            logger.error(current_tile)


    con.close()

    return result
