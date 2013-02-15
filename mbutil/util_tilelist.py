import sqlite3, uuid, sys, logging, time, os, json, zlib, hashlib, tempfile

from util import mbtiles_connect, optimize_connection, optimize_database
from util_convert import convert_tile_to_bbox

logger = logging.getLogger(__name__)


def mbtiles_tilelist(mbtiles_file, **kwargs):
    logger.info("Tile list for database %s" % (mbtiles_file))

    flip_y    = kwargs.get('flip_y', False)
    as_bboxes = kwargs.get('as_bboxes', False)

    zoom      = kwargs.get('zoom', -1)
    min_zoom  = kwargs.get('min_zoom', 0)
    max_zoom  = kwargs.get('max_zoom', 18)

    journal_mode = kwargs.get('journal_mode', 'wal')


    if zoom >= 0:
        min_zoom = max_zoom = zoom

    con = mbtiles_connect(mbtiles_file)
    cur = con.cursor()
    optimize_connection(cur, journal_mode)

    for current_zoom_level in range(min_zoom, max_zoom+1):
        logger.debug("Starting zoom level %d" % (current_zoom_level))

        tiles = cur.execute("""SELECT tile_column, tile_row FROM map WHERE zoom_level = ?""",
            [current_zoom_level])

        t = tiles.fetchone()
        while t:
            tile_column, tile_row = int(t[0]), int(t[1])

            if as_bboxes:
                convert_tile_to_bbox(current_zoom_level, tile_column, tile_row, flip_y)
            else:
                if flip_y:
                    tile_row = flip_y(current_zoom_level, tile_row)
                sys.stdout.write("%d/%d/%d\n" % (current_zoom_level, tile_column, tile_row))

            t = tiles.fetchone()


    con.close()
