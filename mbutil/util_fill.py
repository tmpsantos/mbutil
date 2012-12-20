import sqlite3, uuid, sys, logging, time, os, json, zlib, hashlib, tempfile

from util import mbtiles_connect, optimize_connection, optimize_database, coordinate_to_tile

logger = logging.getLogger(__name__)


def fill_mbtiles(mbtiles_file, image_filename, **kwargs):
    logger.info("Filling database %s" % (mbtiles_file))


    zoom      = kwargs.get('zoom', -1)
    min_zoom  = kwargs.get('min_zoom', 0)
    max_zoom  = kwargs.get('max_zoom', 18)
    flip_y    = kwargs.get('flip_y', False)
    bbox      = kwargs.get('bbox', None)
    tile_bbox = kwargs.get('tile_bbox', None)

    if zoom >= 0:
        min_zoom = max_zoom = zoom
    elif min_zoom == max_zoom:
        zoom = min_zoom

    if tile_bbox != None and zoom == 0:
        logger.info("--tile-bbox can only be used with --zoom, exiting...")
        return

    if tile_bbox == None and bbox == None:
        logger.info("Either --tile-bbox or --box must be given, exiting...")
        return

    con = mbtiles_connect(mbtiles_file)
    cur = con.cursor()
    optimize_connection(cur)


    existing_mbtiles_is_compacted = (con.execute("select count(name) from sqlite_master where type='table' AND name='images';").fetchone()[0] > 0)
    if not existing_mbtiles_is_compacted:
        logger.info("The mbtiles file must be compacted, exiting...")
        return

    if existing_mbtiles_is_compacted:
        compaction_update(cur)

    # Insert an image
    tmp_file = open(image_filename, "r")
    tile_data = tmp_file.read()
    tmp_file.close()

    m = hashlib.md5()
    m.update(tile_data)
    tile_id = m.hexdigest()

    cur.execute("""INSERT OR IGNORE INTO images (tile_id, tile_data) VALUES (?, ?)""",
        (tile_id, sqlite3.Binary(tile_data)))


    count = 0
    start_time = time.time()


    for z in range(min_zoom, max_zoom+1):
        min_x = min_y = max_x = max_y = 0

        if tile_bbox != None:
            match = re.match(r'(\d+),(\d+),(\d+),(\d+)', tile_bbox, re.I)
            if match:
                min_x, min_y, max_x, max_y = int(match.group(1)), int(match.group(2)), int(match.group(3)), int(match.group(4))
        elif bbox != None:
            match = re.match(r'([-0-9\.]+),([-0-9\.]+),([-0-9\.]+),([-0-9\.]+)', conversion_string, re.I)
            if match:
                left, bottom, right, top = float(match.group(1)), float(match.group(2)), float(match.group(3)), float(match.group(4))
                min_x, min_y = coordinate_to_tile(left, bottom, zoom)
                max_x, max_y = coordinate_to_tile(right, top, zoom)

        if min_y > max_y:
            min_y, max_y = max_y, min_y

        for x in range(min_x, max_x+1):
            for y in range(min_y, max_y+1):
                if flip_tile_y:
                    y = flip_y(zoom, y)

                # z, x, y
                cur.execute("""INSERT OR IGNORE INTO map (zoom_level, tile_column, tile_row, tile_id, updated_at) VALUES (?, ?, ?, ?, ?)""",
                    (z, x, y, tile_id, int(time.time())))

                count = count + 1
                if (count % 100) == 0:
                    logger.debug("%d tiles inserted (%.1f tiles/sec)" %
                        (count, count / (time.time() - start_time)))
                    if print_progress:
                        sys.stdout.write("\r%d tiles inserted (%.1f tiles/sec)" %
                            (count, count / (time.time() - start_time)))
                        sys.stdout.flush()


    if print_progress:
        sys.stdout.write('\n')

    logger.info("%d tiles inserted (100.0%% @ %.1f tiles/sec)" %
        (count, count / (time.time() - start_time)))
    if print_progress:
        sys.stdout.write("%d tiles inserted (100.0%% @ %.1f tiles/sec)\n" %
            (count, count / (time.time() - start_time)))
        sys.stdout.flush()

    con.commit()
    con.close()
