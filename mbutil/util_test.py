import sqlite3, uuid, sys, logging, time, os, json, zlib, hashlib, tempfile, multiprocessing

from util import mbtiles_connect, optimize_connection, optimize_database
from multiprocessing import Pool

logger = logging.getLogger(__name__)


def test_tile(next_tile):
    tile_file_path, command_list, revert_test = next_tile['filename'], next_tile['command_list'], next_tile['revert_test']

    if command_list == None or tile_file_path == None or not os.path.isfile(tile_file_path):
        return next_tile

    command = command_list[0]

    # logger.debug("Executing command: %s" % command)
    result = os.system(command % (tile_file_path))

    if (revert_test == False and result != 0) or (revert_test == True and result == 0):
        sys.stderr.write("/%s/%s/%s.%s\n" % (next_tile['tile_z'], next_tile['tile_x'], next_tile['tile_y'], next_tile['format']))

    return next_tile


def test_mbtiles(mbtiles_file, **kwargs):
    logger.info("Testing database %s" % (mbtiles_file))


    zoom     = kwargs.get('zoom', -1)
    min_zoom = kwargs.get('min_zoom', 0)
    max_zoom = kwargs.get('max_zoom', 18)
    tmp_dir  = kwargs.get('tmp_dir', None)
    revert_test = kwargs.get('revert_test', False)
    default_pool_size = kwargs.get('poolsize', -1)

    if zoom >= 0:
        min_zoom = max_zoom = zoom

    con = mbtiles_connect(mbtiles_file)
    cur = con.cursor()
    optimize_connection(cur)

    zoom_levels = [int(x[0]) for x in cur.execute("SELECT distinct(zoom_level) FROM tiles").fetchall()]
    max_rowid   = (con.execute("select max(rowid) from map").fetchone()[0])

    chunk = 1000
    image_format = 'png'
    try:
        image_format = con.execute("select value from metadata where name='format';").fetchone()[0]
    except:
        pass


    if default_pool_size < 1:
        default_pool_size = None
        logger.debug("Using default pool size")
    else:
        logger.debug("Using pool size = %d" % (default_pool_size))

    pool = Pool(default_pool_size)
    multiprocessing.log_to_stderr(logger.level)


    for current_zoom_level in zoom_levels:
        if current_zoom_level < min_zoom or current_zoom_level > max_zoom:
            continue

        for i in range((max_rowid / chunk) + 1):
            # logger.debug("Starting range %d-%d" % (i*chunk, (i+1)*chunk))
            tiles = cur.execute("""select images.tile_id, images.tile_data, map.zoom_level, map.tile_column, map.tile_row
                from map, images
                where (map.rowid > ? and map.rowid <= ?)
                and (map.zoom_level=?)
                and (images.tile_id == map.tile_id)""",
                ((i * chunk), ((i + 1) * chunk), current_zoom_level))

            tiles_to_process = []

            t = tiles.fetchone()

            while t:
                tile_data = t[1]
                tile_z = t[2]
                tile_x = t[3]
                tile_y = t[4]

                tmp_file_fd, tmp_file_name = tempfile.mkstemp(suffix=".%s" % (image_format), prefix="tile_", dir=tmp_dir)
                tmp_file = os.fdopen(tmp_file_fd, "w")
                tmp_file.write(tile_data)
                tmp_file.close()

                tiles_to_process.append({
                    'tile_x' : tile_x,
                    'tile_y' : tile_y,
                    'tile_z' : tile_z,
                    'filename' : tmp_file_name,
                    'format' : image_format,
                    'revert_test' : revert_test,
                    'command_list' : kwargs.get('command_list', [])
                })

                t = tiles.fetchone()


            if len(tiles_to_process) == 0:
                continue

            # Execute commands in parallel
            # logger.debug("Starting multiprocessing...")
            processed_tiles = pool.map(test_tile, tiles_to_process)

            for next_tile in processed_tiles:
                tile_file_path = next_tile['filename']
                os.remove(tile_file_path)


    pool.close()
    con.close()
