import sqlite3, uuid, sys, logging, time, os, json, zlib, hashlib, tempfile

from util import mbtiles_connect, execute_commands_on_tile, flip_y, prettify_connect_string

logger = logging.getLogger(__name__)


def disk_to_mbtiles(directory_path, mbtiles_file, **kwargs):

    auto_commit     = kwargs.get('auto_commit', False)
    journal_mode    = kwargs.get('journal_mode', 'wal')
    synchronous_off = kwargs.get('synchronous_off', False)

    print_progress  = kwargs.get('progress', False)

    zoom     = kwargs.get('zoom', -1)
    min_zoom = kwargs.get('min_zoom', 0)
    max_zoom = kwargs.get('max_zoom', 18)
    tmp_dir  = kwargs.get('tmp_dir', None)

    if tmp_dir and not os.path.isdir(tmp_dir):
        os.mkdir(tmp_dir)

    if zoom >= 0:
        min_zoom = max_zoom = zoom


    zoom_level_string = None

    if min_zoom == max_zoom:
        zoom_level_string = "zoom level %d" % (min_zoom)
    else:
        zoom_level_string = "zoom levels %d -> %d" % (min_zoom, max_zoom)

    logger.info("Importing path:'%s' --> %s (%s)" % (directory_path, prettify_connect_string(mbtiles_file), zoom_level_string))


    con = mbtiles_connect(mbtiles_file, auto_commit, journal_mode, synchronous_off, False)

    con.mbtiles_setup()

    if not con.is_compacted():
        con.close()
        logger.info("The mbtiles database must be compacted, exiting...")
        return

    con.mbtiles_setup()


    image_format = 'png'
    try:

        metadata = json.load(open(os.path.join(directory_path, 'metadata.json'), 'r'))
        image_format = metadata.get('format', 'png')

        # Check that the old and new image formats are the same
        receiving_metadata = con.metadata()

        if receiving_metadata != None:
            original_format = receiving_metadata.get('format')

            if original_format != None and image_format != original_format:
                sys.stderr.write('The databases to merge must use the same image format (png or jpg)\n')
                sys.exit(1)
        else:
            for name, value in metadata.items():
                con.update_metadata(name, value)
            logger.info('metadata from metadata.json restored')

    except IOError:
        logger.warning('metadata.json not found')


    count = 0
    start_time = time.time()

    if print_progress:
        sys.stdout.write("0 tiles imported (0 tiles/sec)")
        sys.stdout.flush()


    for r1, zs, ignore in os.walk(os.path.join(directory_path, "tiles")):
        for z in zs:
            if int(z) < min_zoom or int(z) > max_zoom:
                continue

            for r2, xs, ignore in os.walk(os.path.join(r1, z)):
                for x in xs:
                    for r2, ignore, ys in os.walk(os.path.join(r1, z, x)):
                        for y in ys:
                            y, extension = y.split('.')

                            f = open(os.path.join(r1, z, x, y) + '.' + extension, 'rb')
                            tile_data = f.read()
                            f.close()

                            if kwargs.get('flip_y', False) == True:
                                y = str(flip_y(z, y))

                            # Execute commands
                            if kwargs.get('command_list'):
                                tile_data = execute_commands_on_tile(kwargs['command_list'], image_format, tile_data, tmp_dir)

                            m = hashlib.md5()
                            m.update(tile_data)
                            tile_id = m.hexdigest()

                            con.insert_tile_to_images(tile_id, tile_data)
                            con.insert_tile_to_map(z, x, y, tile_id)

                            count = count + 1
                            if (count % 100) == 0:
                                logger.debug("%d tiles imported (%.1f tiles/sec)" % (count, count / (time.time() - start_time)))
                                if print_progress:
                                    sys.stdout.write("\r%d tiles imported (%.1f tiles/sec)" % (count, count / (time.time() - start_time)))
                                    sys.stdout.flush()


    if print_progress:
        sys.stdout.write('\n')

    logger.info("%d tiles imported." % (count))
    if print_progress:
        sys.stdout.write("%d tiles imported.\n" % (count))
        sys.stdout.flush()


    con.optimize_database(kwargs.get('skip_analyze', False), kwargs.get('skip_vacuum', False))

    con.close()
