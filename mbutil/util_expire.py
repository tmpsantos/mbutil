import sys, logging, sqlite3, time

logger = logging.getLogger(__name__)

from util import mbtiles_connect, prettify_connect_string


def expire_mbtiles(mbtiles_file, **kwargs):
    logger.info("Expiring tiles from %s" % (prettify_connect_string(mbtiles_file)))

    zoom        = kwargs.get('zoom', -1)
    min_zoom    = kwargs.get('min_zoom', 0)
    max_zoom    = kwargs.get('max_zoom', 18)

    auto_commit     = kwargs.get('auto_commit', False)
    journal_mode    = kwargs.get('journal_mode', 'wal')
    synchronous_off = kwargs.get('synchronous_off', False)
    expire_days     = kwargs.get('expire', 0)

    if expire_days == 0:
        return

    if zoom >= 0:
        min_zoom = max_zoom = zoom
    elif min_zoom == max_zoom:
        zoom = min_zoom


    con = mbtiles_connect(mbtiles_file, auto_commit, journal_mode, synchronous_off, False, True)

    expire_timestamp = (int(time.time()) - (int(expire_days) * 86400))

    logger.debug("Expiring tiles older than %s" % (time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(expire_timestamp))))

    con.delete_tiles(min_zoom, max_zoom, 0, expire_timestamp)

    con.optimize_database(kwargs.get('skip_analyze', False), kwargs.get('skip_vacuum', False))

    con.close()
