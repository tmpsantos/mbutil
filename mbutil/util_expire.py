import sys, logging, sqlite3, time

logger = logging.getLogger(__name__)

from util import mbtiles_connect, optimize_connection


def expire_mbtiles(mbtiles_file, **kwargs):
    logger.info("Expiring tiles from database %s" % (mbtiles_file))


    journal_mode    = kwargs.get('journal_mode', 'wal')
    synchronous_off = kwargs.get('synchronous_off', False)
    expire_days     = kwargs.get('expire', 0)

    if expire_days == 0:
        return


    con = mbtiles_connect(mbtiles_file)
    cur = con.cursor()
    optimize_connection(cur, journal_mode, synchronous_off)


    expire_timestamp = (int(time.time()) - (int(expire_days) * 86400))

    logger.debug("Expiring tiles older than %s" % (time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(expire_timestamp))))

    cur.execute("""DELETE FROM map WHERE updated_at < ?""", (expire_timestamp, ))
    

    con.commit()
    con.close()
