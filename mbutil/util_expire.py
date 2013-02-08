import sys, logging, sqlite3, time

logger = logging.getLogger(__name__)

from util import mbtiles_connect, optimize_connection


def expire_mbtiles(mbtiles_file, **kwargs):
    logger.info("Expiring tiles from database %s" % (mbtiles_file))


    wal_journal = kwargs.get('wal_journal', False)
    synchronous_off = kwargs.get('synchronous_off', False)
    expire_days = kwargs.get('expire', 0)

    if expire_days == 0:
        return


    con = mbtiles_connect(mbtiles_file)
    cur = con.cursor()
    optimize_connection(cur, wal_journal, synchronous_off)


    expire_timestamp = (int(time.time()) - (int(expire_days) * 86400))

    cur.execute("""DELETE FROM map WHERE updated_at < ?""", (expire_timestamp, ))

    logger.debug("%d tiles removed" % (con.total_changes))
    

    con.commit()
    con.close()
