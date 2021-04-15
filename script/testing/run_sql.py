import argparse
import psycopg2

from .util.common import LOG

if __name__ == "__main__":
    aparser = argparse.ArgumentParser(description='Run a SQL command.')
    aparser.add_argument('sql', type=str, help='The SQL to be run.')
    aparser.add_argument('--host', type=str, default='localhost', help='The hostname of the DBMS.')
    aparser.add_argument('--port', type=int, default=15721, help='The port of the DBMS.')
    aparser.add_argument('--user', type=str, default='noisepage', help='The username for connecting to the DBMS.')
    aparser.add_argument("--autocommit", type=bool, default=True, help="True if the connection should autocommit.")
    aparser.add_argument("--print_results", type=bool, default=False,
                         help="True if there are results which should be printed.")
    aparser.add_argument('--quiet', type=bool, default=False, help='True if the SQL should be quietly executed.')
    args = vars(aparser.parse_args())

    try:
        conn = psycopg2.connect(host=args['host'], port=args['port'], user=args['user'])
        try:
            conn.set_session(autocommit=args['autocommit'])
            with conn.cursor() as cursor:
                if not args['quiet']:
                    LOG.info(f"Executing SQL on [host={args['host']},port={args['port']},user={args['user']}]: {args['sql']}")
                cursor.execute(args['sql'])
                if args['print_results']:
                    LOG.info(cursor.fetchall())
        finally:
            conn.close()
    except Exception as e:
        LOG.error(f"Executing SQL failed: {args['sql']}")
        raise e
