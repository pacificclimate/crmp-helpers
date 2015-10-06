import sys
import logging
import csv

from argparse import ArgumentParser
from pkg_resources import resource_filename

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from pycds import History, Station, Network

logging.basicConfig(level=logging.DEBUG, stream=sys.stdout)
log = logging.getLogger(__name__)

def main(args):

    Session = sessionmaker(create_engine(args.connection_string))
    sesh = Session()

    sesh.begin_nested()

    records = 0
    updates = 0

    try:
        with open(args.fname, 'rb') as f:
            reader = csv.DictReader(f)
            for row in reader:
                records += 1
                hid = row[args.hidcolname]
                try:
                    q = sesh.query(History).filter(History.id == hid)
                    r = q.all()

                    # Basic check for borkiness
                    if len(r) != 1:
                        raise Exception('Multiple History ids found. This should never happen')

                    hist = q.first()
                    element = getattr(hist, args.dbname)
                    element = row[args.colname]
                    updates += 1

                except Exception as e:
                    if args.supress:
                        continue
                    else:
                        raise e
        log.info('Sucessfully inserted {} of {} entries into the session'.format(updates, records))
    except:
        log.exception('An error has occured, rolling back')
        sesh.rollback()
    else:
        if args.diag:
            log.info('Diagnostic mode, rolling back all transactions')
            sesh.rollback()
        else:
            log.info('Commiting the session')
            sesh.commit()
    finally:
        sesh.close()

if __name__ == '__main__':

    parser = ArgumentParser()
    parser.add_argument('hidcolname', help='Column name containing the history_id to modify'),
    parser.add_argument('colname', help='Column name containing the data to insert'),
    parser.add_argument('dbname', choices=['elevation', 'province'],
                        help='meta_history entity name to update'),
    parser.add_argument('fname', help='Path to csv file being processed'),
    parser.add_argument('-c', '--connection_string', required=True,
                        help='PostgreSQL connection string of form:\n\tdialect+driver://username:password@host:port/database\nExamples:\n\tpostgresql://scott:tiger@localhost/mydatabase\n\tpostgresql+psycopg2://scott:tiger@localhost/mydatabase\n\tpostgresql+pg8000://scott:tiger@localhost/mydatabase')
    parser.add_argument('-d', '--diag', default=True, action="store_true",
                        help="Turn on diagnostic mode (no commits)")
    parser.add_argument('-s', '--supress', default=False, action="store_true",
                        help="Supress insertion/lookup errors. Defaults to False.")

    args = parser.parse_args()
    main(args)