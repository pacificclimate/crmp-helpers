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
    errors = 0

    try:
        with open(args.fname, 'rb') as f:
            reader = csv.DictReader(f)
            for row in reader:
                records += 1

                entry = row[args.colname]
                if entry == 'NA':
                    continue

                hid = row[args.hidcolname]
                try:
                    q = sesh.query(History).filter(History.id == hid)
                    r = q.all()

                    # Basic check for borkiness
                    if len(r) != 1:
                        raise Exception('Multiple History ids found. This should never happen')

                    hist = r[0]
                    element = setattr(hist, args.dbname, entry)
                    updates += 1

                except Exception as e:
                    if args.supress:
                        errors += 1
                        log.warning('Unable to update history_id {} field {} with {}'.format(hid, args.dbname, entry))
                        continue
                    else:
                        raise e
        log.info('Sucessfully inserted {} of {} entries into the session with {} errors'.format(updates, records, errors))

    except:
        log.exception('An error has occured, rolling back')
        sesh.rollback()
    else:
        if args.commit:
            log.info('Commiting the session')
            sesh.commit()
        else:
            log.info('Diagnostic mode, rolling back all transactions')
            sesh.rollback()
    finally:
        sesh.close()

if __name__ == '__main__':

    parser = ArgumentParser()
    parser.add_argument('hidcolname', help='Column name containing the history_id to modify'),
    parser.add_argument('colname', help='Column name containing the data to insert'),
    parser.add_argument('dbname', choices=['elevation', 'province'],
                        help='meta_history entity name to update. Valid entries are "elevation" and "province"'),
    parser.add_argument('fname', help='Path to csv file being processed'),
    parser.add_argument('-c', '--connection_string', required=True,
                        help='PostgreSQL connection string of form:\n\tdialect+driver://username:password@host:port/database\nExamples:\n\tpostgresql://scott:tiger@localhost/mydatabase\n\tpostgresql+psycopg2://scott:tiger@localhost/mydatabase\n\tpostgresql+pg8000://scott:tiger@localhost/mydatabase')
    parser.add_argument('--commit', default=False, action="store_true",
                        help="Actually commit the session. Default is to roll back"),
    parser.add_argument('-s', '--supress', default=False, action="store_true",
                        help="Supress insertion/lookup errors. Defaults to False.")

    args = parser.parse_args()
    main(args)
