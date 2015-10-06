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

    try:
        with open(resource_filename('crmp-helpers', '/data/provs_and_elevs_metadata_patch_20151005.csv'), 'rb') as f:
            reader = csv.DictReader(f)
            '''
            header: "station_id","history_id","station_name","lon","lat","province","elev","freq","network_id","native_id"
            '''
            for row in reader:
                hid = row['history_id']
                q = sesh.query(History).filter(History.id == hid)
                r = q.all()

                # Basic check for borkiness
                if len(r) != 1:
                    raise Exception('Multiple History ids found. This should never happen')

                hist = q.first()
                hist.elevation = row['elev']

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
    parser.add_argument('-c', '--connection_string', required=True,
                         help='PostgreSQL connection string of form:\n\tdialect+driver://username:password@host:port/database\nExamples:\n\tpostgresql://scott:tiger@localhost/mydatabase\n\tpostgresql+psycopg2://scott:tiger@localhost/mydatabase\n\tpostgresql+pg8000://scott:tiger@localhost/mydatabase')
    parser.add_argument('-D', '--diag', default=True, action="store_true",
                        help="Turn on diagnostic mode (no commits)")

    args = parser.parse_args()
    main(args)
