from argparse import ArgumentParser
import os, sqlite3, logging
from budget import sheets

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

logging.basicConfig(
    level=logging.INFO,
    datefmt="%Y-%m-%d %H:%M:%S",
    format="[{asctime}] {name} {threadName} {levelname}: {message}",
    style='{'
)

def run():
    parser = ArgumentParser(
        prog="update-db",
        description="pulls the rent data from a google spreadsheet to a local database"
    )
    parser.add_argument("--db", metavar="CONN_STR", type=str,
            help="specify the database to use (default: $DATABASE_URI)",
            default=os.getenv('DATABASE_URI'))
    parser.add_argument("--sheet-id", metavar="SPREADSHEET_ID", type=str,
            help="specify the google spreadsheet to use (default: $SPREADSHEET_ID)",
            default=os.getenv('SPREADSHEET_ID'))
    parser.add_argument("--client-secret", metavar="FILE", type=str,
            help="specify the client secret for the project (default: $CLIENT_SECRET)",
            default=os.getenv('CLIENT_SECRET'))

    args = parser.parse_args()

    df = sheets.get_spreadsheet_dataframe(args.sheet_id, args.client_secret)

    con = sqlite3.connect(args.db, detect_types=sqlite3.PARSE_DECLTYPES)
    logger.info(f"{args.db}: connection established")

    TABLE_NAME = "rent"
    df.to_sql(name=TABLE_NAME, con=con, if_exists="replace", dtype={"id": "INTEGER PRIMARY KEY"})
    logger.info(f"{args.db}: dataframe written to '{TABLE_NAME}' table")

    con.close()
    logger.info(f"{args.db}: connection established")


if __name__ == "__main__":
    run()