import os, sqlite3
from argparse import ArgumentParser, ArgumentTypeError
import pandas as pd
from budget import analysis
from budget.parsers import parse_month_datetime


def run():
    """ Runs the expense estimate script. """
    parser = ArgumentParser(
        prog="estimate-rent",
        description="calculates a rent estimate"
    )
    parser.add_argument("month", metavar="MONTH", type=check_month,
            help="the month to estimate in the format '%%b', '%%B', '%%b %%Y', or '%%B %%Y'")
    parser.add_argument("residents", metavar="NUM_RESIDENTS", type=check_positive,
            help="the number of people occupying the residence (must be > 0)")
    parser.add_argument("--db", metavar="CONN_STR", type=str,
            help="specify the database to use (default: $DATABASE_URI)",
            default=os.getenv('DATABASE_URI'))

    args = parser.parse_args()

    con = sqlite3.connect(args.db, detect_types=sqlite3.PARSE_DECLTYPES)
    df = pd.read_sql('SELECT * FROM rent ORDER BY year DESC, month DESC', con)
    con.close()

    print("All Rent Data:")
    print(df, '\n')

    print("Upcoming Rent Estimate:")
    est = analysis.estimate_rent(df, args.residents, args.month)
    print(est)


def check_month(arg: str):
    value = parse_month_datetime(arg)
    if value == None:
        raise ArgumentTypeError("%s has an invalid month format" % value)
    return value

def check_positive(arg: str):
    value = int(arg)
    if value <= 0:
        raise ArgumentTypeError("%s is not greater than 0" % value)
    return value

if __name__ == "__main__":
    run()