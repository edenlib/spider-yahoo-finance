"""Entrypoint.

Python 3.8
Author: Adam Turner <turner.adch@gmail.com>
"""

# standard library
import datetime
import sys
# eden library
import conndb
# local modules
import pipeline


def main():
    cmd = {"ticker": "AMZN", "year": datetime.datetime.today().year, "month": 1, "day": 1}

    for i, k in enumerate(cmd):
        try:
            cmd[k] = sys.argv[i+1]
        except IndexError:
            break

    start_date = datetime.datetime(year=int(cmd["year"]), month=int(cmd["month"]), day=int(cmd["day"]))

    r = pipeline.extract(start_date, cmd["ticker"])

    df = pipeline.transform(r, cmd["ticker"])

    # conn = conndb.DBConfig.from_json("/home/adam/cfg/postgres-test.json").create_psycopg2_connection()
    conn = conndb.DBConfig.from_json("/app/cfg/postgres-test.json").create_psycopg2_connection()

    try:
        pipeline.load(df, conn)
    finally:
        conn.close()

    return None


if __name__ == "__main__":
    main()
