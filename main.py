"""Extracts ticker price data from Yahoo Finance and loads to db.

Python 3.8
Author: Adam Turner <turner.adch@gmail.com>
"""

# standard library
import datetime
import sys
# python package index
import lxml.html
import pandas as pd
import requests
# eden library
import conndb


def extract(cmd):
    """Requests data from Yahoo Finance.

    Args:
        cmd: dict with start date from command line arguments

    Returns:
        requests Response object
    """
    p1 = int(datetime.datetime(year=int(cmd["year"]), month=int(cmd["month"]), day=int(cmd["day"])).timestamp())
    p2 = int(datetime.datetime.today().timestamp())
    url = f"https://finance.yahoo.com/quote/{cmd['ticker']}/history?" \
        f"period1={p1}&" \
        f"period2={p2}&interval=1d&filter=history&frequency=1d&includeAdjustedClose=true"
    headers = {"user-agent": "Mozilla/5.0 (X11; Fedora; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/88.0.4324.96 Safari/537.36"}

    return requests.get(url, headers=headers)


def transform(r, ticker):
    """Processes data from the HTML response.

    Args:
        r: requests Response object
        ticker: str ticker

    Returns:
        df: pandas DataFrame object
    """
    doc = lxml.html.fromstring(r.text)
    matrix = doc.xpath("//table[@data-test='historical-prices']//td")

    numcols = 7
    records = []
    for i in range(0, len(matrix), numcols):
        records.append(tuple([e.text_content() for e in matrix[i:i+7]]))

    df = pd.DataFrame.from_records(records)
    df.columns = ["date", "open", "high", "low", "close", "adj_close", "volume"]
    df.dropna(axis=0, how="any", inplace=True)
    df.insert(loc=0, column="ticker", value=ticker)

    return df


def load(df):
    """Loads data into Postgres database.

    Args:
        df: pandas DataFrame object
    """


    return None


def pipeline():
    cmd = {"ticker": "AMZN", "year": datetime.datetime.today().year, "month": 1, "day": 1}

    for i, k in enumerate(cmd):
        try:
            cmd[k] = sys.argv[i+1]
        except IndexError:
            break

    print(cmd)

    r = extract(cmd)

    df = transform(r, cmd["ticker"])

    load(df)

    # TODO: debug and add send to db function
    breakpoint()

    return None


if __name__ == "__main__":
    pipeline()
