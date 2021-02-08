"""Classic ETL pipeline.

Python 3.8
Author: Adam Turner <turner.adch@gmail.com>
"""

# standard library
import datetime
import io
import re
# python package index
import lxml.html
import pandas as pd
import requests


def extract(start_date, ticker):
    """Requests data from Yahoo Finance.

    Args:
        start_date: datetime datetime object to start 
        ticker: str ticker name

    Returns:
        requests Response object
    """
    print("Extracting...")
    p1 = int(start_date.timestamp())
    p2 = int(datetime.datetime.today().timestamp())
    url = f"https://finance.yahoo.com/quote/{ticker}/history?" \
        f"period1={p1}&" \
        f"period2={p2}&interval=1d&filter=history&frequency=1d&includeAdjustedClose=true"
    headers = {"user-agent": "Mozilla/5.0 (X11; Fedora; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/88.0.4324.96 Safari/537.36"}

    return requests.get(url, headers=headers)


def transform(response, ticker):
    """Processes data from the HTML response.

    Args:
        response: requests Response object
        ticker: str ticker name

    Returns:
        df: pandas DataFrame object
    """
    print("Transforming...")
    doc = lxml.html.fromstring(response.text)
    matrix = doc.xpath("//table[@data-test='historical-prices']//td")

    numcols = 7
    records = []
    for i in range(0, len(matrix), numcols):
        records.append(tuple([e.text_content() for e in matrix[i:i+7]]))

    df = pd.DataFrame.from_records(records)
    df.columns = ["date", "open", "high", "low", "close", "adj_close", "volume"]
    df.replace(to_replace=re.compile(r","), value="", inplace=True)
    df.dropna(axis=0, how="any", inplace=True)
    df.insert(loc=0, column="ticker", value=ticker)

    return df


def load(df, conn):
    """Loads data into Postgres database.

    Args:
        df: pandas DataFrame object
        conn: live psycopg2 Connection object
    """
    print(f"Loading...\n{df.head()}")
    with io.StringIO() as csv_buffer:
        df.to_csv(csv_buffer, sep=",", header=True, index=False)
        csv_buffer.seek(0)
        with conn.cursor() as curs:
            query = "COPY market_data.hist_yahoo_finance_staging FROM STDIN WITH (FORMAT csv, DELIMITER ',', HEADER TRUE);"
            curs.copy_expert(query, csv_buffer)
    conn.commit()
    print("Commit!")

    return None
