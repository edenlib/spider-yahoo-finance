"""Entrypoint.

Python 3.8
Author: Adam Turner <turner.adch@gmail.com>
"""

# standard library
import datetime
import sys
# local modules
import pipeline


def main():
    cmd = {"ticker": "AMZN", "year": datetime.datetime.today().year, "month": 1, "day": 1}

    for i, k in enumerate(cmd):
        try:
            cmd[k] = sys.argv[i+1]
        except IndexError:
            break

    print(cmd)

    r = pipeline.extract(cmd)

    df = pipeline.transform(r, cmd["ticker"])

    pipeline.load(df)

    # TODO: debug and add send to db function
    breakpoint()

    return None


if __name__ == "__main__":
    main()
