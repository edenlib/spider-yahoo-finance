# Yahoo Finance Spider
Hits up our friends at Yahoo Finance and requests historical market data.

## Container Quickstart
Environment variables are all optional and default to `AMZN 2020 1 1`. Pass a ticker and start date `YYYY-MM-DD`.
```shell
$ podman build --tag spider_yahoo_finance:1.0 .
$ podman run --rm --name syf -e TICKER=AMZN YEAR=2016 MONTH=1 DAY=1 spider_yahoo_finance:1.0
```
