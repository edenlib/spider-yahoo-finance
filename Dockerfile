FROM python:3.8

ENV TICKER=AMZN
ENV YEAR=2020
ENV MONTH=1
ENV DAY=1

COPY requirements.txt /app/requirements.txt

RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir --upgrade -r /app/requirements.txt

COPY src /app/src

ENTRYPOINT [ "python" ]
CMD /app/src/main.py ${TICKER} ${YEAR} ${MONTH} ${DAY}
