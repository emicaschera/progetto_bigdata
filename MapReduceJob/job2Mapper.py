#!/usr/bin/env python3
import sys
from _datetime import datetime
import csv


def parse_line(line):

    reader = csv.reader([line])
    for row in reader:
        return row


for line in sys.stdin:

    if line.startswith('ticker'):
        continue

    data = parse_line(line)

    if len(data) != 7:
        continue

    # ticker, close_price, low, high, volume, date, name = data

    ticker = data[0]
    date = data[1]
    close_price = data[2]
    open_price = data[3]
    industry = data[4]
    sector = data[5]
    volume = data[6]

    ticker = ticker.strip()
    close_price = float(close_price.strip())
    open_price = float(open_price.strip())
    volume = int(volume.strip())
    industry = industry.strip()
    sector = sector.strip()
    year = datetime.strptime(date, "%Y-%m-%d").year

    print(f"{industry}, {year}, {close_price}, {open_price}, {ticker}, {sector}, {volume}")
