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

    #ticker, close_price, low, high, volume, date, name = data

    ticker = data[0]
    close_price = data[1]
    low = data[2]
    high = data[3]
    volume = data[4]
    date = data[5]
    name = data[6]

    date_obj = datetime.strptime(date, "%Y-%m-%d")
    close_price = float(close_price.strip())
    low = float(low.strip())
    high = float(high.strip())
    volume = int(volume.strip())
    name = name.strip()

    year = date_obj.year
    
    print(f"{ticker},{year},{close_price},{low},{high},{volume},{name},{date_obj}")
