#!/usr/bin/env python3
import csv
import datetime
import sys
from collections import defaultdict
from _datetime import datetime

stats = defaultdict(lambda: {'first_close': None, 'last_close': None, 'low': float('inf'), 'high': float('-inf'),
                             'volume_sum': 0, 'count': 0, 'name': '',
                             'first_date': datetime.max, 'last_date': datetime.min})

for line in sys.stdin:
    try:
        line = line.strip()

        ticker, year, close_price, low, high, volume, name, date = line.split(',')

        year = int(year)
        ticker = ticker.strip()
        close_price = float(close_price.strip())
        low = float(low.strip())
        high = float(high.strip())
        volume = int(volume.strip())
        name = name.strip()
        date = date.split(' ')[0]
        date = datetime.strptime(date, '%Y-%m-%d')

        ticker_year = (ticker, year)

        if date < stats[ticker_year]['first_date']:
            stats[ticker_year]['first_close'] = close_price
            stats[ticker_year]['first_date'] = date
        if date > stats[ticker_year]['last_date']:
            stats[ticker_year]['last_close'] = close_price
            stats[ticker_year]['last_date'] = date

        stats[ticker_year]['low'] = min(stats[ticker_year]['low'], low)
        stats[ticker_year]['high'] = max(stats[ticker_year]['high'], high)
        stats[ticker_year]['volume_sum'] += volume
        stats[ticker_year]['count'] += 1
        stats[ticker_year]['name'] = name
    except ValueError as e:
        print(f"errore_processamento: {line}", file=sys.stderr)
        print(e, file=sys.stderr)
        continue

for k, v in stats.items():
    percentage_change = ((v['last_close'] - v['first_close']) / v['first_close']) * 100
    average_volume = v['volume_sum'] / v['count']
    print(f"{k},{v['name']},{v['low']:.2f},{v['high']:.2f},{average_volume:.2f},{percentage_change:.2f}


"""
def save_to_csv(filename):
    with open(filename, mode='w', newline='') as csvfile:
        fieldnames = ['Ticker', 'Anno', 'Nome', 'Minimo', 'Massimo', 'Volume Medio', 'Variazione Percentuale']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

        writer.writeheader()
        for k, v in stats.items():
            ticker, year = k
            percentage_change = ((v['last_close'] - v['first_close']) / v['first_close']) * 100
            average_volume = v['volume_sum'] / v['count']
            writer.writerow({
                'Ticker': ticker,
                'Anno': year,
                'Nome': v['name'],
                'Minimo': f"{v['low']:.2f}",
                'Massimo': f"{v['high']:.2f}",
                'Volume Medio': f"{average_volume:.2f}",
                'Variazione Percentuale': f"{percentage_change:.2f}"
            })


output_filepath = '/home/emilio/PycharmProjects/progetto_bigdata/MapReduceJob/outputJob1/output.csv'
# Esempio di utilizzo
save_to_csv(output_filepath)
"""