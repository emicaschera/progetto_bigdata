#!/usr/bin/env python3
import csv
import sys
from io import StringIO

# dictionary to calculate the var_perc of the whole industry for every year
industry_open_close_price = {}

industry_ticker_open_close_price = {}

# dictionary used to calculate  the variation of the whole industry
industry_ticker_var = {}

# dictionary to store the sum of the volume value for each industry, year, ticker
industry2ticker_volume = {}

# dictionary used to calculate di ticker with max variation for each industry, year
industry2max_var = {}

# dictionary used to calculate the variation for each industry, ticker, year
industry2var = {}

for line in sys.stdin:

    def parse_line(line):
        # Utilizziamo StringIO per trattare la singola linea come un file
        f = StringIO(line)
        reader = csv.reader(f)
        elements = next(reader)

        # Controlliamo se il numero di elementi è corretto
        if len(elements) != 7:
            raise ValueError(f"Errore: la linea '{line}' non ha esattamente 7 elementi.")

        # Estraggo i singoli elementi
        ticker, date, close_price, open_price, industry, sector, volume = elements

        return ticker, date, close_price, open_price, industry, sector, volume


    # remove leading and trailing spaces
    line = line.strip()
    # splitting the line into variables
    industry, year, close_price, open_price, ticker, sector, volume = parse_line(line)
    # print("Linea valida:", ticker, year, close_price, open_price, industry, sector, volume)

    # remove leading and trailing spaces
    year = int(year.strip())
    close_price = float(close_price.strip())
    open_price = float(open_price.strip())
    volume = int(volume.strip())
    industry = industry.strip()
    sector = sector.strip()
    ticker = ticker.strip()

    industry_year = (industry, year)

    # fill the dictionary used to calculate the ticker with max volume
    if industry_year in industry2ticker_volume:
        if ticker in industry2ticker_volume[industry_year]:
            industry2ticker_volume[industry_year][ticker] += volume
        else:
            industry2ticker_volume[industry_year] = {ticker: volume}
    else:
        industry2ticker_volume[industry_year] = {ticker: volume}

    # fill the dictionary to store the sum of open/close price for each industry and year
    if industry_year in industry_open_close_price:
        industry_open_close_price[industry_year]['open_price'] += open_price
        industry_open_close_price[industry_year]['close_price'] += close_price
    else:
        industry_open_close_price[industry_year] = {'open_price': open_price, 'close_price': close_price}

    # fill the dictionary to store the sum of close/open for every industry, year, ticker
    if industry_year in industry_ticker_open_close_price:
        if ticker in industry_ticker_open_close_price[industry_year]:
            industry_ticker_open_close_price[industry_year][ticker]['open_price'] += open_price
            industry_ticker_open_close_price[industry_year][ticker]['close_price'] += close_price
        else:
            industry_ticker_open_close_price[industry_year][ticker] = {'open_price': open_price,
                                                                       'close_price': close_price}
    else:
        industry_ticker_open_close_price[industry_year] = {
            ticker: {'open_price': open_price, 'close_price': close_price}}






print("di seguito una lista delle industri e per ogni anno una lista delle aziende relative all industria con la "
      "relativa somma dei volumi")
for k in industry2ticker_volume:
    print(k, industry2ticker_volume[k])

print("\n\ndi seguito una lista delle industrie e per ogni anno la variazione percentuale dell intera industria:")
variations = {}
for k in industry_open_close_price:
    var = ((industry_open_close_price[k]['close_price'] - industry_open_close_price[k]['open_price']) /
           industry_open_close_price[k]['open_price']) * 100
    variations[k] = var

sorted_variations = sorted(variations.items(), key=lambda x: x[1], reverse=True)

for industry, var in sorted_variations:
    print(industry, var)

for k in industry_ticker_open_close_price:
    ticker_item = industry_ticker_open_close_price[k]
    for j in ticker_item:
        var = (ticker_item[j]['close_price'] - ticker_item[j]['open_price']) / ticker_item[j]['open_price'] * 100
        industry_ticker_var[k] = {j: var}

print("\n\ndi seguito una lista delle industrie e per ogni anno la variazione oercentuale dei singoli ticker :")

for k in industry_ticker_var:
   print(k, industry_ticker_var[k])


"""def salva_mappe_in_csv(industry2ticker_volume, industry_open_close_price, industry_ticker_var):
    # Salva industry2ticker_volume in un file CSV
    with open('/home/emilio/PycharmProjects/progetto_bigdata/MapReduceJob/industry2ticker_volume.csv', 'w',
              newline='') as file:
        writer = csv.writer(file)
        writer.writerow(['Industria', 'Anno', 'Ticker', 'Volume'])
        for industria_anno, ticker_volume in industry2ticker_volume.items():
            industria, anno = industria_anno
            for ticker, volume in ticker_volume.items():
                writer.writerow([industria, anno, ticker, volume])

    # Salva industry_open_close_price in un file CSV
    with open('/home/emilio/PycharmProjects/progetto_bigdata/MapReduceJob/industry_open_close_price.csv', 'w',
              newline='') as file:
        writer = csv.writer(file)
        writer.writerow(['Industria', 'Anno', 'Open Price', 'Close Price', 'Variazione Percentuale'])
        for industria_anno, prezzi in industry_open_close_price.items():
            industria, anno = industria_anno
            open_price = prezzi['open_price']
            close_price = prezzi['close_price']
            variazione_percentuale = ((close_price - open_price) / open_price) * 100
            writer.writerow([industria, anno, open_price, close_price, variazione_percentuale])

    # Salva industry_ticker_var in un file CSV
    with open('/home/emilio/PycharmProjects/progetto_bigdata/MapReduceJob/industry_ticker_var.csv', 'w',
              newline='') as file:
        writer = csv.writer(file)
        writer.writerow(['Industria', 'Anno', 'Ticker', 'Variazione Percentuale'])
        for industria_anno, ticker_var in industry_ticker_var.items():
            industria, anno = industria_anno
            for ticker, variazione_percentuale in ticker_var.items():
                writer.writerow([industria, anno, ticker, variazione_percentuale])
"""

# Chiamata alla funzione per salvare le mappe in file CSV
#salva_mappe_in_csv(industry2ticker_volume, industry_open_close_price, industry_ticker_var)
