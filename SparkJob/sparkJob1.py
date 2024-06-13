#!/usr/bin/env python3

import argparse
import time
from pyspark.sql import SparkSession

#from pyspark.sql.functions import col, a first, last, year, collect_list, struct

# create parser and set its arguments
parser = argparse.ArgumentParser()
parser.add_argument("--input_path", type=str, help="input file path")
parser.add_argument("--output_path", type=str, help="output folder path")

# parse parameters
args = parser.parse_args()
input_filepath, output_filepath = args.input_path, args.output_path

# initialize SparkSession
spark = SparkSession \
    .builder \
    .appName("SparkJob1") \
    .config("spark.executor.memory", "6g") \
    .config("spark.executor.cores", "2") \
    .config("spark.driver.memory", "6g") \
    .config("spark.executor.instances", "2") \
    .getOrCreate()

# Read the dataset
df = spark.read.csv(input_filepath, header=True, inferSchema=True)

rdd = df.rdd


def extract_ticker_year(row):
    ticker = row.ticker
    year = str(row[5])[:4]
    low = float(row[2])
    high = float(row[3])
    volume = row.volume
    close = float(row[1])
    name = row[6]
    return (ticker, year), (low, high, volume, close, name)


ticker_year_rdd = rdd.map(extract_ticker_year)

min_max_avg_rdd = ticker_year_rdd.aggregateByKey((float('inf'), float('-inf'), 0, 0, None, None, None),
                                                 lambda acc, value: (min(acc[0], value[0]), max(acc[1], value[1]), acc[2] + value[2], acc[3] + 1,
                                                                     value[3] if acc[4] is None else acc[4], value[3], value[4]),
                                                 lambda acc1, acc2: (min(acc1[0], acc2[0]), max(acc1[1], acc2[1]), acc1[2] + acc2[2], acc1[3] + acc2[3],
                                                                     acc2[4] if acc1[4] is None else acc1[4], acc2[5], acc1[6]))

# Calcola il volume medio
min_max_avg_rdd = min_max_avg_rdd.mapValues(lambda x: (x[0], x[1], x[2] / x[3], ((x[5] - x[4]) / x[4]) * 100 if x[4] is not None and x[5] is not None else None, x[6]))

# Converti l'RDD in DataFrame
df = min_max_avg_rdd.map(lambda x: (x[0][0], x[1][4], x[0][1], round(x[1][0], 2), round(x[1][1], 2), round(x[1][2], 2), round(x[1][3], 2))).toDF(
    ["Ticker", "Nome", "Anno", "PrezzoMin", "PrezzoMax", "VolumeMedio", "variazionePercentuale"])
df = df.orderBy("Ticker")
"""df = df.withColumn("PrezzoMin", round("PrezzoMin", 2)) \
       .withColumn("PrezzoMax", round("PrezzoMax", 2)) \
       .withColumn("VolumeMedio", round("VolumeMedio", 2)) \
       .withColumn("variazionePercentuale", round("variazionePercentuale", 2))"""



# Mostra il DataFrame risultante
df.show()
#df.coalesce(1).write.csv(output_filepath, header=True, mode="overwrite")


# Interrompi SparkSession
spark.stop()
