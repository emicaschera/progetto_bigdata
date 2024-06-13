#!/usr/bin/env python3

import argparse
from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import col, min, max, avg, first, last, year, collect_list, struct

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
    .getOrCreate()

# Read the dataset
df = spark.read.csv(input_filepath, header=True, inferSchema=True)


# Aggiungi una colonna con l'anno
df = df.withColumn("year", year(df["date"]))

# Definisci una finestra per il partizionamento per ticker e year, ordinata per data
window_spec = Window.partitionBy("ticker", "year").orderBy("date")

# Calculate the first and last closing price of the year
df = df.withColumn("first_close", first("close").over(window_spec)) \
    .withColumn("last_close", last("close").over(window_spec))

# df.show()

result = df.groupBy("ticker", "year", "name") \
    .agg(
    first("first_close").alias("first_close"),
    last("last_close").alias("last_close"),
    min("low").alias("min_price"),
    max("high").alias("max_price"),
    avg("volume").alias("avg_volume"),

) \
    .withColumn("percent_var", ((col("first_close") - col("last_close")) / col("last_close")) * 100) \

final_result = result.drop("first_close", "last_close")

final_result = final_result.select("ticker",  "name", "year", "min_price", "max_price", "avg_volume", "percent_var")


# Mostra i risultati
final_result.show()

final_result.write.mode("overwrite").csv(output_filepath)

# Stop the SparkSession
spark.stop()
