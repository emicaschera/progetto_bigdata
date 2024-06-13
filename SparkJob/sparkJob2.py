#!/usr/bin/env python3

import argparse
from operator import add

from pyspark.sql import SparkSession

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

rdd = df.rdd


def parse_line(row):
    ticker = row.ticker
    year = str(row[1])[:4]
    close = row.close
    open = row.open
    industry = row.industry
    sector = row.sector
    volume = row.volume
    return ticker, year, close, open, industry, sector, volume


rdd = rdd.map(parse_line)

# find the higher variation for every (industry, year) --> (higher variation)

# RDD with key (industry, year) --> value (close, open)
industry_year_rdd = rdd.map(lambda x: ((x[4], x[1]), (x[2], x[3])))

# sum (open) and (close) for every industry, year
industry_year_total_open_close_rdd = industry_year_rdd.reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1]))

# process the variation for the specific industry, year
industry_year_variation = (industry_year_total_open_close_rdd.
                           mapValues(lambda total: ((total[0] - total[1]) / total[1]) * 100))


# transform the RDD in dataframe, so it's easily to show
industry_year_variation_df = industry_year_variation.map(lambda x: (x[0][0], x[0][1], x[1])).toDF(
    ["industry", "year", "year_percent_variation"])

industry_year_variation_df = industry_year_variation_df.orderBy("year_percent_variation", ascending=False)

# find the higher variation for every (industry, year) --> (ticker, higher variation)

# RDD with key (ticker, industry, year) --> value (open, close)
ticker_industry_year_rdd = rdd.map(lambda x: ((x[0], x[4], x[1]), (x[2], x[3])))

# sum (open) and (close) for the specific ticker, industry v
ticker_industry_year_total_open_close_rdd = ticker_industry_year_rdd.reduceByKey(
    lambda x, y: (x[0] + y[0], x[1] + y[1]))

# process the variation for the specific ticker, industry, year
ticker_industry_year_variation_rdd = ticker_industry_year_total_open_close_rdd.mapValues(
    lambda total: ((total[0] - total[1]) / total[1]) * 100)

# RDD with key (industry, year) --> value (ticker, perc_variation)
industry_year2ticker_variation_rdd = ticker_industry_year_variation_rdd.map(
    lambda x: ((x[0][1], x[0][2]), (x[0][0], x[1])))

# find ticker with higher variation
industry_year2ticker_max_variation_rdd = (industry_year2ticker_variation_rdd.
                                          reduceByKey(lambda x, y: (x if x[1] > y[1] else y)))

# transform the RDD in dataframe, so it's easily to show
industry_year2ticker_max_variation_df = (industry_year2ticker_max_variation_rdd.
                                         map(lambda x: (x[0][0], x[0][1], x[1][0], x[1][1])).
                                         toDF(["industry", "year", "ticker", "percent_variation"]))

#industry_year2ticker_max_variation_df = industry_year2ticker_max_variation_df.orderBy("percent_variation", ascending=False)
# find the ticker with higher exchange volume for each industry, year

# RDD with key (ticker, industry, year) --> value (volume)
ticker_industry_year2volume_rdd = rdd.map(lambda x: ((x[0], x[4], x[1]), x[6]))

# find the total exchange volume
ticker_industry_year2total_volume_rdd = ticker_industry_year2volume_rdd.reduceByKey(add)

# find the ticker with higher exchange volume
ticker_industry_year2max_volume_rdd = (
    ticker_industry_year2total_volume_rdd.map(lambda x: ((x[0][1], x[0][2]), (x[0][0], x[1])))
    .reduceByKey(lambda a, b: a if a[1] > b[1] else b))

# transform the RDD in dataframe, so it's easily to show
ticker_industry_year2max_volume_df = ticker_industry_year2max_volume_rdd.map(
    lambda x: (x[0][0], x[0][1], x[1][0], x[1][1])).toDF(["industry", "year", "ticker", "max_volume"])

# Perform the join on the columns 'industry' and 'year'
joined_df = industry_year_variation_df.join(
    industry_year2ticker_max_variation_df,
    on=['industry', 'year'],
    how='inner'  # Use 'inner', 'left', 'right', or 'outer' depending on your requirements
)

# Show the result
sorted_joined = joined_df.orderBy("percent_variation", ascending=False)
sorted_joined.show(10, truncate=False)
# show output
#industry_year2ticker_max_variation_df.show(10, truncate=False)
ticker_industry_year2max_volume_df.show(10, truncate=False)
#industry_year_variation_df.show(10, truncate=False)
"""industry_year2ticker_max_variation_df.write.csv(f"{output_filepath}/industry_year2ticker_max_variation_df.csv", header=True)
ticker_industry_year2max_volume_df.write.csv(f"{output_filepath}/ticker_industry_year2max_volume_df.csv", header=True)
industry_year_variation_df.write.csv(f"{output_filepath}/industry_year_variation_df.csv", header=True)"""