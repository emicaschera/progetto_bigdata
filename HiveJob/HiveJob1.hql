
SET mapreduce.map.memory.mb=4096;
SET mapreduce.reduce.memory.mb=4096;
SET hive.auto.convert.join=false;
SET mapreduce.job.maps=10;
SET mapreduce.job.reduces=10;

SET hive.cli.print.header=true;


create table if not exists stock_data_job1 (
    ticker string,
    close float,
    low float,
    high float,
    volume int,
    year date,
    name string
)
row format delimited
fields terminated by ','
lines terminated by '\n'
tblproperties ("skip.header.line.count"="1");

load data local inpath '/home/emilio/PycharmProjects/progetto_bigdata/dataset/dataset_job1.csv'
overwrite into table stock_data_job1;


WITH yearly_data AS (
    SELECT
        ticker,
        name,
        year(year) AS year,
        first_value(close) OVER (PARTITION BY ticker, year(year) ORDER BY year) AS first_close,
        last_value(close) OVER (PARTITION BY ticker, year(year) ORDER BY year ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_close,
        MIN(low) OVER (PARTITION BY ticker, year(year)) AS min_low,
        MAX(high) OVER (PARTITION BY ticker, year(year)) AS max_high,
        AVG(volume) OVER (PARTITION BY ticker, year(year)) AS avg_volume
    FROM
        stock_data_job1
),
    aggregated_data AS (
    SELECT
        ticker,
        name,
        year,
        ((last_close - first_close) / first_close) * 100 AS annual_percentage_change,
        min_low,
        max_high,
        avg_volume
    FROM
        yearly_data
    GROUP BY
        ticker,
        name,
        year,
        first_close,
        last_close,
        min_low,
        max_high,
        avg_volume
)

SELECT
    ticker,
    name,
    year,
    round(annual_percentage_change,2),
    min_low AS annual_min_price,
    max_high AS annual_max_price,
    round(avg_volume,2) AS annual_avg_volume
FROM
    aggregated_data
limit 10


