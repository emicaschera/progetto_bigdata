create table if not exists stock_data_job2 (
    ticker string,
    `date` date,
    close float,
    open float,
    industry string,
    sector string,
    volume int
)
row format delimited
fields terminated by ','
lines terminated by '\n'
tblproperties ("skip.header.line.count"="1");

load data local inpath '/home/emilio/PycharmProjects/progetto_bigdata/dataset/dataset_job2.csv'
overwrite into table stock_data_job2;

with industry_year_stats as (
    select
        industry,
        year(`date`) as year,
        sum(close) as total_close,
        sum(open) as total_open
    from
        stock_data_job2
    group by
        industry, year(`date`)
),
ticker_variation as (
    select
        industry,
        ticker,
        year(`date`) as year,
        sum(close) as total_close,
        sum(open) as total_open
    from
        stock_data_job2
    group by
        industry, year(`date`), ticker
),
max_variatoin_ticker as (
    select
        industry,
        year,
        ticker,
        MAX(((total_close - total_open) / total_open) * 100) AS max_variation
    from
        ticker_variation
    group by
        industry, year, ticker
),
industry_ticker_volume as (
    select
        industry,
        year(`date`) as year,
        ticker,
        sum(volume) as total_volume
    from
        stock_data_job2
    group by
        industry, year(`date`), ticker
),
    industry_max_volume as (
        select
            industry,
            year,
            max(total_volume) as max_volume
        from
            industry_ticker_volume itv

        group by
            industry, year
    )
select
    iys.industry,
    itv.ticker,
    iys.year,
    (((iys.total_close - iys.total_open) / iys.total_open) * 100) AS percentage_change,
    itv.total_volume,
    mvt.max_variation
from
    industry_year_stats iys
join
    industry_ticker_volume itv
on
    iys.industry = itv.industry
and
    iys.year = itv.year
join
    industry_max_volume imv
on
    itv.industry = imv.industry
and
    itv.year = imv.year
and
    itv.total_volume = imv.max_volume
join
    max_variatoin_ticker mvt
on
    iys.industry = mvt.industry
and
    iys.year = mvt.year
GROUP BY
    iys.industry,
    iys.year,
    itv.ticker,
    iys.total_close,
    iys.total_open,
    itv.total_volume,
    mvt.max_variation
order by
    mvt.max_variation desc
limit
    10
