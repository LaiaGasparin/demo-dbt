{{ config(materialized='table') }}

select
  to_number(to_char(d.date_day,'YYYYMMDD')) as date_key,
  d.date_day,
  year(d.date_day)        as year,
  month(d.date_day)       as month_num,
  to_char(d.date_day,'Month') as month_name,
  quarter(d.date_day)     as quarter,
  day(d.date_day)         as day_of_month,
  dayofweek(d.date_day)   as weekday_num,
  to_char(d.date_day,'DY') as weekday_name
from (
  select
    to_date(dateadd(day, seq4(), '1990-01-01')) as date_day
  from table(generator(rowcount => 20000))  -- ~55 years
) d
