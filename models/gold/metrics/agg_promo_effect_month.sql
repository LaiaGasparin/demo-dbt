{{ config(materialized='table', tags=['metrics']) }}

with base as (
  select
    d.year,
    d.month_num,
    (d.year*100 + d.month_num) as month_key,
    f.net_sales,
    case when dp.part_type ilike 'PROMO%' then f.net_sales else 0 end as promo_net_sales -- only take sales of promo
  from {{ ref('fact_sales') }} f
  join {{ ref('dim_date') }} d   on f.ship_date_key = d.date_key   -- ship month then real revenue on that date, order may mislead
  join {{ ref('dim_part') }} dp  on f.part_key = dp.part_key
)

select
  year,
  month_num,
  month_key,
  sum(promo_net_sales) as promo_revenue,
  sum(net_sales)       as total_revenue,
  case when sum(net_sales)=0 then 0
       else round(100.0 * sum(promo_net_sales)/sum(net_sales), 2)
  end as promo_revenue_pct
from base
group by 1,2,3
order by 1,2
