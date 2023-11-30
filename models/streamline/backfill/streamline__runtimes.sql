{{ config (
    materialized = "view",
    tags = ['streamline_view']
) }}
-- Generate a spine of dates from 2018-01-01 to today

with base as (
    select
        date_day::TIMESTAMP as run_time
    from
        {{ ref('core__dim_dates')}}
    where
        date_day between '2018-01-01' and dateadd(day, 4747, '2018-01-01')  -- coin gecko pro provides historical data from 2018-01-01, dim_dates has 4747 days for this range (2018-01-01 to 2030-12-31)
)
select
    run_time
from
    base
where
    run_time <= date_trunc('day', current_timestamp)