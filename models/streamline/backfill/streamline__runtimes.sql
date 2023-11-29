{{ config (
    materialized = "view",
    tags = ['streamline_view']
) }}
-- Generate a spine of dates from 2018-01-01 to today
with base as (
    select
        dateadd(day, id, '2017-12-31 00:00:00') as run_time -- coin gecko pro provides historical data from 2018-01-01
    from
        (select
            row_number() over (
                order by
                    seq4()
            ) as id
        from
            table(generator(rowcount => 4000))
        )
)
select
    run_time
from
    base
where
    run_time <= date_trunc('day', current_timestamp)