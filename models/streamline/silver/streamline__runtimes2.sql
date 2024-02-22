{{ config (
    materialized = "view",
    tags = ['streamline_view']
) }}
-- Generate a spine of dates from 2018-01-01 to one day prior to current date

SELECT
    date_day :: TIMESTAMP AS run_time
FROM
    {{ ref('core__dim_dates') }}
WHERE
    date_day BETWEEN '2018-01-01' -- coingecko api pro plan start date
    AND DATEADD('day', -1, SYSDATE())
-- end date updated to run one day prior to current date