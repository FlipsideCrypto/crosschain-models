{{ config(
    materialized = "view",
) }}


WITH cte_my_date AS (
    SELECT DATEADD(HOUR, SEQ4(), '2009-01-01 00:00:00') AS my_date
    FROM TABLE(GENERATOR(ROWCOUNT=>1000000))
)
SELECT
    TO_TIMESTAMP(my_date) as date_hour
FROM cte_my_date
WHERE date_hour < current_timestamp