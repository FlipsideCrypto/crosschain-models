{{
    config(
        tags=["test_weekly"]
    )
}}

WITH base AS (
    SELECT
        status_code,
        count(*) AS count
    FROM
        crosschain.bronze_api.log_messages
    WHERE
        block_timestamp BETWEEN CURRENT_DATE - 92
        AND CURRENT_DATE - 2
    GROUP BY
        1,
)
select
    status_code,
    count
from
    base
where
    status_code is not in (200, 202, 404)
    and count > 0
    and count < 100