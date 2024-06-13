WITH base AS (
    SELECT
        SPLIT_PART(MESSAGE, ',',2) AS status_code,
        count(*) as counter
    FROM
        {{ target.database }}.bronze_api.log_messages
    WHERE
        timestamp BETWEEN DATEADD(HOUR, -1, CURRENT_TIMESTAMP())
        AND CURRENT_TIMESTAMP()
        AND LOG_LEVEL = 'DEBUG'
    GROUP BY 1

)
select
    status_code,
    counter
from
    base
where
    status_code not in (200, 202, 404, 451)
    and counter > 500