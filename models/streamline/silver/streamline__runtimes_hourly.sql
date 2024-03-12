{{ config (
    materialized = "view"
) }}
-- Generate a spine of hours from 2023-03-01 to current timestamp

SELECT
    date_hour :: TIMESTAMP AS run_time
FROM
    {{ ref('core__dim_date_hours') }}
WHERE
    date_hour :: DATE BETWEEN '2023-03-01'
    AND SYSDATE()