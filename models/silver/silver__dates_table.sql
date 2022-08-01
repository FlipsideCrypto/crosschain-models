{{ config(
    materialized = "table"
) }}
{{ dbt_date.get_date_dimension(
    '2017-01-01',
    current_date
) }}