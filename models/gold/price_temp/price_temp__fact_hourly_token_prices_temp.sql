{{ config(
    materialized = 'view',
    persist_docs ={ "relation": true,
    "columns": true }
) }}

SELECT
    id,
    recorded_hour AS HOUR,
    OPEN,
    high,
    low,
    CLOSE,
    provider,
    inserted_timestamp,
    modified_timestamp,
    all_prices_all_providers_id AS fact_hourly_token_prices_id
FROM
    {{ ref('silver__all_prices_all_providers2') }}
