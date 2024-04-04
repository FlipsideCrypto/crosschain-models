{{ config(
    materialized = 'view',
    persist_docs ={ "relation": true,
    "columns": true }
) }}

SELECT
    HOUR,
    symbol,
    price,
    blockchain,
    is_imputed,
    is_deprecated,
    inserted_timestamp,
    modified_timestamp,
    complete_native_prices_id AS ez_hourly_native_prices_id
FROM
    {{ ref('silver__complete_native_prices') }}
