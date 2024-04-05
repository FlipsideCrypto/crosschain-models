{{ config(
    materialized = 'view',
    persist_docs ={ "relation": true,
    "columns": true }
) }}

SELECT
    HOUR,
    token_address,
    symbol,
    name,
    decimals,
    price,
    blockchain,
    blockchain_name,
    blockchain_id,
    FALSE AS is_native,
    is_imputed,
    is_deprecated,
    inserted_timestamp,
    modified_timestamp,
    complete_token_prices_id AS ez_hourly_token_prices_id
FROM
    {{ ref('silver__complete_token_prices') }}
UNION ALL
SELECT
    HOUR,
    NULL AS token_address,
    symbol,
    name,
    decimals,
    price,
    blockchain,
    blockchain AS blockchain_name,
    blockchain AS blockchain_id,
    TRUE AS is_native,
    is_imputed,
    is_deprecated,
    inserted_timestamp,
    modified_timestamp,
    complete_native_prices_id AS ez_hourly_native_prices_id
FROM
    {{ ref('silver__complete_native_prices') }}