{{ config(
    materialized = 'view',
    persist_docs ={ "relation": true,
    "columns": true }
) }}

SELECT
    HOUR,
    token_address,
    blockchain,
    provider,
    price,
    is_imputed,
    COALESCE(
        inserted_timestamp,
        '2000-01-01'
    ) AS inserted_timestamp,
    COALESCE(
        modified_timestamp,
        '2000-01-01'
    ) AS modified_timestamp,
    COALESCE(
        token_prices_all_providers_hourly_id,
        {{ dbt_utils.generate_surrogate_key(['hour','token_address','blockchain','provider']) }}
    ) AS fact_hourly_token_prices_id
FROM
    {{ ref('silver__token_prices_all_providers_hourly') }}
