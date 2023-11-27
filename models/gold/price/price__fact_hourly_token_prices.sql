{{ config(
    materialized = 'view',
    persist_docs ={ "relation": true,
    "columns": true }
) }}

SELECT
    hour,
    token_address,
    blockchain,
    provider,
    price,
    is_imputed,
    COALESCE(inserted_timestamp,'2000-01-01') as inserted_timestamp,
    COALESCE(modified_timestamp,'2000-01-01') as modified_timestamp,
    COALESCE(token_prices_all_providers_hourly_id,{{ dbt_utils.generate_surrogate_key(['hour','token_address','blockchain']) }}) as fact_hourly_token_prices_id
FROM {{ ref('silver__token_prices_all_providers_hourly') }}



