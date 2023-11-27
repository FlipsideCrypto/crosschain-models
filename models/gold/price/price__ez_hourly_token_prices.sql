{{ config(
    materialized = 'view',
    persist_docs ={ "relation": true,
    "columns": true }
) }}

SELECT 
    hour,
    p.token_address,
    symbol,
    decimals,
    price,
    p.blockchain,
    is_imputed,
    GREATEST(COALESCE(p.inserted_timestamp,'2000-01-01'), COALESCE(m.inserted_timestamp,'2000-01-01')) as inserted_timestamp,
    GREATEST(COALESCE(p.modified_timestamp,'2000-01-01'), COALESCE(m.modified_timestamp,'2000-01-01')) as modified_timestamp,
    {{ dbt_utils.generate_surrogate_key(['hour','p.token_address','p.blockchain']) }}) AS ez_hourly_token_prices_id
FROM {{ ref('silver__token_prices_priority_hourly') }} p
LEFT JOIN {{ ref('price__ez_asset_metadata') }} m
    ON p.token_address = m.token_address 
        AND p.blockchain = m.blockchain