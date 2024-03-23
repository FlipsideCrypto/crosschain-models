{{ config(
    materialized = 'view',
    persist_docs ={ "relation": true,
    "columns": true }
) }}

SELECT
    HOUR,
    p.token_address,
    p.id,
    symbol,
    decimals,
    price,
    p.blockchain,
    p.blockchain_id,
    is_imputed,
    CASE
        WHEN m.is_deprecated IS NULL THEN FALSE
        ELSE m.is_deprecated
    END AS is_deprecated,
    GREATEST(COALESCE(p.inserted_timestamp, '2000-01-01'), COALESCE(m.inserted_timestamp, '2000-01-01')) AS inserted_timestamp,
    GREATEST(COALESCE(p.modified_timestamp, '2000-01-01'), COALESCE(m.modified_timestamp, '2000-01-01')) AS modified_timestamp,
    token_prices_priority_hourly_id AS ez_hourly_token_prices_id
FROM
    {{ ref('silver__token_prices_priority2') }}
    p
    LEFT JOIN {{ ref('price_temp__ez_asset_metadata_temp') }}
    m
    ON p.token_address = m.token_address
    AND p.blockchain = m.blockchain
