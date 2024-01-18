{{ config(
    materialized = 'view',
    persist_docs ={ "relation": true,
    "columns": true }
) }}

SELECT
    b.blockchain,
    b.platform,
    b.block_number,
    b.block_timestamp,
    b.tx_hash,
    b.source_chain,
    b.destination_chain,
    b.bridge_address,
    b.source_address,
    b.destination_address,
    b.direction,
    b.token_address,
    p.symbol AS token_symbol,
    b.amount_raw,
    CASE
        WHEN p.decimals IS NOT NULL THEN b.amount_raw / power(
            10,
            p.decimals
        )
    END amount,
    ROUND(
        p.price * amount,
        2
    ) AS amount_usd,
    GREATEST(COALESCE(b.inserted_timestamp, '2000-01-01'), COALESCE(p.inserted_timestamp, '2000-01-01')) AS inserted_timestamp,
    GREATEST(COALESCE(b.modified_timestamp, '2000-01-01'), COALESCE(p.modified_timestamp, '2000-01-01')) AS modified_timestamp,
    fact_bridge_activity_id AS ez_bridge_activity_id
FROM
    {{ ref('defi__fact_bridge_activity') }}
    b
    LEFT JOIN {{ ref('price__ez_hourly_token_prices') }}
    p
    ON b.blockchain = p.blockchain
    AND b.token_address = p.token_address
    AND DATE_TRUNC(
        'hour',
        b.block_timestamp
    ) = p.hour
