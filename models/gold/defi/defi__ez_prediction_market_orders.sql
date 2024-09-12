{{ config(
    materialized = 'view',
    persist_docs ={ "relation": true,
    "columns": true },
    meta ={ 'database_tags':{ 'table':{ 'PROTOCOL': 'POLYMARKET',
    'PURPOSE': 'PREDICTION MARKET',
    } } }
) }}

SELECT
    'polygon' AS blockchain,
    'polymarket' AS platform,
    block_number,
    block_timestamp,
    tx_hash,
    question,
    market_slug,
    end_date_iso,
    outcome,
    order_hash,
    marker,
    taker,
    condition_id,
    question_id,
    asset_id, 
    maker_asset_id, 
    taker_asset_id,
    amount_usd,
    shares,
    price_per_share
FROM
    {{ source(
        'polygon_silver',
        'polymarket_filled_orders'
    ) }}