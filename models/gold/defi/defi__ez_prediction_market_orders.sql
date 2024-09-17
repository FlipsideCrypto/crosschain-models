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
    outcome,
    maker,
    taker,
    amount_usd,
    shares,
    price_per_share,
    end_date_iso as end_date,
    order_hash,
    asset_id,
    condition_id,
    question_id,
    inserted_timestamp,
    modified_timestamp,
    polymarket_filled_orders_id as ez_prediction_market_orders_id
FROM
    {{ source(
        'polygon_silver',
        'polymarket_filled_orders'
    ) }}