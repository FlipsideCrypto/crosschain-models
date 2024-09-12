{{ config(
    materialized = 'view',
    persist_docs ={ "relation": true,
    "columns": true },
    meta={
        'database_tags':{
            'table': {
                'PROTOCOL': 'POLYMARKET'
            }
        }
    }
) }}

SELECT
    'polygon' AS blockchain,
    'polymarket' AS platform,
    condition_id,
    question,
    description,
    token_1_token_id,
    token_1_outcome,
    token_1_winner,
    token_2_token_id,
    token_2_outcome,
    token_2_winner,
    enable_order_book,
    active,
    closed,
    archived,
    accepting_orders,
    accepting_order_timestamp,
    minimum_order_size,
    minimum_tick_size,
    question_id,
    market_slug,
    end_date_iso,
    game_start_time,
    seconds_delay,
    fpmm,
    maker_base_fee,
    taker_base_fee,
    neg_risk,
    neg_risk_market_id,
    neg_risk_request_id,
    rewards,
    tags
FROM
    {{ source(
        'external',
        'dim_markets'
    ) }}
