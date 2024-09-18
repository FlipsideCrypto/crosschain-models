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
    question,
    description,
    condition_id,
    question_id,
    token_1_token_id as token_0_id,
    token_1_outcome as token_0_outcome,
    token_1_winner as token_0_winner,
    token_2_token_id as token_1_id,
    token_2_outcome as token_1_outcome,
    token_2_winner as token_1_winner,
    active,
    closed,
    minimum_order_size,
    minimum_tick_size,
    end_date_iso as end_date,
    game_start_time as start_time,
    maker_base_fee,
    taker_base_fee,
    neg_risk,
    _inserted_timestamp AS  inserted_timestamp,
    modified_timestamp,
    dim_markets_id
FROM
    {{ source(
        'external',
        'polymarket_markets'
    ) }}