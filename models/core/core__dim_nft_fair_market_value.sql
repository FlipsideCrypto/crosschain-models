{{ config(
    materialized = 'view', 
) }}

SELECT 
    collection
    , mint :: STRING AS mint
    , token_id :: INT AS token_id
    , deal_score_rank :: INT AS deal_score_rank
    , rarity_rank :: INT AS rarity_rank
    , floor_price :: FLOAT AS floor_price
    , fair_market_price :: FLOAT AS fair_market_price
    , price_low :: FLOAT AS price_low
    , price_high :: FLOAT AS price_high
FROM 
    {{ ref('silver__nft_fair_market_value') }}