{{ config(
    materialized = 'view', 
) }}

SELECT 
    collection
    , mint
    , token_id
    , deal_score_rank
    , rarity_rank
    , floor_price
    , fair_market_price
    , price_low
    , price_high
FROM 
    {{ ref('silver__nft_fair_market_value') }}