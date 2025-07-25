{{ config(
    materialized = 'view',
    persist_docs ={ "relation": true,
    "columns": true },
    meta ={ 'database_tags':{ 'table':{ 'PURPOSE': 'PRICES, METADATA',
    } } },
    tags = ['prices']
) }}

SELECT
    asset_id,
    recorded_date AS day,
    name,
    symbol,
    fully_diluted_valuation,
    circulating_supply,
    total_supply,
    max_supply,
    current_price,
    ath,
    ath_change_percentage,
    ath_date,
    atl,
    atl_change_percentage,
    atl_date,
    high_24h,
    low_24h,
    price_change_24h,
    price_change_percentage_24h,
    market_cap,
    market_cap_rank,
    market_cap_change_24h,
    market_cap_change_percentage_24h,
    total_volume,
    roi_json,
    image_url,
    last_updated,
    provider,
    inserted_timestamp,
    modified_timestamp,
    complete_market_data_coingecko_id AS fact_asset_metrics_daily_id
FROM
    {{ ref('silver__complete_market_data_coingecko') }}
