{{ config(
    materialized = 'incremental',
    unique_key = ['complete_market_data_coingecko_id'],
    incremental_strategy = 'delete+insert',
    cluster_by = ['_inserted_timestamp::DATE'],
    tags = ['market_data']
) }}

SELECT 
    _inserted_date AS recorded_date,
    id AS asset_id,
    name,
    UPPER(m.symbol) AS symbol,
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
    index,
    provider,
    _inserted_timestamp,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    {{ dbt_utils.generate_surrogate_key(['asset_id','recorded_date','provider']) }} AS complete_market_data_coingecko_id,
    '{{ invocation_id }}' AS _invocation_id
FROM {{ ref('bronze__all_market_data_coingecko') }} m

{% if is_incremental() %}
WHERE _inserted_timestamp > (
    SELECT
        MAX(_inserted_timestamp)
    FROM
        {{ this }}
)
{% endif %}

qualify(ROW_NUMBER() over (PARTITION BY asset_id, recorded_date, provider
ORDER BY
    _inserted_timestamp DESC)) = 1