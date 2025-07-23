{{ config(
    materialized = 'incremental',
    unique_key = ['market_data_coingecko_id'],
    incremental_strategy = 'delete+insert',
    cluster_by = '_inserted_timestamp::date',
    tags = ['market_data']
) }}

SELECT
    index,
    -- Core identifiers
    value:id::STRING AS id,
    value:name::STRING AS name,
    value:symbol::STRING AS symbol,
    value:image::STRING AS image,
    
    -- Price data (using FLOAT for decimal precision)
    value:current_price::FLOAT AS current_price,
    value:ath::FLOAT AS ath,
    value:ath_change_percentage::FLOAT AS ath_change_percentage,
    TO_TIMESTAMP(value:ath_date::STRING) AS ath_date,
    value:atl::FLOAT AS atl,
    value:atl_change_percentage::FLOAT AS atl_change_percentage,
    TO_TIMESTAMP(value:atl_date::STRING) AS atl_date,
    
    -- 24h price changes (using FLOAT for decimal precision)
    value:high_24h::FLOAT AS high_24h,
    value:low_24h::FLOAT AS low_24h,
    value:price_change_24h::FLOAT AS price_change_24h,
    value:price_change_percentage_24h::FLOAT AS price_change_percentage_24h,
    
    -- Market cap data
    TRY_TO_NUMBER(value:market_cap::STRING) AS market_cap,
    TRY_TO_NUMBER(value:market_cap_rank::STRING) AS market_cap_rank,
    value:market_cap_change_24h::FLOAT AS market_cap_change_24h,
    value:market_cap_change_percentage_24h::FLOAT AS market_cap_change_percentage_24h,
    TRY_TO_NUMBER(value:fully_diluted_valuation::STRING) AS fully_diluted_valuation,
    
    -- Supply data (using TRY_TO_NUMBER for large integers)
    TRY_TO_NUMBER(value:circulating_supply::STRING) AS circulating_supply,
    TRY_TO_NUMBER(value:total_supply::STRING) AS total_supply,
    TRY_TO_NUMBER(value:max_supply::STRING) AS max_supply,
    
    -- Volume data
    TRY_TO_NUMBER(value:total_volume::STRING) AS total_volume,
    
    -- ROI (can be null)
    value:roi AS roi,
    
    -- Timestamps and metadata
    TO_TIMESTAMP(value:last_updated::STRING) AS last_updated,

    VALUE AS DATA,
    provider,
    partition_key,
    partition_ts,
    _inserted_date,
    _inserted_timestamp,
    {{ dbt_utils.generate_surrogate_key(['id','_inserted_date']) }} AS market_data_coingecko_id

FROM {{ ref('bronze__streamline_market_data_coingecko') }}
WHERE DATA IS NOT NULL
{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT MAX(_inserted_timestamp)
    FROM {{ this }}
)
{% endif %} 

qualify(ROW_NUMBER() over (PARTITION BY id, _inserted_date
ORDER BY
    _inserted_timestamp DESC)) = 1