{{ config(
    materialized = 'incremental',
    unique_key = ['all_market_data_coingecko_id'],
    incremental_strategy = 'delete+insert',
    cluster_by = ['_inserted_date::DATE'],
    tags = ['market_data']
) }}

SELECT
    INDEX,
    VALUE :id :: STRING AS id,
    VALUE :name :: STRING AS NAME,
    VALUE :symbol :: STRING AS symbol,
    VALUE :current_price :: FLOAT AS current_price,
    VALUE :ath :: FLOAT AS ath,
    VALUE :ath_change_percentage :: FLOAT AS ath_change_percentage,
    TO_TIMESTAMP(
        VALUE :ath_date :: STRING
    ) AS ath_date,
    VALUE :atl :: FLOAT AS atl,
    VALUE :atl_change_percentage :: FLOAT AS atl_change_percentage,
    TO_TIMESTAMP(
        VALUE :atl_date :: STRING
    ) AS atl_date,
    VALUE :high_24h :: FLOAT AS high_24h,
    VALUE :low_24h :: FLOAT AS low_24h,
    VALUE :price_change_24h :: FLOAT AS price_change_24h,
    VALUE :price_change_percentage_24h :: FLOAT AS price_change_percentage_24h,
    TRY_TO_NUMBER(
        VALUE :market_cap :: STRING
    ) AS market_cap,
    TRY_TO_NUMBER(
        VALUE :market_cap_rank :: STRING
    ) AS market_cap_rank,
    VALUE :market_cap_change_24h :: FLOAT AS market_cap_change_24h,
    VALUE :market_cap_change_percentage_24h :: FLOAT AS market_cap_change_percentage_24h,
    TRY_TO_NUMBER(
        VALUE :fully_diluted_valuation :: STRING
    ) AS fully_diluted_valuation,
    TRY_TO_NUMBER(
        VALUE :circulating_supply :: STRING
    ) AS circulating_supply,
    TRY_TO_NUMBER(
        VALUE :total_supply :: STRING
    ) AS total_supply,
    TRY_TO_NUMBER(
        VALUE :max_supply :: STRING
    ) AS max_supply,
    TRY_TO_NUMBER(
        VALUE :total_volume :: STRING
    ) AS total_volume,
    VALUE :roi :: variant AS roi_json,
    VALUE :image :: STRING AS image_url,
    TO_TIMESTAMP(
        VALUE :last_updated :: STRING
    ) AS last_updated,
    VALUE AS DATA,
    provider,
    partition_key,
    partition_ts,
    _inserted_date,
    _inserted_timestamp,
    {{ dbt_utils.generate_surrogate_key(['id','_inserted_date']) }} AS all_market_data_coingecko_id
FROM
    {{ ref('bronze__streamline_market_data_coingecko') }}
WHERE
    DATA IS NOT NULL

{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        MAX(_inserted_timestamp)
    FROM
        {{ this }}
)
{% endif %}

qualify(ROW_NUMBER() over (PARTITION BY id, _inserted_date
ORDER BY
    _inserted_timestamp DESC)) = 1
