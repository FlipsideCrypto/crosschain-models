{{ config(
    materialized = 'incremental',
    unique_key = 'stablecoin_id'
) }}

WITH stablecoin_base AS (

SELECT
    ethereum.streamline.udf_api(
        'GET','https://stablecoins.llama.fi/stablecoins?includePrices=false',{},{}
    ) AS read,
    SYSDATE() AS _inserted_timestamp
)

SELECT
    VALUE:id::STRING AS stablecoin_id,
    VALUE:name::STRING AS stablecoin,
    VALUE:symbol::STRING AS symbol,
    VALUE:pegType::STRING AS peg_type,
    VALUE:pegMechanism::STRING AS peg_mechanism,
    VALUE:priceSource::STRING AS price_source,
    VALUE:chains AS chains,
    _inserted_timestamp
FROM stablecoin_base,
    LATERAL FLATTEN (input=> read:data:peggedAssets)
    
{% if is_incremental() %}
WHERE stablecoin_id NOT IN (
    SELECT
        DISTINCT stablecoin_id
    FROM
        {{ this }}
)
{% endif %}