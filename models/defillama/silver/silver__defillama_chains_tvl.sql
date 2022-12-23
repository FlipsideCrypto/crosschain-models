{{ config(
    materialized = 'incremental',
    unique_key = 'id',
    full_refresh = false
) }}

WITH chain_base AS (

SELECT DISTINCT chain
FROM crosschain_dev.defillama.dim_chains
),

chain_tvl_base AS (

SELECT
    chain,
    ethereum.streamline.udf_api(
        'GET',CONCAT('https://api.llama.fi/charts/',chain),{},{}
    ) AS read,
    SYSDATE() AS _inserted_timestamp
FROM chain_base
{% if is_incremental() %}
WHERE chain NOT IN (
    SELECT
        chain
    FROM (
        SELECT 
            DISTINCT chain,
            MAX(timestamp::DATE) AS max_timestamp
        FROM {{ this }}
        GROUP BY 1
        HAVING CURRENT_DATE = max_timestamp
    ) c
)
{% endif %}
LIMIT 60
)

SELECT
    chain,
    TO_TIMESTAMP(VALUE:date::INTEGER) AS timestamp,
    VALUE:totalLiquidityUSD::INTEGER AS tvl_usd,
    _inserted_timestamp,
     {{ dbt_utils.surrogate_key(
        ['chain', 'timestamp']
    ) }} AS id
FROM chain_tvl_base,
    LATERAL FLATTEN (input=> read:data)