{{ config(
    materialized = 'incremental',
    unique_key = 'dex_slug'
) }}

WITH base AS (

SELECT
    ethereum.streamline.udf_api(
        'GET','https://api.llama.fi/overview/dexs?excludeTotalDataChart=true&excludeTotalDataChartBreakdown=true&dataType=totalVolume',{},{}
    ) AS dex_read,
    ethereum.streamline.udf_api(
        'GET','https://api.llama.fi/overview/options?excludeTotalDataChart=true&excludeTotalDataChartBreakdown=true&dataType=totalPremiumVolume',{},{}
    ) AS options_read,
    SYSDATE() AS _inserted_timestamp
)
    
SELECT
    VALUE:name::STRING AS dex,
    VALUE:module::STRING AS dex_slug,
    VALUE:category::STRING AS category,
    VALUE:chains AS chains,
    _inserted_timestamp
FROM base,
    LATERAL FLATTEN (input=> dex_read:data:protocols)
{% if is_incremental() %}
WHERE dex_slug NOT IN (
    SELECT
        DISTINCT dex_slug
    FROM
        {{ this }}
)
{% endif %}
UNION
SELECT
    VALUE:name::STRING AS dex,
    VALUE:module::STRING AS dex_slug,
    VALUE:category::STRING AS category,
    VALUE:chains AS chains,
    _inserted_timestamp
FROM base,
    LATERAL FLATTEN (input=> options_read:data:protocols)

{% if is_incremental() %}
WHERE dex_slug NOT IN (
    SELECT
        DISTINCT dex_slug
    FROM
        {{ this }}
)
{% endif %}