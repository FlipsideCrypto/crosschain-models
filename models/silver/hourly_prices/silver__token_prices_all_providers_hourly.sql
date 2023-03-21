{{ config(
    materialized = 'incremental',
    unique_key = "_unique_key",
    incremental_strategy = 'merge',
    cluster_by = ['hour::DATE'],
) }}

WITH all_providers AS (

SELECT
    recorded_hour AS hour,
    LOWER(token_address) AS token_address,
    LOWER(REGEXP_REPLACE(platform,'[^a-zA-Z0-9/-]+')) AS platform,
    'coingecko' AS provider,
    close AS price,
    imputed AS is_imputed,
    _inserted_timestamp
FROM
    {{ ref('silver__token_prices_coin_gecko_hourly') }} 

{% if is_incremental() %}
WHERE _inserted_timestamp > (
    SELECT
        MAX(_inserted_timestamp)
    FROM
        {{ this }}
    )
{% endif %}

UNION ALL

SELECT
    recorded_hour AS hour,
    LOWER(token_address) AS token_address,
    LOWER(REGEXP_REPLACE(platform,'[^a-zA-Z0-9/-]+')) AS platform,
    'coinmarketcap' AS provider,
    close AS price,
    imputed AS is_imputed,
    _inserted_timestamp
FROM
    {{ ref('silver__token_prices_coin_market_cap_hourly')}} 

{% if is_incremental() %}
WHERE _inserted_timestamp > (
    SELECT
        MAX(_inserted_timestamp)
    FROM
        {{ this }}
    )
{% endif %}
),

FINAL AS (
SELECT
    hour,
    token_address,
    CASE 
        WHEN platform IN ('arbitrum-nova','arbitrum-one') THEN 'arbitrum'
        WHEN platform IN ('avalanche') THEN 'avalanche'
        WHEN platform IN ('binance-smart-chain','binancecoin','bnb') THEN 'bsc'
        WHEN platform IN ('ethereum','ethereum-classic','ethereumclassic','ethereumpow') THEN 'ethereum'
        WHEN platform IN ('gnosis','xdai') THEN 'gnosis'
        WHEN platform IN ('optimism','optimistic-ethereum') THEN 'optimism'
        WHEN platform IN ('polygon','polygon-pos') THEN 'polygon'
        ELSE NULL
    END AS blockchain,
    provider,
    price,
    is_imputed,
    _inserted_timestamp,
    {{ dbt_utils.surrogate_key( ['hour','token_address','blockchain','provider'] ) }} AS _unique_key
FROM all_providers p
WHERE blockchain IS NOT NULL
)

SELECT
    hour,
    token_address,
    blockchain,
    provider,
    price,
    is_imputed,
    _inserted_timestamp,
    _unique_key
FROM FINAL
    QUALIFY(ROW_NUMBER() OVER (PARTITION BY hour, token_address, blockchain, provider 
ORDER BY _inserted_timestamp DESC)) = 1