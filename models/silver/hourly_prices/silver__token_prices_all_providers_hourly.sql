{{ config(
    materialized = 'incremental',
    unique_key = "_unique_key",
    incremental_strategy = 'merge',
    cluster_by = ['hour::DATE'],
) }}

WITH all_providers AS (

SELECT
    'coingecko' AS provider,
    recorded_hour AS hour,
    LOWER(token_address) AS token_address,
    symbol,
    platform,
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
    'coinmarketcap' AS provider,
    recorded_hour AS hour,
    lower(token_address) as token_address,
    symbol,
    platform,
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
)

SELECT
    hour,
    token_address,
    COALESCE(c.symbol :: VARIANT, p.symbol) AS symbol,
    platform,
    c.decimals,
    price,
    is_imputed,
    _inserted_timestamp,
    {{ dbt_utils.surrogate_key( ['hour', 'token_address'] ) }} AS _unique_key
FROM final p
LEFT JOIN {{ source('ethereum_core','dim_contracts') }} c 
    ON LOWER(c.address) = LOWER(p.token_address)
QUALIFY(ROW_NUMBER() OVER(PARTITION BY hour, token_address ORDER BY priority ASC)) = 1