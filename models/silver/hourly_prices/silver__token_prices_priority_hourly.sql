WITH all_providers AS (
    
SELECT
    hour,
    token_address,
    symbol,
    blockchain,
    provider,
    decimals,
    price,
    is_imputed,
    _inserted_timestamp
FROM {{ ref('silver__token_prices_all_providers_hourly') }}
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
    symbol,
    blockchain,
    provider,
    price,
    is_imputed,
    _inserted_timestamp,
    CASE
        WHEN provider = 'coingecko' AND is_imputed = FALSE THEN 1
        WHEN provider = 'coinmarketcap' AND is_imputed = FALSE THEN 2
        WHEN provider = 'coingecko' AND is_imputed = TRUE THEN 3
        WHEN provider = 'coinmarketcap' AND is_imputed = TRUE THEN 4
    END AS priority
FROM all_providers
)

SELECT
    hour,
    token_address,
    symbol,
    price,
    blockchain,
    is_imputed,
    _inserted_timestamp,
    {{ dbt_utils.surrogate_key( ['hour', 'token_address', 'symbol', 'blockchain'] ) }} AS _unique_key
FROM FINAL
QUALIFY(ROW_NUMBER() OVER(PARTITION BY hour, token_address, symbol, blockchain 
    ORDER BY priority ASC)) = 1

