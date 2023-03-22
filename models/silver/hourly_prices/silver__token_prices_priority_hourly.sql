{{ config(
    materialized = 'incremental',
    unique_key = "_unique_key",
    incremental_strategy = 'merge',
    cluster_by = ['hour::DATE'],
) }}

WITH all_providers AS (
    
SELECT
    hour,
    token_address,
    blockchain,
    provider,
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

all_prices AS (

SELECT 
    hour,
    token_address,
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
),

final_priority AS (
SELECT
    hour,
    token_address,
    price,
    blockchain,
    is_imputed,
    _inserted_timestamp,
    {{ dbt_utils.surrogate_key( ['hour', 'token_address', 'blockchain'] ) }} AS _unique_key
FROM all_prices
QUALIFY(ROW_NUMBER() OVER (PARTITION BY hour, token_address, blockchain 
    ORDER BY priority ASC)) = 1
),

asset_metadata AS (

    SELECT 
        token_address,
        symbol,
        decimals,
        blockchain,
        provider
    FROM {{ ref('silver__asset_metadata_all_providers') }}
    {% if is_incremental() %}
    WHERE CONCAT(token_address,'-',symbol,'-',blockchain) NOT IN (
        SELECT
            CONCAT(token_address,'-',symbol,'-',blockchain)
        FROM
            {{ this }}
        )
    {% endif %}
),

SELECT 
    hour,
    p.token_address,
    COALESCE(cg.symbol,cmc.symbol) AS symbol,
    COALESCE(cg.decimals,cmc.decimals) AS decimals,
    price,
    p.blockchain,
    is_imputed,
    _inserted_timestamp,
    _unique_key
FROM final_priority p
LEFT JOIN (
    SELECT *
    FROM asset_metadata
    WHERE provider = 'coingecko'
        ) cg 
    ON p.token_address = cg.token_address AND p.blockchain = cg.blockchain
LEFT JOIN (
    SELECT *
    FROM asset_metadata
    WHERE provider = 'coinmarketcap'
        ) cmc 
    ON p.token_address = cmc.token_address AND p.blockchain = cmc.blockchain