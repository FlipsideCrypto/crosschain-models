{{ config(
    materialized = 'incremental',
    unique_key = "_unique_key",
    incremental_strategy = 'merge',
    cluster_by = ['hour::DATE'],
    post_hook = "ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION on equality(token_address, hour, blockchain)"
) }}

WITH all_providers AS (

    SELECT
        HOUR,
        token_address,
        blockchain,
        provider,
        price,
        is_imputed,
        _inserted_timestamp
    FROM
        {{ ref('silver__token_prices_all_providers_hourly') }}

{% if is_incremental() %}
WHERE
    _inserted_timestamp >= (
        SELECT
            MAX(_inserted_timestamp) :: DATE - 1
        FROM
            {{ this }}
    )
{% endif %}
),
all_prices AS (
    SELECT
        HOUR,
        token_address,
        blockchain,
        provider,
        price,
        is_imputed,
        _inserted_timestamp,
        CASE
            WHEN provider = 'coingecko'
            AND is_imputed = FALSE THEN 1
            WHEN provider = 'coinmarketcap'
            AND is_imputed = FALSE THEN 2
            WHEN provider = 'coingecko'
            AND is_imputed = TRUE THEN 3
            WHEN provider = 'coinmarketcap'
            AND is_imputed = TRUE THEN 4
            WHEN provider = 'pool balance' THEN 5
            WHEN provider = 'swap' THEN 6
        END AS priority
    FROM
        all_providers
)
SELECT
    HOUR,
    CASE
        WHEN token_address ILIKE 'ibc%' THEN token_address
        ELSE LOWER(token_address)
    END AS token_address,
    price,
    blockchain,
    is_imputed,
    _inserted_timestamp,
    {{ dbt_utils.surrogate_key(
        ['hour', 'token_address', 'blockchain']
    ) }} AS _unique_key
FROM
    all_prices qualify(ROW_NUMBER() over (PARTITION BY HOUR, token_address, blockchain
ORDER BY
    priority ASC)) = 1
