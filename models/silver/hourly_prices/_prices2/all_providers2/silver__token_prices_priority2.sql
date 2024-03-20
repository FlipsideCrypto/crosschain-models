{{ config(
    materialized = 'incremental',
    unique_key = ['hour','token_address','blockchain_id'],
    incremental_strategy = 'delete+insert',
    cluster_by = ['hour::DATE'],
    post_hook = "ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION on equality(token_address, hour, blockchain)",
    tags = ['prices']
) }}

WITH all_providers AS (

    SELECT
        HOUR,
        token_address,
        blockchain,
        blockchain_id,
        price,
        is_imputed,
        id,
        provider,
        CASE
            WHEN provider = 'coingecko'
            AND is_imputed = FALSE THEN 1
            WHEN provider = 'coinmarketcap'
            AND is_imputed = FALSE THEN 2
            WHEN provider = 'coingecko'
            AND is_imputed = TRUE THEN 3
            WHEN provider = 'coinmarketcap'
            AND is_imputed = TRUE THEN 4
            WHEN provider = 'osmosis-pool-balance' THEN 5
            WHEN provider = 'osmosis-swap' THEN 6
        END AS priority,
        source,
        _inserted_timestamp
    FROM
        {{ ref('silver__token_prices_all_providers2') }}

{% if is_incremental() %}
WHERE
    _inserted_timestamp >= (
        SELECT
            MAX(_inserted_timestamp) - INTERVAL '4 hours'
        FROM
            {{ this }}
    )
{% endif %}
)
SELECT
    HOUR,
    token_address,
    blockchain,
    blockchain_id,
    price,
    is_imputed,
    id,
    provider,
    priority,
    source,
    _inserted_timestamp,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    {{ dbt_utils.generate_surrogate_key(['hour','token_address','blockchain_id']) }} AS token_prices_priority_hourly_id,
    '{{ invocation_id }}' AS _invocation_id
FROM
    all_providers qualify(ROW_NUMBER() over (PARTITION BY HOUR, token_address, blockchain_id
ORDER BY
    priority ASC)) = 1
