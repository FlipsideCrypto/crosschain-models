{{ config(
    materialized = 'incremental',
    unique_key = ['native_asset_metadata_priority_id'],
    incremental_strategy = 'delete+insert',
    post_hook = "ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION on equality(symbol, blockchain)",
    tags = ['prices']
) }}

WITH all_providers AS (
    SELECT
        id,
        symbol,
        name,
        decimals,
        blockchain,
        provider,
        CASE
            WHEN provider = 'coingecko' THEN 1
            WHEN provider = 'coinmarketcap' THEN 2
        END AS priority,
        source,
        is_deprecated,
        _inserted_timestamp
    FROM
        {{ ref('silver__native_asset_metadata_all_providers') }}

{% if is_incremental() %}
WHERE
    _inserted_timestamp >= (
        SELECT
            MAX(_inserted_timestamp) - INTERVAL '4 hours'
        FROM
            {{ this }}
    )
    OR symbol NOT IN (
        SELECT
            DISTINCT symbol
        FROM
            {{ this }}
    )  --load all data for new assets
{% endif %}
)
SELECT
    id,
    symbol,
    name,
    decimals,
    blockchain,
    provider,
    priority,
    source,
    is_deprecated,
    _inserted_timestamp,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    {{ dbt_utils.generate_surrogate_key(['symbol']) }} AS native_asset_metadata_priority_id,
    '{{ invocation_id }}' AS _invocation_id
FROM
    all_providers qualify(ROW_NUMBER() over (PARTITION BY symbol
ORDER BY
    _inserted_timestamp DESC, priority ASC, blockchain ASC, id ASC)) = 1 -- select the last inserted record (latest supported provider), then by priority etc.
