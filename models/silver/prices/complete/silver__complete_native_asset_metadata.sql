{{ config(
    materialized = 'incremental',
    unique_key = ['complete_native_asset_metadata_id'],
    incremental_strategy = 'delete+insert',
    post_hook = "ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION on equality(symbol, blockchain)",
    tags = ['prices']
) }}

WITH base_assets AS (

    SELECT
        id AS asset_id,
        UPPER(symbol) AS symbol_adj,
        NAME,
        decimals,
        blockchain,
        is_deprecated,
        provider,
        source,
        _inserted_timestamp
    FROM
        {{ ref('silver__native_asset_metadata_priority') }}

{% if is_incremental() %}
WHERE
    _inserted_timestamp >= (
        SELECT
            MAX(_inserted_timestamp)
        FROM
            {{ this }}
    )
    OR symbol_adj NOT IN (
        SELECT
            DISTINCT symbol
        FROM
            {{ this }}
    ) --load all data for new assets
{% endif %}
)
SELECT
    asset_id,
    symbol_adj AS symbol,
    NAME,
    decimals,
    blockchain,
    is_deprecated,
    provider,
    source,
    _inserted_timestamp,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    {{ dbt_utils.generate_surrogate_key(['symbol']) }} AS complete_native_asset_metadata_id,
    '{{ invocation_id }}' AS _invocation_id
FROM
    base_assets
