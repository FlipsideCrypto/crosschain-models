{{ config(
    materialized = 'incremental',
    unique_key = ['id','token_address','name','symbol','platform','platform_id'],
    incremental_strategy = 'delete+insert',
    cluster_by = ['_inserted_timestamp::DATE'],
    tags = ['prices']
) }}

WITH base AS (

    SELECT
        id,
        p.value :: STRING AS token_address,
        NAME,
        symbol,
        p.key :: STRING AS platform,
        platform AS platform_id,
        source,
        _inserted_timestamp
    FROM
        {{ ref('bronze__all_asset_metadata_coingecko2') }} A,
        LATERAL FLATTEN(
            input => VALUE :platforms
        ) p

{% if is_incremental() %}
WHERE
    _inserted_timestamp >= (
        SELECT
            MAX(_inserted_timestamp)
        FROM
            {{ this }}
    )
{% endif %}
)
SELECT
    id,
    token_address,
    NAME,
    symbol,
    platform,
    platform_id,
    source,
    _inserted_timestamp
FROM
    base qualify(ROW_NUMBER() over (PARTITION BY id, token_address, NAME, symbol, platform, platform_id
ORDER BY
    _inserted_timestamp DESC)) = 1
