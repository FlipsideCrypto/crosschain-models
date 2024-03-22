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
        p.this :token_address :: STRING AS token_address,
        NAME,
        symbol,
        p.this :name :: STRING AS platform,
        p.this :id :: STRING AS platform_id,
        p.this :slug :: STRING AS platform_slug,
        p.this :symbol :: STRING AS platform_symbol,
        source,
        _inserted_timestamp
    FROM
        {{ ref('bronze__all_asset_metadata_coinmarketcap2') }} A,
        LATERAL FLATTEN(
            input => VALUE :platform
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
    platform_slug,
    platform_symbol,
    source,
    _inserted_timestamp
FROM
    base qualify(ROW_NUMBER() over (PARTITION BY id, token_address, NAME, symbol, platform, platform_id
ORDER BY
    _inserted_timestamp DESC)) = 1
