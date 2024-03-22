{{ config(
    materialized = 'incremental',
    unique_key = ['id','token_address','name','symbol','platform','platform_id'],
    incremental_strategy = 'delete+insert',
    cluster_by = ['_inserted_timestamp::DATE'],
    tags = ['prices']
) }}

WITH base_sp AS (

    SELECT
        'sp' AS source,
        VALUE,
        provider,
        id,
        NAME,
        symbol,
        _inserted_timestamp
    FROM
        {{ ref('bronze__streamline_asset_metadata_coingecko_sp') }}
    WHERE
        id IS NOT NULL

{% if is_incremental() %}
AND 1 = 2
{% endif %}
),
base_streamline AS (
    SELECT
        'streamline' AS source,
        VALUE,
        provider,
        id,
        NAME,
        symbol,
        _inserted_timestamp
    FROM
        {{ ref('bronze__streamline_asset_metadata_coingecko') }}
    WHERE
        id IS NOT NULL

{% if is_incremental() %}
AND _inserted_timestamp > (
    SELECT
        MAX(_inserted_timestamp)
    FROM
        {{ this }}
)
{% endif %}
),
all_assets AS (
    SELECT
        *
    FROM
        base_sp
    UNION ALL
    SELECT
        *
    FROM
        base_streamline
),
FINAL AS (
    SELECT
        id,
        p.value :: STRING AS token_address,
        NAME,
        symbol,
        p.key :: STRING AS platform,
        platform AS platform_id,
        source,
        p.value,
        _inserted_timestamp
    FROM
        all_assets A,
        LATERAL FLATTEN(
            input => VALUE :platforms
        ) p
)
SELECT
    id,
    token_address,
    NAME,
    symbol,
    platform,
    platform_id,
    source,
    VALUE,
    _inserted_timestamp
FROM
    FINAL qualify(ROW_NUMBER() over (PARTITION BY id, token_address, NAME, symbol, platform, platform_id
ORDER BY
    _inserted_timestamp DESC)) = 1
