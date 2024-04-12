{{ config(
    materialized = 'incremental',
    unique_key = ['all_asset_metadata_coingecko_id'],
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
AND _inserted_timestamp >= (
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
        CASE
            WHEN p.value :: STRING = 'null' THEN NULL
            ELSE p.value :: STRING
        END AS token_address,
        NAME,
        symbol,
        CASE
            WHEN p.key :: STRING = 'null' THEN NULL
            ELSE p.key :: STRING
        END AS platform,
        platform AS platform_id,
        source,
        CASE
            WHEN p.value = 'null' THEN NULL
            ELSE p.value
        END AS VALUE,
        _inserted_timestamp
    FROM
        all_assets A,
        LATERAL FLATTEN(
            input => VALUE :platforms, OUTER => TRUE
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
    {{ dbt_utils.generate_surrogate_key(['id','token_address','name','symbol','platform','platform_id']) }} AS all_asset_metadata_coingecko_id,
    _inserted_timestamp
FROM
    FINAL qualify(ROW_NUMBER() over (PARTITION BY id, token_address, NAME, symbol, platform, platform_id
ORDER BY
    _inserted_timestamp DESC)) = 1
