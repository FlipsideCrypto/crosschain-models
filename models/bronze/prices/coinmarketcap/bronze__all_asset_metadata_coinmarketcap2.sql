{{ config(
    materialized = 'incremental',
    unique_key = ['all_asset_metadata_coinmarketcap_id'],
    incremental_strategy = 'delete+insert',
    cluster_by = ['_inserted_timestamp::DATE'],
    tags = ['prices']
) }}

WITH base_sp AS (

    SELECT
        'sp' AS source,
        VALUE,
        provider,
        id :: STRING AS id,
        symbol,
        NAME,
        first_historical_data,
        last_historical_data,
        is_active,
        platform,
        RANK,
        slug,
        _inserted_timestamp
    FROM
        {{ ref('bronze__streamline_asset_metadata_coinmarketcap_sp') }}
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
        id :: STRING AS id,
        symbol,
        NAME,
        first_historical_data,
        last_historical_data,
        is_active,
        platform,
        RANK,
        slug,
        _inserted_timestamp
    FROM
        {{ ref('bronze__streamline_asset_metadata_coinmarketcap') }}
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
        p.this :token_address :: STRING AS token_address,
        NAME,
        symbol,
        p.this :name :: STRING AS platform,
        p.this :id :: STRING AS platform_id,
        p.this :slug :: STRING AS platform_slug,
        p.this :symbol :: STRING AS platform_symbol,
        source,
        p.this AS VALUE,
        COALESCE(first_historical_data,'1970-01-01'::TIMESTAMP) AS first_historical_data,
        COALESCE(last_historical_data,'1970-01-01'::TIMESTAMP) AS last_historical_data,
        is_active,
        RANK,
        slug,
        _inserted_timestamp
    FROM
        all_assets A,
        LATERAL FLATTEN(
            input => VALUE :platform, OUTER => TRUE
        ) p
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
    VALUE,
    first_historical_data,
    last_historical_data,
    is_active,
    RANK,
    slug,
    {{ dbt_utils.generate_surrogate_key(['id','token_address','name','symbol','platform','platform_id']) }} AS all_asset_metadata_coinmarketcap_id,
    _inserted_timestamp
FROM
    FINAL qualify(ROW_NUMBER() over (PARTITION BY id, token_address, NAME, symbol, platform, platform_id
ORDER BY
    _inserted_timestamp DESC)) = 1
