{{ config(
    materialized = 'incremental',
    unique_key = ['id', '_inserted_timestamp'],
    incremental_strategy = 'delete+insert',
    cluster_by = ['_inserted_timestamp::DATE']
) }}

WITH base_sp AS (

    SELECT
        'sp' AS source,
        VALUE,
        provider,
        id,
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
AND _inserted_timestamp > (
    SELECT
        MAX(_inserted_timestamp)
    FROM
        {{ this }}
)
{% endif %}
),
base_streamline AS (
    SELECT
        'streamline' AS source,
        VALUE,
        provider,
        id,
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
)
SELECT
    VALUE,
    provider,
    id,
    symbol,
    NAME,
    first_historical_data,
    last_historical_data,
    is_active,
    platform,
    RANK,
    slug,
    source,
    _inserted_timestamp
FROM
    all_assets qualify(ROW_NUMBER() over (PARTITION BY id
ORDER BY
    _inserted_timestamp DESC)) = 1
