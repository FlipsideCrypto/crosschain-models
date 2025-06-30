{{ config(
    materialized = 'incremental',
    unique_key = 'complete_provider_prices_id',
    incremental_strategy = 'delete+insert',
    cluster_by = ['recorded_hour::DATE','provider'],
    post_hook = "ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION ON EQUALITY(asset_id),SUBSTRING(asset_id)",
    tags = ['prices']
) }}

WITH coingecko AS (

    SELECT
        id,
        recorded_hour,
        CASE WHEN p.OPEN < 0 THEN 0 ELSE p.OPEN END AS OPEN,
        CASE WHEN p.high < 0 THEN 0 ELSE p.high END AS high,
        CASE WHEN p.low < 0 THEN 0 ELSE p.low END AS low,
        CASE WHEN p.CLOSE < 0 THEN 0 ELSE p.CLOSE END AS CLOSE,
        'coingecko' AS provider,
        source,
        _inserted_timestamp
    FROM
        {{ ref('bronze__all_prices_coingecko') }} p

{% if is_incremental() %}
WHERE _inserted_timestamp >= (
    SELECT
        MAX(_inserted_timestamp)
    FROM
        {{ this }}
)
{% endif %}
),
coinmarketcap AS (
    SELECT
        id,
        recorded_hour,
        CASE WHEN p.OPEN < 0 THEN 0 ELSE p.OPEN END AS OPEN,
        CASE WHEN p.high < 0 THEN 0 ELSE p.high END AS high,
        CASE WHEN p.low < 0 THEN 0 ELSE p.low END AS low,
        CASE WHEN p.CLOSE < 0 THEN 0 ELSE p.CLOSE END AS CLOSE,
        'coinmarketcap' AS provider,
        source,
        _inserted_timestamp
    FROM
        {{ ref('bronze__all_prices_coinmarketcap') }} p

{% if is_incremental() %}
WHERE _inserted_timestamp >= (
    SELECT
        MAX(_inserted_timestamp)
    FROM
        {{ this }}
)
{% endif %}
),
all_providers AS (
    SELECT
        *
    FROM
        coingecko
    UNION ALL
    SELECT
        *
    FROM
        coinmarketcap
)
SELECT
    id AS asset_id,
    recorded_hour,
    OPEN,
    high,
    low,
    CLOSE,
    provider,
    source,
    _inserted_timestamp,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    {{ dbt_utils.generate_surrogate_key(['asset_id','recorded_hour','provider']) }} AS complete_provider_prices_id,
    '{{ invocation_id }}' AS _invocation_id
FROM
    all_providers qualify(ROW_NUMBER() over(PARTITION BY asset_id, recorded_hour, provider
ORDER BY
    _inserted_timestamp DESC)) = 1
