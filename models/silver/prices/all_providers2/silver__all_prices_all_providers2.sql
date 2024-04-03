{{ config(
    materialized = 'incremental',
    unique_key = ['id','recorded_hour'],
    incremental_strategy = 'delete+insert',
    cluster_by = ['_inserted_timestamp::DATE'],
    tags = ['prices']
) }}

WITH coingecko AS (

    SELECT
        id,
        recorded_hour,
        OPEN,
        high,
        low,
        CLOSE,
        'coingecko' AS provider,
        source,
        _inserted_timestamp
    FROM
        {{ ref('bronze__all_prices_coingecko2') }}

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
        OPEN,
        high,
        low,
        CLOSE,
        'coinmarketcap' AS provider,
        source,
        _inserted_timestamp
    FROM
        {{ ref('bronze__all_prices_coinmarketcap2') }}

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
    id,
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
    {{ dbt_utils.generate_surrogate_key(['id','recorded_hour']) }} AS all_prices_all_providers_id,
    '{{ invocation_id }}' AS _invocation_id
FROM
    all_providers qualify(ROW_NUMBER() over(PARTITION BY id, recorded_hour, provider
ORDER BY
    _inserted_timestamp DESC)) = 1
