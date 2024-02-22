{{ config (
    materialized = "incremental",
    unique_key = "hourly_prices_coin_gecko_complete_id",
    cluster_by = "run_time::date",
    post_hook = "ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION on equality(id)",
    tags = ['streamline_prices_complete2']
) }}

WITH backfill AS (

    SELECT
        id,
        run_time,
        metadata,
        DATA,
        error,
        {{ dbt_utils.generate_surrogate_key(['id','run_time']) }} AS hourly_prices_coin_gecko_complete_id,
        metadata$file_last_modified AS _inserted_at
    FROM
        {{ ref(
            'bronze__streamline_hourly_prices_coingecko_backfill'
        ) }}

{% if is_incremental() %}
WHERE
    _inserted_at >= COALESCE(
        (
            SELECT
                MAX(_inserted_at) AS _inserted_at
            FROM
                {{ this }}
        ),
        '1900-01-01' :: TIMESTAMP_NTZ
    )
{% endif %}
),
history AS (
    SELECT
        id,
        run_time,
        metadata,
        DATA,
        error,
        {{ dbt_utils.generate_surrogate_key(['id','run_time']) }} AS hourly_prices_coin_gecko_complete_id,
        metadata$file_last_modified AS _inserted_at
    FROM
        {{ ref(
            'bronze__streamline_hourly_prices_coingecko_history'
        ) }}

{% if is_incremental() %}
WHERE
    _inserted_at >= COALESCE(
        (
            SELECT
                MAX(_inserted_at) AS _inserted_at
            FROM
                {{ this }}
        ),
        '1900-01-01' :: TIMESTAMP_NTZ
    )
{% endif %}
),
all_historical_prices AS (
    SELECT
        *
    FROM
        backfill
    UNION ALL
    SELECT
        *
    FROM
        history
)
SELECT
    id,
    run_time,
    metadata,
    DATA,
    error,
    hourly_prices_coin_gecko_complete_id,
    _inserted_at
FROM
    all_historical_prices
-- `complete` model includes only assets from `history` data as `realtime` does not require `complete`