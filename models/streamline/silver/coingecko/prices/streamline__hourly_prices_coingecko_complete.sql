{{ config (
    materialized = "incremental",
    unique_key = ['id','recorded_date'],
    cluster_by = "recorded_date::date",
    post_hook = "ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION on equality(id)",
    tags = ['streamline_cg_prices_complete']
) }}

WITH backfill AS (

    SELECT
        id,
        run_time :: DATE AS recorded_date,
        _inserted_timestamp
    FROM
        {{ ref(
            'bronze__streamline_hourly_prices_coingecko_backfill'
        ) }}

{% if is_incremental() %}
WHERE
    _inserted_timestamp >= (
        SELECT
            MAX(_inserted_timestamp)
        FROM
            {{ this }}
    )
{% endif %}
),
history AS (
    SELECT
        id,
        run_time :: DATE AS recorded_date,
        _inserted_timestamp
    FROM
        {{ ref(
            'bronze__streamline_hourly_prices_coingecko_history'
        ) }}

{% if is_incremental() %}
WHERE
    _inserted_timestamp >= (
        SELECT
            MAX(_inserted_timestamp)
        FROM
            {{ this }}
    )
{% endif %}
),
realtime AS (
    SELECT
        id,
        (
            (
                CASE
                    WHEN TO_TIMESTAMP(
                        f.value [0] :: STRING
                    ) = DATE_TRUNC(
                        'hour',
                        TO_TIMESTAMP(
                            f.value [0] :: STRING
                        )
                    ) THEN TO_TIMESTAMP(
                        f.value [0] :: STRING
                    )
                    ELSE NULL
                END
            ) :: STRING
        ) :: DATE AS recorded_date,
        _inserted_timestamp
    FROM
        {{ ref('bronze__streamline_hourly_prices_coingecko_realtime') }}
        s,
        LATERAL FLATTEN(
            input => DATA
        ) f
    WHERE
        recorded_date IS NOT NULL

{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        MAX(_inserted_timestamp)
    FROM
        {{ this }}
)
{% endif %}
),
all_prices AS (
    SELECT
        *
    FROM
        backfill
    UNION ALL
    SELECT
        *
    FROM
        history
    UNION ALL
    SELECT
        *
    FROM
        realtime
)
SELECT
    id,
    recorded_date,
    {{ dbt_utils.generate_surrogate_key(['id','recorded_date']) }} AS hourly_prices_coingecko_complete_id,
    _inserted_timestamp
FROM
    all_prices qualify(ROW_NUMBER() over (PARTITION BY id, recorded_date
ORDER BY
    _inserted_timestamp DESC)) = 1
