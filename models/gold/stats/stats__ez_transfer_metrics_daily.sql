-- depends_on: {{ ref('silver__transfers') }}
-- depends_on: {{ ref('price__ez_prices_hourly') }}
-- depends_on: {{ ref('silver__native_fee_token') }}
{{ config(
    materialized = 'incremental',
    incremental_strategy = 'merge',
    merge_exclude_columns = ["inserted_timestamp"],
    unique_key = ['blockchain','block_date'],
    cluster_by = ['blockchain','block_date'],
    tags = ['metrics_daily']
) }}

{% if execute %}

{% if is_incremental() %}
{% set max_mod_query %}

SELECT
    MAX(modified_timestamp) modified_timestamp
FROM
    {{ this }}

    {% endset %}
    {% set max_mod = run_query(max_mod_query) [0] [0] %}
    {% if not max_mod or max_mod == 'None' %}
        {% set max_mod = '2099-01-01' %}
    {% endif %}
{% endif %}

--get the distinct blockchains & block dates that we are processing
{% set dates_query %}
CREATE
OR REPLACE temporary TABLE silver.ez_transfer_metrics__intermediate_tmp AS
SELECT
    DISTINCT blockchain,
    block_timestamp :: DATE AS block_date
FROM
    {{ ref('silver__transfers') }}
WHERE
    block_timestamp :: DATE < SYSDATE() :: DATE

{% if is_incremental() %}
AND modified_timestamp >= '{{ max_mod }}'
{% else %}
    AND block_timestamp :: DATE >= '2025-01-01'
{% endif %}

{% endset %}
{% do run_query(dates_query) %}
--create a dynamic where clause with literal block dates
{% set date_query %}
SELECT
    DISTINCT block_date
FROM
    silver.ez_transfer_metrics__intermediate_tmp {% endset %}
    {% set date_results = run_query(date_query) %}
    {% set date_filter %}
    A.block_timestamp :: DATE IN ({% if date_results.rows | length > 0 %}
        {% for date in date_results %}
            '{{ date[0] }}' {% if not loop.last %},
            {% endif %}
        {% endfor %}
    {% else %}
        '2099-01-01'
    {% endif %}) {% endset %}
    --roll transactions up to the hour/sender level
    {% set inc_query %}
    CREATE
    OR REPLACE temporary TABLE silver.ez_transfer_metrics__xfer_intermediate_tmp AS
SELECT
    A.blockchain,
    {# A.block_timestamp :: DATE AS block_date, #}
    DATE_TRUNC(
        'hour',
        A.block_timestamp
    ) AS block_timestamp_hour,
    A.from_address,
    SUM(
        A.amount_usd
    ) AS amount_usd,
    SUM(amount) AS amount
FROM
    {{ ref('silver__transfers') }} A
    JOIN silver.ez_transfer_metrics__intermediate_tmp b
    ON A.blockchain = b.blockchain
    AND A.block_timestamp :: DATE = b.block_date
WHERE
    {{ date_filter }}
    AND A.token_is_verified
GROUP BY
    A.blockchain,
    block_timestamp_hour,
    from_address {% endset %}
    {% do run_query(inc_query) %}
    --find distinct score dates
    {% set score_dates_query %}
    CREATE
    OR REPLACE temporary TABLE silver.ez_transfer_metrics__score_dates_intermediate_tmp AS
SELECT
    DISTINCT A.blockchain,
    A.score_date
FROM
    {{ source(
        'datascience_onchain_scores',
        'all_scores'
    ) }} A {% endset %}
    {% do run_query(score_dates_query) %}
    --find block dates where we do not have a score for that exact date
    {% set score_asof_query %}
    CREATE
    OR REPLACE temporary TABLE silver.ez_transfer_metrics__scores_asof_intermediate_tmp AS
SELECT
    DISTINCT A.blockchain,
    A.block_date,
    b.score_date
FROM
    ez_transfer_metrics__intermediate_tmp A asof
    JOIN silver.ez_transfer_metrics__score_dates_intermediate_tmp b match_condition (
        A.block_date >= score_date
    )
    ON A.blockchain = b.blockchain qualify ROW_NUMBER() over (
        PARTITION BY A.blockchain,
        A.block_Date
        ORDER BY
            ABS(DATEDIFF('day', score_date, A.block_date))
    ) = 1 {% endset %}
    {% do run_query(score_asof_query) %}
    --Get the score for that block date or the closest date we have prior to that date
    {% set scores_query %}
    CREATE
    OR REPLACE temporary TABLE silver.ez_transfer_metrics__scores_intermediate_tmp AS
SELECT
    A.blockchain,
    A.user_address,
    b.block_date,
    A.total_score
FROM
    {{ source(
        'datascience_onchain_scores',
        'all_scores'
    ) }} A
    JOIN silver.ez_transfer_metrics__scores_asof_intermediate_tmp b
    ON A.blockchain = b.blockchain
    AND A.score_date = b.score_date {% endset %}
    {% do run_query(scores_query) %}
    --delete the scores temp with a score less than 4 or the additional rows from the asof join
    {% set scores_del_query %}
DELETE FROM
    silver.ez_transfer_metrics__scores_intermediate_tmp
WHERE
    total_score < 4 {% endset %}
    {% do run_query(scores_del_query) %}
{% endif %}

--Final aggregate of the data to the daily level
WITH prices AS (
    SELECT
        A.hour,
        b.blockchain,
        A.price
    FROM
        {{ ref('price__ez_prices_hourly') }} A
        JOIN {{ ref('silver__native_fee_token') }}
        b
        ON A.blockchain = COALESCE(
            b.blockchain_override,
            b.blockchain
        )
        AND COALESCE(
            A.token_address,
            ''
        ) = COALESCE(
            b.address,
            ''
        )
        AND A.symbol = b.symbol
    WHERE
        --three day look back to make sure we fill any gaps
        HOUR :: DATE >= (
            SELECT
                MIN(block_date) -3
            FROM
                silver.ez_transfer_metrics__intermediate_tmp
        )
)
SELECT
    A.blockchain,
    A.block_timestamp_hour :: DATE AS block_date,
    SUM(amount_usd) AS total_transfer_volume_usd,
    SUM(
        amount_usd / price
    ) AS in_unit_total_transfer_volume,
    SUM(
        CASE
            WHEN C.blockchain IS NOT NULL THEN amount_usd
            ELSE 0
        END
    ) AS quality_total_transfer_volume_usd,
    SUM(
        CASE
            WHEN C.blockchain IS NOT NULL THEN amount_usd / price
            ELSE 0
        END
    ) AS in_unit_quality_total_transfer_volume,
    {{ dbt_utils.generate_surrogate_key(['a.blockchain','A.block_timestamp_hour :: DATE']) }} AS ez_transfer_metrics_daily_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp
FROM
    silver.ez_transfer_metrics__xfer_intermediate_tmp A asof
    JOIN prices b match_condition (
        A.block_timestamp_hour >= b.hour
    )
    ON A.blockchain = b.blockchain
    LEFT JOIN silver.ez_transfer_metrics__scores_intermediate_tmp C
    ON A.blockchain = C.blockchain
    AND A.from_address = C.user_address
    AND A.block_timestamp_hour :: DATE = C.block_date
GROUP BY
    A.blockchain,
    A.block_timestamp_hour :: DATE
