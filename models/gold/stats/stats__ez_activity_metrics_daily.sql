-- depends_on: {{ ref('silver__fact_transactions_lite') }}
-- depends_on: {{ ref('price__ez_prices_hourly') }}
-- depends_on: {{ ref('silver__native_fee_token') }}
{{ config(
    materialized = 'incremental',
    incremental_strategy = 'merge',
    merge_exclude_columns = ["inserted_timestamp"],
    unique_key = ['blockchain','block_date','is_quality']
) }}

{% if execute %}

{% if is_incremental() %}
{% set max_mod_query %}

SELECT
    MAX(modified_timestamp) :: DATE modified_timestamp
FROM
    {{ this }}

    {% endset %}
    {% set max_mod = run_query(max_mod_query) [0] [0] %}
    {% if not max_mod or max_mod == 'None' %}
        {% set max_mod = '2099-01-01' %}
    {% endif %}
{% endif %}

--get the distinct block dates that we are processing
{% set dates_query %}
CREATE
OR REPLACE temporary TABLE silver.ez_activity_metrics__intermediate_tmp AS
SELECT
    DISTINCT blockchain,
    block_timestamp :: DATE AS block_date
FROM
    {{ ref('silver__fact_transactions_lite') }}
WHERE
    block_timestamp :: DATE < SYSDATE() :: DATE

{% if is_incremental() %}
AND modified_timestamp :: DATE >= '{{ max_mod }}'
{% else %}
    AND block_timestamp :: DATE >= '2025-05-14'
{% endif %}

{% endset %}
{% do run_query(dates_query) %}
--apply prices to the transactions and roll up to the user/day level
{% set inc_query %}
CREATE
OR REPLACE temporary TABLE silver.ez_activity_metrics__tx_intermediate_tmp AS WITH prices AS (
    SELECT
        A.hour,
        A.blockchain,
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
        HOUR :: DATE >= (
            SELECT
                MIN(block_date) -3
            FROM
                silver.ez_activity_metrics__intermediate_tmp
        )
)
SELECT
    A.blockchain,
    block_timestamp :: DATE AS block_date,
    sender,
    COUNT(1) AS transaction_count,
    SUM(
        CASE
            WHEN tx_succeeded THEN 1
            ELSE 0
        END
    ) AS transaction_count_success,
    SUM(
        CASE
            WHEN NOT tx_succeeded THEN 1
            ELSE 0
        END
    ) AS transaction_count_failed,
    SUM(fee_native) AS total_fees_native,
    SUM(
        fee_native * price
    ) AS total_fees_usd
FROM
    {{ ref('silver__fact_transactions_lite') }} A
    JOIN silver.ez_activity_metrics__intermediate_tmp b
    ON A.blockchain = b.blockchain
    AND A.block_timestamp :: DATE = b.block_date asof
    JOIN prices C match_condition (
        A.block_timestamp >= C.hour
    )
    ON A.blockchain = C.blockchain
GROUP BY
    1,
    2,
    3 {% endset %}
    {% do run_query(inc_query) %}
    --Get the score closest to the block date for the senders
    {% set scores_query %}
    CREATE
    OR REPLACE temporary TABLE silver.ez_activity_metrics__scores_intermediate_tmp AS
SELECT
    A.blockchain,
    A.user_address,
    A.score_date,
    b.block_Date,
    A.total_score,
    ROW_NUMBER() over (
        PARTITION BY A.blockchain,
        A.user_address,
        b.block_Date
        ORDER BY
            score_date DESC
    ) AS rn
FROM
    {{ source(
        'datascience_onchain_scores',
        'all_scores'
    ) }} A asof
    JOIN silver.ez_activity_metrics__intermediate_tmp b match_condition (
        score_date <= b.block_date
    )
    ON A.blockchain = b.blockchain
WHERE
    score_date >= (
        SELECT
            MIN(block_date) -7
        FROM
            silver.ez_activity_metrics__intermediate_tmp
    )
    AND total_score >= 4 {% endset %}
    {% do run_query(scores_query) %}
{% endif %}

--Aggregate the data to the daily level with a quality flag
SELECT
    A.blockchain,
    A.block_date,
    CASE
        WHEN C.blockchain IS NOT NULL THEN TRUE
        ELSE FALSE
    END AS is_quality,
    SUM(transaction_count) AS transaction_count,
    SUM(
        transaction_count_success
    ) AS transaction_count_success,
    SUM(
        transaction_count_failed
    ) AS transaction_count_failed,
    COUNT(1) AS unique_initiator_count,
    SUM(total_fees_native) AS total_fees_native,
    SUM(total_fees_usd) AS total_fees_usd,
    {{ dbt_utils.generate_surrogate_key(['a.blockchain','a.block_date','is_quality']) }} AS ez_activity_metrics_daily_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    silver.ez_activity_metrics__tx_intermediate_tmp A
    LEFT JOIN silver.ez_activity_metrics__scores_intermediate_tmp C
    ON A.blockchain = C.blockchain
    AND A.sender = C.user_address
    AND A.block_date = C.block_date
    AND C.rn = 1
GROUP BY
    A.blockchain,
    A.block_date,
    is_quality
