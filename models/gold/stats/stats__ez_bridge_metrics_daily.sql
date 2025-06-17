-- depends_on: {{ ref('silver__fact_transactions_lite') }}
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
OR REPLACE temporary TABLE silver.ez_activity_bridge__intermediate_tmp AS
SELECT
    DISTINCT blockchain,
    block_timestamp :: DATE AS block_date
FROM
    {{ ref('defi__ez_bridge_activity') }}
WHERE
    block_timestamp :: DATE < SYSDATE() :: DATE

{% if is_incremental() %}
AND modified_timestamp >= '{{ max_mod }}'
{% else %}
    AND block_timestamp :: DATE >= '2025-01-01'
    AND block_timestamp :: DATE <= '2025-01-10'
{% endif %}

{% endset %}
{% do run_query(dates_query) %}
--create a dynamic where clause with literal block dates
{% set date_query %}
SELECT
    DISTINCT block_date
FROM
    silver.ez_activity_bridge__intermediate_tmp {% endset %}
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
    {# {% set inc_query %}
    CREATE
    OR REPLACE temporary TABLE silver.ez_activity_metrics__intermediate_tmp AS #}
    WITH roll AS (
        SELECT
            A.blockchain,
            block_timestamp :: DATE AS block_date,
            SUM(CASE
                WHEN direction = 'inbound' THEN amount_usd
                ELSE 0
            END) AS  total_inbound_volume,
                 count( distinct CASE
                WHEN direction = 'inbound' THEN destination_address
            END) AS  distinct_inbound_addresses,
              count( distinct CASE
                WHEN direction = 'inbound' THEN tx_hash
            END) AS  distinct_inbound_transactions,

             SUM(CASE
                WHEN direction = 'outbound' THEN amount_usd
                ELSE 0
            END) AS  total_outbound_volume,
                 count( distinct CASE
                WHEN direction = 'outbound' THEN source_address
            END) AS  distinct_outbound_addresses,
              count( distinct CASE
                WHEN direction = 'outbound' THEN tx_hash
            END) AS  distinct_outbound_transactions,

        
        FROM
            {{ ref('defi__ez_bridge_activity') }} A
            JOIN silver.ez_activity_metrics__intermediate_tmp b
            ON A.blockchain = b.blockchain
            AND A.block_timestamp :: DATE = b.block_date
        WHERE
            {{ date_filter }}
        GROUP BY
            A.blockchain,
            block_timestamp_hour,
            sender
    )
SELECT
    A.blockchain,
    A.block_timestamp_hour :: DATE AS block_date,
    SUM(transaction_count) AS transaction_count,
    SUM(
        CASE
            WHEN C.blockchain IS NOT NULL THEN transaction_count
            ELSE 0
        END
    ) AS quality_transaction_count,
    SUM(transaction_count_succeeded) AS transaction_count_succeeded,
    SUM(
        CASE
            WHEN C.blockchain IS NOT NULL THEN transaction_count_succeeded
            ELSE 0
        END
    ) AS quality_transaction_count_succeeded,
    COUNT(
        DISTINCT sender
    ) AS unique_initiator_count,
    COUNT(
        DISTINCT CASE
            WHEN C.blockchain IS NOT NULL THEN sender
        END
    ) AS quality_unique_initiator_count,
    SUM(total_fees_native) AS total_fees_native,
    SUM(
        CASE
            WHEN C.blockchain IS NOT NULL THEN total_fees_native
            ELSE 0
        END
    ) AS quality_total_fees_native,
    SUM(
        total_fees_native * price
    ) AS total_fees_usd,
    SUM(
        CASE
            WHEN C.blockchain IS NOT NULL THEN total_fees_native * price
            ELSE 0
        END
    ) AS quality_total_fees_usd,
    {{ dbt_utils.generate_surrogate_key(['a.blockchain',' A.block_timestamp_hour :: DATE']) }} AS ez_activity_metrics_daily_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp
FROM
    silver.ez_activity_metrics__tx_intermediate_tmp A asof
    JOIN prices b match_condition (
        A.block_timestamp_hour >= b.hour
    )
    ON A.blockchain = b.blockchain
    LEFT JOIN silver.ez_activity_metrics__scores_intermediate_tmp C
    ON A.blockchain = C.blockchain
    AND A.sender = C.user_address
    AND A.block_timestamp_hour :: DATE = C.block_date
GROUP BY
    A.blockchain,
    A.block_timestamp_hour :: DATE
