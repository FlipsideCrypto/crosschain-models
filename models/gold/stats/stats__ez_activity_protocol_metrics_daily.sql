-- depends_on: {{ ref('silver__verified_contracts') }}
-- depends_on:  {{ ref('silver__fact_protocol_ineractions') }}
-- depends_on:  {{ ref('core__dim_labels') }}
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
OR REPLACE temporary TABLE silver.ez_activity_protocol_metrics__intermediate_tmp AS
SELECT
    DISTINCT blockchain,
    block_timestamp :: DATE AS block_date
FROM
    {{ ref('silver__fact_protocol_ineractions') }}
WHERE
    block_timestamp :: DATE < SYSDATE() :: DATE

{% if is_incremental() %}
AND modified_timestamp >= '{{ max_mod }}'
{% else %}
    AND block_timestamp :: DATE >= '2025-01-01'
    AND block_timestamp :: DATE <= '2025-01-02'
{% endif %}

{% endset %}
{% do run_query(dates_query) %}
--create a dynamic where clause with literal block dates
{% set date_query %}
SELECT
    DISTINCT block_date
FROM
    silver.ez_activity_protocol_metrics__intermediate_tmp {% endset %}
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
    --get the labels
    {% set labels_query %}
    CREATE
    OR REPLACE temporary TABLE silver.ez_activity_protocol_metrics_labels__intermediate_tmp AS
SELECT
    blockchain,
    address,
    label
FROM
    (
        SELECT
            blockchain,
            contract_address address,
            platform AS label,
            1 AS priority
        FROM
            {{ ref('silver__verified_contracts') }}
        UNION ALL
        SELECT
            blockchain,
            address,
            project_name AS label,
            2 AS priority
        FROM
            {{ ref('core__dim_labels') }}
        WHERE
            label_type NOT IN (
                'operator',
                'chadmin',
                'flotsam',
                'token',
                'cex'
            )
    )
WHERE
    blockchain IN (
        SELECT
            blockchain
        FROM
            silver.ez_activity_protocol_metrics__intermediate_tmp
    ) qualify ROW_NUMBER() over (
        PARTITION BY blockchain,
        address
        ORDER BY
            priority ASC
    ) = 1 {% endset %}
    {% do run_query(labels_query) %}
    --roll transactions up to the hour/sender level
    {% set inc_query %}
    CREATE
    OR REPLACE temporary TABLE silver.ez_activity_protocol_metrics__tx_intermediate_tmp AS
SELECT
    A.blockchain,
    block_timestamp :: DATE AS block_date,
    contract_address,
    sender,
    C.label AS protocol,
    COUNT(
        DISTINCT tx_hash
    ) AS transaction_count
FROM
    {{ ref('silver__fact_protocol_ineractions') }} A
    JOIN silver.ez_activity_protocol_metrics__intermediate_tmp b
    ON A.blockchain = b.blockchain
    AND A.block_timestamp :: DATE = b.block_date
    JOIN silver.ez_activity_protocol_metrics_labels__intermediate_tmp C
    ON A.blockchain = C.blockchain
    AND A.contract_address = C.address
WHERE
    {{ date_filter }}
GROUP BY
    A.blockchain,
    block_timestamp :: DATE,
    contract_address,
    sender,
    C.label {% endset %}
    {% do run_query(inc_query) %}
    --find distinct score dates
    {% set score_dates_query %}
    CREATE
    OR REPLACE temporary TABLE silver.ez_activity_protocol_metrics__score_dates_intermediate_tmp AS
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
    OR REPLACE temporary TABLE silver.ez_activity_protocol_metrics__scores_asof_intermediate_tmp AS
SELECT
    DISTINCT A.blockchain,
    A.block_date,
    b.score_date
FROM
    silver.ez_activity_protocol_metrics__intermediate_tmp A asof
    JOIN silver.ez_activity_protocol_metrics__score_dates_intermediate_tmp b match_condition (
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
    OR REPLACE temporary TABLE silver.ez_activity_protocol_metrics__scores_intermediate_tmp AS
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
    JOIN silver.ez_activity_protocol_metrics__scores_asof_intermediate_tmp b
    ON A.blockchain = b.blockchain
    AND A.score_date = b.score_date {% endset %}
    {% do run_query(scores_query) %}
    --delete the scores temp with a score less than 4 or the additional rows from the asof join
    {% set scores_del_query %}
DELETE FROM
    silver.ez_activity_protocol_metrics__scores_intermediate_tmp
WHERE
    total_score < 4 {% endset %}
    {% do run_query(scores_del_query) %}
{% endif %}

--Final aggregate of the data to the daily level
SELECT
    A.blockchain,
    A.block_date,
    A.protocol,
    SUM(transaction_count) AS transaction_count,
    SUM(
        CASE
            WHEN C.blockchain IS NOT NULL THEN transaction_count
            ELSE 0
        END
    ) AS quality_transaction_count,
    COUNT(
        DISTINCT sender
    ) AS num_users,
    COUNT(
        DISTINCT CASE
            WHEN C.blockchain IS NOT NULL THEN sender
        END
    ) AS num_quality_users,
    {{ dbt_utils.generate_surrogate_key(['a.blockchain',' A.block_date']) }} AS ez_activity_protocol_metrics_daily_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp
FROM
    silver.ez_activity_protocol_metrics__tx_intermediate_tmp A
    LEFT JOIN silver.ez_activity_protocol_metrics__scores_intermediate_tmp C
    ON A.blockchain = C.blockchain
    AND A.sender = C.user_address
    AND A.block_date = C.block_date
GROUP BY
    A.blockchain,
    A.block_date,
    A.protocol
