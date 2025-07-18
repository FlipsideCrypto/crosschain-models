-- depends_on: {{ ref('silver__transfers') }}
-- depends_on: {{ ref('price__ez_prices_hourly') }}
-- depends_on: {{ ref('silver__native_fee_token') }}
-- depends_on: {{ ref('core__dim_labels') }}
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
OR REPLACE temporary TABLE silver.ez_transfer_protocol_metrics__intermediate_tmp AS
SELECT
    DISTINCT blockchain,
    block_timestamp :: DATE AS block_date
FROM
    {{ ref('silver__transfers') }}
WHERE
    block_timestamp :: DATE < SYSDATE() :: DATE

{% if is_incremental() %}
{# AND modified_timestamp >= '{{ max_mod }}' #}
AND block_timestamp :: DATE >= '2025-07-01'
{% else %}
    AND block_timestamp :: DATE >= '2025-01-01'
    AND block_timestamp :: DATE >= '2025-01-03'
{% endif %}

{% endset %}
{% do run_query(dates_query) %}
--create a dynamic where clause with literal block dates
{% set date_query %}
SELECT
    DISTINCT block_date
FROM
    silver.ez_transfer_protocol_metrics__intermediate_tmp {% endset %}
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
    OR REPLACE temporary TABLE silver.ez_transfer_protocol_labels__intermediate_tmp AS
SELECT
    blockchain,
    address,
    project_name AS label
FROM
    {{ ref('core__dim_labels') }}
WHERE
    blockchain IN (
        SELECT
            blockchain
        FROM
            silver.ez_transfer_protocol_metrics__intermediate_tmp
    )
    AND label_type NOT IN (
        'cex',
        'flotsam',
        'token',
        'chadmin'
    )
    AND CONCAT(
        label_type,
        '-',
        label_subtype
    ) NOT IN (
        'nft-general_contract',
        'nft-nf_token_contract'
    ) {% endset %}
    {% do run_query(labels_query) %}
    --roll transactions up to the hour/sender level
    {% set inc_query %}
    CREATE
    OR REPLACE temporary TABLE silver.ez_transfer_protocol_metrics__xfer_intermediate_tmp AS
SELECT
    A.blockchain,
    A.block_timestamp :: DATE AS block_date,
    A.origin_from_address,
    A.from_address,
    A.to_address,
    c_from.label AS from_label,
    c_to.label AS to_label,
    SUM(
        A.amount_usd
    ) AS amount_usd
FROM
    {{ ref('silver__transfers') }} A
    JOIN silver.ez_transfer_protocol_metrics__intermediate_tmp b
    ON A.blockchain = b.blockchain
    AND A.block_timestamp :: DATE = b.block_date
    LEFT JOIN silver.ez_transfer_protocol_labels__intermediate_tmp c_from
    ON A.blockchain = c_from.blockchain
    AND A.from_address = c_from.address
    LEFT JOIN silver.ez_transfer_protocol_labels__intermediate_tmp c_to
    ON A.blockchain = c_to.blockchain
    AND A.to_address = c_to.address
WHERE
    {{ date_filter }}
    AND COALESCE(
        c_from.label,
        c_to.label
    ) IS NOT NULL
    AND A.token_is_verified
GROUP BY
    A.blockchain,
    A.block_timestamp :: DATE,
    A.origin_from_address,
    A.from_address,
    A.to_address,
    c_from.label,
    c_to.label {% endset %}
    {% do run_query(inc_query) %}
    --find distinct score dates
    {% set score_dates_query %}
    CREATE
    OR REPLACE temporary TABLE silver.ez_transfer_protocol_metrics__score_dates_intermediate_tmp AS
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
    OR REPLACE temporary TABLE silver.ez_transfer_protocol_metrics__scores_asof_intermediate_tmp AS
SELECT
    DISTINCT A.blockchain,
    A.block_date,
    b.score_date
FROM
    ez_transfer_protocol_metrics__intermediate_tmp A asof
    JOIN silver.ez_transfer_protocol_metrics__score_dates_intermediate_tmp b match_condition (
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
    OR REPLACE temporary TABLE silver.ez_transfer_protocol_metrics__scores_intermediate_tmp AS
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
    JOIN silver.ez_transfer_protocol_metrics__scores_asof_intermediate_tmp b
    ON A.blockchain = b.blockchain
    AND A.score_date = b.score_date {% endset %}
    {% do run_query(scores_query) %}
    --delete the scores temp with a score less than 4 or the additional rows from the asof join
    {% set scores_del_query %}
DELETE FROM
    silver.ez_transfer_protocol_metrics__scores_intermediate_tmp
WHERE
    total_score < 4 {% endset %}
    {% do run_query(scores_del_query) %}
{% endif %}

WITH ob AS (
    SELECT
        block_date,
        from_label AS label,
        SUM(amount_usd) AS amount_usd,
        SUM(
            CASE
                WHEN C.blockchain IS NOT NULL THEN amount_usd
                ELSE 0
            END
        ) AS quality_amount_usd,
        SUM(
            CASE
                WHEN from_label = to_label THEN amount_usd
                ELSE 0
            END
        ) AS internal_amount_usd,
        SUM(
            CASE
                WHEN C.blockchain IS NOT NULL
                AND from_label = to_label THEN amount_usd
                ELSE 0
            END
        ) AS quality_internal_amount_usd
    FROM
        silver.ez_transfer_protocol_metrics__xfer_intermediate_tmp A
        LEFT JOIN silver.ez_transfer_protocol_metrics__scores_intermediate_tmp C
        ON A.blockchain = C.blockchain
        AND A.origin_from_address = C.user_address
        AND A.block_date = C.block_date
    WHERE
        from_label IS NOT NULL
    GROUP BY
        block_date,
        from_label
),
ib AS (
    SELECT
        block_date,
        to_label AS label,
        SUM(amount_usd) AS amount_usd,
        SUM(
            CASE
                WHEN C.blockchain IS NOT NULL THEN amount_usd
                ELSE 0
            END
        ) AS quality_amount_usd,
        SUM(
            CASE
                WHEN from_label = to_label THEN amount_usd
                ELSE 0
            END
        ) AS internal_amount_usd,
        SUM(
            CASE
                WHEN C.blockchain IS NOT NULL
                AND from_label = to_label THEN amount_usd
                ELSE 0
            END
        ) AS quality_internal_amount_usd
    FROM
        silver.ez_transfer_protocol_metrics__xfer_intermediate_tmp A
        LEFT JOIN silver.ez_transfer_protocol_labels__intermediate_tmp b_from
        ON A.from_address = b_from.address
        LEFT JOIN silver.ez_transfer_protocol_labels__intermediate_tmp b_to
        ON A.from_address = b_to.address
        LEFT JOIN silver.ez_transfer_protocol_metrics__scores_intermediate_tmp C
        ON A.blockchain = C.blockchain
        AND A.origin_from_address = C.user_address
        AND A.block_date = C.block_date
    WHERE
        to_label IS NOT NULL
    GROUP BY
        block_date,
        to_label
)
SELECT
    blockchain,
    block_date,
    label AS protocol,
    COALESCE(
        ib.amount_usd,
        0
    ) AS inflow_usd,
    COALESCE(
        ob.amount_usd,
        0
    ) AS outflow_usd,
    inflow_usd - outflow_usd AS net_usd_inflow,
    SUM(
        CASE
            WHEN internal_amount_usd IS NOT NULL THEN internal_amount_usd
            ELSE inflow_usd + outflow_usd
        END
    ) AS gross_usd_volume,
    COALESCE(
        ib.quality_amount_usd,
        0
    ) AS quality_inflow_usd,
    COALESCE(
        ob.quality_amount_usd,
        0
    ) AS quality_outflow_usd,
    quality_inflow_usd - quality_outflow_usd AS quality_net_usd_inflow,
    SUM(
        CASE
            WHEN quality_internal_amount_usd IS NOT NULL THEN quality_internal_amount_usd
            ELSE quality_inflow_usd + quality_outflow_usd
        END
    ) AS quality_gross_usd_volume,
    {{ dbt_utils.generate_surrogate_key(['blockchain',' block_date','protocol']) }} AS ez_transfer_protocol_metrics_daily_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp
FROM
    ob A full
    OUTER JOIN ib b USING(
        blockchain,
        block_date,
        label
    )
