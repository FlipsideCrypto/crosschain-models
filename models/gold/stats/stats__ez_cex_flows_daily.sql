-- depends_on: {{ ref('silver__transfers') }}
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

--get the cex labels
{% set labels_query %}
CREATE
OR REPLACE temporary TABLE silver.ez_cex_flows_labels__intermediate_tmp AS
SELECT
    blockchain,
    address,
    label_type,
    label_subtype
FROM
    {{ ref('core__dim_labels') }}
WHERE
    label_type = 'cex' {% endset %}
    {% do run_query(labels_query) %}
    --get the distinct blockchains & block dates that we are processing
    {% set dates_query %}
    CREATE
    OR REPLACE temporary TABLE silver.ez_cex_flows__intermediate_tmp AS
SELECT
    DISTINCT blockchain,
    block_timestamp :: DATE AS block_date
FROM
    {{ ref('silver__transfers') }}
WHERE
    block_timestamp :: DATE < SYSDATE() :: DATE
    AND block_timestamp :: DATE >= '2025-01-01'

{% if is_incremental() %}
AND modified_timestamp >= '{{ max_mod }}'
AND block_timestamp :: DATE >= '2025-01-01'
{% endif %}

{% endset %}
{% do run_query(dates_query) %}
--get the cex labels
{% set labels_query %}
CREATE
OR REPLACE temporary TABLE silver.ez_cex_flows_labels__intermediate_tmp AS
SELECT
    blockchain,
    address,
    label_type,
    label_subtype
FROM
    {{ ref('core__dim_labels') }}
WHERE
    label_type = 'cex' {% endset %}
    {% do run_query(labels_query) %}
    --create a dynamic where clause with literal block dates
    {% set date_query %}
SELECT
    DISTINCT block_date
FROM
    silver.ez_cex_flows__intermediate_tmp {% endset %}
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
    --get the xfer data
    {% set xfer_query %}
    CREATE
    OR REPLACE temporary TABLE silver.ez_cex_flows_xfers__intermediate_tmp AS
SELECT
    A.blockchain,
    A.block_timestamp,
    A.token_is_verified,
    A.from_address,
    A.to_address,
    A.amount_usd,
    A.tx_hash
FROM
    {{ ref('silver__transfers') }} A
    JOIN silver.ez_cex_flows__intermediate_tmp b
    ON A.blockchain = b.blockchain
    AND A.block_timestamp :: DATE = b.block_date
WHERE
    block_timestamp :: DATE < SYSDATE() :: DATE
    AND block_timestamp :: DATE >= '2025-01-01'

{% if is_incremental() %}
AND {{ date_filter }}
{% endif %}

{% endset %}
{% do run_query(xfer_query) %}
{% endif %}

WITH withdraw AS (
    SELECT
        A.blockchain,
        A.block_timestamp :: DATE AS block_date,
        SUM(
            CASE
                WHEN token_is_verified THEN amount_usd
                ELSE 0
            END
        ) AS withdrawal_volume_usd,
        COUNT(
            DISTINCT tx_hash
        ) AS withdrawal_txn_count,
        COUNT(
            DISTINCT to_address
        ) AS unique_withdrawing_addresses
    FROM
        silver.ez_cex_flows_xfers__intermediate_tmp A
        JOIN silver.ez_cex_flows_labels__intermediate_tmp from_label
        ON A.blockchain = from_label.blockchain
        AND A.from_address = from_label.address
        AND from_label.label_subtype = 'hot_wallet'
        LEFT JOIN silver.ez_cex_flows_labels__intermediate_tmp to_label
        ON A.blockchain = to_label.blockchain
        AND A.to_address = to_label.address
    WHERE
        COALESCE(
            to_label.label_type,
            ''
        ) <> 'cex'
    GROUP BY
        A.blockchain,
        A.block_timestamp :: DATE
),
deposit AS (
    SELECT
        A.blockchain,
        A.block_timestamp :: DATE AS block_date,
        SUM(
            CASE
                WHEN token_is_verified THEN amount_usd
                ELSE 0
            END
        ) AS deposit_volume_usd,
        COUNT(
            DISTINCT tx_hash
        ) AS deposit_txn_count,
        COUNT(
            DISTINCT from_address
        ) AS unique_depositing_addresses
    FROM
        silver.ez_cex_flows_xfers__intermediate_tmp A
        JOIN silver.ez_cex_flows_labels__intermediate_tmp to_label
        ON A.blockchain = to_label.blockchain
        AND A.to_address = to_label.address
        AND to_label.label_subtype = 'deposit_wallet'
        LEFT JOIN silver.ez_cex_flows_labels__intermediate_tmp from_label
        ON A.blockchain = from_label.blockchain
        AND A.from_address = from_label.address
    WHERE
        COALESCE(
            from_label.label_type,
            ''
        ) <> 'cex'
    GROUP BY
        A.blockchain,
        A.block_timestamp :: DATE
)
SELECT
    blockchain,
    block_date,
    COALESCE(
        w.withdrawal_volume_usd,
        0
    ) AS withdrawal_volume_usd,
    COALESCE(
        w.withdrawal_txn_count,
        0
    ) AS withdrawal_txn_count,
    COALESCE(
        d.deposit_volume_usd,
        0
    ) AS deposit_volume_usd,
    COALESCE(
        d.deposit_txn_count,
        0
    ) AS deposit_txn_count,
    COALESCE(
        w.unique_withdrawing_addresses,
        0
    ) AS unique_withdrawing_addresses,
    COALESCE(
        d.unique_depositing_addresses,
        0
    ) AS unique_depositing_addresses,
    COALESCE(
        w.withdrawal_volume_usd,
        0
    ) - COALESCE(
        d.deposit_volume_usd,
        0
    ) AS net_cex_flow_usd,
    COALESCE(
        w.withdrawal_txn_count,
        0
    ) - COALESCE(
        d.deposit_txn_count,
        0
    ) AS net_cex_flow_txn_count,
    CASE
        WHEN COALESCE(
            d.deposit_volume_usd,
            0
        ) > 0 THEN ROUND(
            (COALESCE(w.withdrawal_volume_usd, 0) - COALESCE(d.deposit_volume_usd, 0)) / COALESCE(
                d.deposit_volume_usd,
                0
            ) * 100,
            2
        )
        ELSE NULL
    END AS net_cex_flow_percent_of_deposits,
    {{ dbt_utils.generate_surrogate_key(['blockchain',' block_date']) }} AS ez_cex_flows_daily_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp
FROM
    withdraw w full
    OUTER JOIN deposit d USING (
        blockchain,
        block_date
    )
