-- depends_on: {{ ref('defi__ez_bridge_activity') }}
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
OR REPLACE temporary TABLE silver.ez_bridge_metrics__intermediate_tmp AS
SELECT
    DISTINCT source_chain,
    destination_chain,
    block_timestamp :: DATE AS block_date
FROM
    {{ ref('defi__ez_bridge_activity') }}
WHERE
    block_timestamp :: DATE < SYSDATE() :: DATE

{% if is_incremental() %}
AND modified_timestamp >= '{{ max_mod }}'
AND block_timestamp :: DATE >= '2025-01-01'
{% endif %}

{% endset %}
{% do run_query(dates_query) %}
--create a dynamic where clause with literal block dates
{% set date_query %}
SELECT
    DISTINCT block_date
FROM
    silver.ez_bridge_metrics__intermediate_tmp {% endset %}
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
{% endif %}

WITH target_chains_cte AS (
    SELECT
        DISTINCT blockchain AS chain_name
    FROM
        {{ ref('defi__ez_bridge_activity') }}
),
ib AS (
    SELECT
        A.destination_chain AS blockchain,
        A.block_timestamp :: DATE AS block_date,
        SUM(
            CASE
                WHEN token_is_verified THEN amount_usd
                ELSE 0
            END
        ) AS total_inbound_volume,
        COUNT(
            DISTINCT destination_address
        ) AS distinct_inbound_addresses,
        COUNT(
            DISTINCT tx_hash
        ) AS distinct_inbound_transactions
    FROM
        {{ ref('defi__ez_bridge_activity') }} A
        JOIN target_chains_cte t
        ON t.chain_name = A.destination_chain
        JOIN (
            SELECT
                DISTINCT destination_chain,
                block_date
            FROM
                silver.ez_bridge_metrics__intermediate_tmp
        ) b
        ON A.destination_chain = b.destination_chain
        AND A.block_timestamp :: DATE = b.block_date
    WHERE
        {{ date_filter }}
    GROUP BY
        A.destination_chain,
        A.block_timestamp :: DATE
),
ob AS (
    SELECT
        A.source_chain AS blockchain,
        A.block_timestamp :: DATE AS block_date,
        SUM(
            CASE
                WHEN token_is_verified THEN amount_usd
                ELSE 0
            END
        ) AS total_outbound_volume,
        COUNT(
            DISTINCT source_address
        ) AS distinct_outbound_addresses,
        COUNT(
            DISTINCT tx_hash
        ) AS distinct_outbound_transactions
    FROM
        {{ ref('defi__ez_bridge_activity') }} A
        JOIN target_chains_cte t
        ON t.chain_name = A.source_chain
        JOIN (
            SELECT
                DISTINCT source_chain,
                block_date
            FROM
                silver.ez_bridge_metrics__intermediate_tmp
        ) b
        ON A.source_chain = b.source_chain
        AND A.block_timestamp :: DATE = b.block_date
    WHERE
        {{ date_filter }}
    GROUP BY
        A.source_chain,
        A.block_timestamp :: DATE
),
base AS (
    SELECT
        DISTINCT blockchain,
        block_date
    FROM
        ib
    UNION
    SELECT
        DISTINCT blockchain,
        block_date
    FROM
        ob
)
SELECT
    A.blockchain,
    A.block_date,
    ib.total_inbound_volume,
    ib.distinct_inbound_addresses,
    ib.distinct_inbound_transactions,
    ob.total_outbound_volume,
    ob.distinct_outbound_addresses,
    ob.distinct_outbound_transactions,
    COALESCE(
        ib.total_inbound_volume,
        0
    ) - COALESCE(
        ob.total_outbound_volume,
        0
    ) AS net_volume,
    COALESCE(
        ib.total_inbound_volume,
        0
    ) + COALESCE(
        ob.total_outbound_volume,
        0
    ) AS gross_volume,
    {{ dbt_utils.generate_surrogate_key(['a.blockchain',' A.block_date']) }} AS ez_bridge_metrics_daily_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp
FROM
    base A
    LEFT JOIN ib
    ON A.blockchain = ib.blockchain
    AND A.block_date = ib.block_date
    LEFT JOIN ob
    ON A.blockchain = ob.blockchain
    AND A.block_date = ob.block_date
