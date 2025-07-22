-- depends_on: {{ ref('defi__ez_dex_swaps') }}
{{ config(
    materialized = 'incremental',
    incremental_strategy = 'merge',
    merge_exclude_columns = ["inserted_timestamp"],
    unique_key = ['blockchain','block_date','protocol'],
    cluster_by = ['blockchain','block_date','protocol'],
    tags = ['metrics_daily','dex']
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
OR REPLACE temporary TABLE silver.ez_dex_metrics__intermediate_tmp AS
SELECT
    DISTINCT blockchain,
    block_timestamp :: DATE AS block_date
FROM
    {{ ref('defi__ez_dex_swaps') }}
WHERE
    block_timestamp :: DATE < SYSDATE() :: DATE
    AND block_timestamp :: DATE >= '2025-01-01'

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
    silver.ez_dex_metrics__intermediate_tmp {% endset %}
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

WITH swaps AS (
    SELECT
        A.blockchain,
        A.platform AS protocol,
        A.block_timestamp :: DATE AS block_date,
        COUNT(1) AS swap_count,
        COUNT(
            DISTINCT trader
        ) AS distinct_swapper_count,
        SUM(
            CASE
                WHEN token_in_is_verified
                AND amount_in_usd IS NOT NULL THEN amount_in_usd
                WHEN token_out_is_verified
                AND amount_out_usd IS NOT NULL THEN amount_out_usd
                ELSE 0
            END
        ) AS gross_dex_volume_usd
    FROM
        {{ ref('defi__ez_dex_swaps') }} A
        JOIN silver.ez_dex_metrics__intermediate_tmp b
        ON A.blockchain = b.blockchain
        AND A.block_timestamp :: DATE = b.block_date
    WHERE
        {{ date_filter }}
    GROUP BY
        A.blockchain,
        A.platform,
        A.block_timestamp :: DATE
)
SELECT
    A.blockchain,
    A.protocol,
    A.block_date,
    A.swap_count,
    A.distinct_swapper_count,
    A.gross_dex_volume_usd,
    {{ dbt_utils.generate_surrogate_key(['a.blockchain',' A.block_date','A.protocol']) }} AS ez_dex_protocol_metrics_daily_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp
FROM
    swaps A
