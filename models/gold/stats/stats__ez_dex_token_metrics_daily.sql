-- depends_on: {{ ref('defi__ez_dex_swaps') }}
{{ config(
    materialized = 'incremental',
    incremental_strategy = 'merge',
    merge_exclude_columns = ["inserted_timestamp"],
    unique_key = ['blockchain','block_date','token_address'],
    cluster_by = ['blockchain','block_date'],
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

WITH swaps_in AS (
    SELECT
        A.blockchain,
        A.token_in AS token_address,
        MAX(
            A.symbol_in
        ) AS symbol,
        A.block_timestamp :: DATE AS block_date,
        COUNT(1) AS buy_swap_count,
        ARRAY_AGG(
            DISTINCT trader
        ) AS swappers,
        SUM(amount_in) AS buy_volume,
        SUM(amount_in_usd) AS buy_usd_volume
    FROM
        {{ ref('defi__ez_dex_swaps') }} A
        JOIN silver.ez_dex_metrics__intermediate_tmp b
        ON A.blockchain = b.blockchain
        AND A.block_timestamp :: DATE = b.block_date
    WHERE
        token_in_is_verified
        AND {{ date_filter }}
    GROUP BY
        A.blockchain,
        A.token_in,
        A.block_timestamp :: DATE
),
swaps_out AS (
    SELECT
        A.blockchain,
        A.token_out AS token_address,
        MAX(
            A.symbol_out
        ) AS symbol,
        A.block_timestamp :: DATE AS block_date,
        COUNT(1) AS sell_swap_count,
        ARRAY_AGG(
            DISTINCT trader
        ) AS swappers,
        SUM(amount_out) AS sell_volume,
        SUM(amount_out_usd) AS sell_usd_volume
    FROM
        {{ ref('defi__ez_dex_swaps') }} A
        JOIN silver.ez_dex_metrics__intermediate_tmp b
        ON A.blockchain = b.blockchain
        AND A.block_timestamp :: DATE = b.block_date
    WHERE
        token_out_is_verified
        AND {{ date_filter }}
    GROUP BY
        A.blockchain,
        A.token_out,
        A.block_timestamp :: DATE
),
base AS (
    SELECT
        DISTINCT blockchain,
        block_date,
        token_address
    FROM
        swaps_in
    UNION
    SELECT
        DISTINCT blockchain,
        block_date,
        token_address
    FROM
        swaps_out
),
swappers AS (
    SELECT
        A.blockchain,
        A.token_address,
        A.block_date,
        ARRAY_SIZE(array_union_agg(swappers)) AS token_swappers
    FROM
        (
            SELECT
                blockchain,
                token_address,
                block_date,
                swappers
            FROM
                swaps_in
            UNION ALL
            SELECT
                blockchain,
                token_address,
                block_date,
                swappers
            FROM
                swaps_out
        ) A
    GROUP BY
        A.blockchain,
        A.token_address,
        A.block_date
)
SELECT
    A.blockchain,
    A.token_address,
    GREATEST(
        si.symbol,
        so.symbol
    ) AS symbol,
    A.block_date,
    si.buy_swap_count,
    si.buy_volume,
    si.buy_usd_volume,
    so.sell_swap_count,
    sers.token_swappers,
    so.sell_volume,
    so.sell_usd_volume,
    COALESCE(
        si.buy_volume,
        0
    ) - COALESCE(
        so.sell_volume,
        0
    ) AS net_purchased,
    ROUND(COALESCE(si.buy_usd_volume, 0) - COALESCE(so.sell_usd_volume, 0), 2) AS net_purchased_usd,
    {{ dbt_utils.generate_surrogate_key(['a.blockchain',' A.block_date','A.token_address']) }} AS ez_dex_token_metrics_daily_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp
FROM
    base A
    LEFT JOIN swappers sers
    ON A.blockchain = sers.blockchain
    AND A.token_address = sers.token_address
    AND A.block_date = sers.block_date
    LEFT JOIN swaps_in si
    ON A.blockchain = si.blockchain
    AND A.token_address = si.token_address
    AND A.block_date = si.block_date
    LEFT JOIN swaps_out so
    ON A.blockchain = so.blockchain
    AND A.token_address = so.token_address
    AND A.block_date = so.block_date
