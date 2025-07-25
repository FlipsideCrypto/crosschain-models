-- depends_on: {{ ref('silver__tokens_stablecoins') }}
-- depends_on: {{ ref('silver__transfers') }}
{{ config(
    materialized = 'incremental',
    incremental_strategy = 'merge',
    merge_exclude_columns = ["inserted_timestamp"],
    unique_key = ['blockchain','block_date','token_address'],
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
OR REPLACE temporary TABLE silver.ez_stable_token_flows__intermediate_tmp AS
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
{% endif %}

{% endset %}
{% do run_query(dates_query) %}
--create a dynamic where clause with literal block dates
{% set date_query %}
SELECT
    DISTINCT block_date
FROM
    silver.ez_stable_token_flows__intermediate_tmp {% endset %}
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

WITH stables AS (
    SELECT
        DISTINCT blockchain,
        token_address
    FROM
        {{ ref('silver__tokens_stablecoins') }}
),
xfer AS (
    SELECT
        A.blockchain,
        A.block_timestamp :: DATE AS block_date,
        A.address AS token_address,
        SUM(
            amount_usd
        ) AS stablecoin_transfer_volume_usd
    FROM
        {{ ref('silver__transfers') }} A
        JOIN silver.ez_stable_token_flows__intermediate_tmp b
        ON A.blockchain = b.blockchain
        AND A.block_timestamp :: DATE = b.block_date
        JOIN stables stbl
        ON A.blockchain = stbl.blockchain
        AND A.address = stbl.token_address
    WHERE
        {{ date_filter }}
        AND amount_usd < 500000000 -- $500M cap
    GROUP BY
        A.blockchain,
        A.block_timestamp :: DATE,
        A.address
)
SELECT
    A.blockchain,
    A.block_date,
    A.token_address,
    b.symbol,
    COALESCE(
        stablecoin_transfer_volume_usd,
        0
    ) AS stablecoin_transfer_volume_usd,
    {{ dbt_utils.generate_surrogate_key(['a.blockchain','a.block_date','a.token_address']) }} AS ez_stablecoin_token_flows_daily_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp
FROM
    xfer A
    JOIN {{ ref('silver__tokens_enhanced') }}
    b
    ON A.blockchain = b.blockchain
    AND A.token_address = b.address
WHERE
    is_verified
