{% macro get_protocol_metrics_daily(blockchain_filter) %}
    WITH quals AS (
        SELECT
            blockchain,
            score_date,
            platform,
            SUM(COUNT) AS num_users,
            SUM(
                CASE
                    WHEN score_bucket >= 4 THEN COUNT
                    ELSE 0
                END
            ) AS n_quality_users,
            SUM(tx_count) AS n_transactions,
            SUM(
                CASE
                    WHEN score_bucket >= 4 THEN tx_count
                    ELSE 0
                END
            ) AS n_quality_transactions
        FROM
            {{ source(
                'datascience_onchain_scores',
                'protocol_metrics'
            ) }}
        GROUP BY
            blockchain,
            score_date,
            platform
    )
SELECT
    COALESCE(
        A.block_date,
        b.score_Date
    ) day_,
    COALESCE(
        A.protocol,
        b.platform
    ) protocol,
    num_users AS n_users,
    n_quality_users,
    n_transactions,
    n_quality_transactions,
    inflow_usd AS usd_inflows,
    outflow_usd AS usd_outflows,
    net_usd_inflow,
    gross_usd_volume,
    quality_inflow_usd AS quality_usd_inflows,
    quality_outflow_usd AS quality_usd_outflows,
    quality_net_usd_inflow AS quality_net_usd,
    quality_gross_usd_volume AS quality_gross_usd
FROM
    {{ ref('stats__ez_transfer_protocol_metrics_daily') }} A full
    OUTER JOIN quals b
    ON A.blockchain = b.blockchain
    AND LOWER(TRIM(A.protocol)) = LOWER(TRIM(b.platform))
    AND A.block_date = b.score_date
WHERE
    COALESCE(
        A.blockchain,
        b.blockchain
    ) = '{{ blockchain_filter }}'
{% endmacro %}
