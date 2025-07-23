{% macro get_protocol_metrics_daily(blockchain_filter) %}
SELECT
    block_date AS day_,
    protocol AS protocol,
    num_users AS n_users,
    num_quality_users AS n_quality_users,
    transaction_count AS n_transactions,
    quality_transaction_count AS n_quality_transactions,
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
    OUTER JOIN {{ ref('stats__ez_activity_protocol_metrics_daily') }}
    b USING (
        blockchain,
        protocol,
        block_date
    )
WHERE
    blockchain = '{{ blockchain_filter }}'
{% endmacro %}
