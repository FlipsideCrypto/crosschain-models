{{ config(
    materialized = 'view',
    tags = ['metrics_daily']
) }}

SELECT
    block_date AS day_,
    A.unique_initiator_count AS active_users_count,
    A.quality_unique_initiator_count AS active_quality_users_count,
    A.transaction_count,
    A.quality_transaction_count,
    A.total_fees_native AS total_fees,
    A.total_fees_usd,
    A.quality_total_fees_native quality_total_fees,
    A.quality_total_fees_usd,
    d.stablecoin_transfer_volume_usd,
    g.in_unit_total_transfer_volume,
    g.total_transfer_volume_usd,
    g.in_unit_quality_total_transfer_volume,
    g.quality_total_transfer_volume_usd,
    e.withdrawal_volume_usd AS cex_withdrawal_volume_usd,
    e.withdrawal_txn_count AS cex_withdrawal_tx_count,
    e.unique_withdrawing_addresses AS cex_unique_withdrawing_addresses,
    e.deposit_volume_usd AS cex_deposit_volume_usd,
    e.deposit_txn_count AS cex_deposit_tx_count,
    e.unique_depositing_addresses AS cex_unique_depositing_addresses,
    e.net_cex_flow_usd AS cex_net_flow_usd,
    C.gross_dex_volume_usd AS chain_gross_dex_volume_usd,
    C.swap_count AS chain_swap_count,
    C.distinct_swapper_count chain_swapper_count,
    f.current_tvl AS tvl_usd,
    (
        A.total_fees_native / A.total_fees_usd
    ) * f.current_tvl AS in_unit_tvl,
    b.total_inbound_volume AS bridge_inbound_volume_usd,
    b.distinct_inbound_addresses AS bridge_inbound_addresses,
    b.distinct_inbound_transactions AS bridge_inbound_tx_count,
    b.total_outbound_volume AS bridge_outbound_volume_usd,
    b.distinct_outbound_addresses AS bridge_outbound_addresses,
    b.distinct_outbound_transactions AS bridge_outbound_tx_count,
    b.gross_volume AS bridge_gross_volume_usd,
    b.net_volume AS bridge_net_inbound_usd
FROM
    {{ ref('stats__ez_activity_metrics_daily') }} A
    LEFT JOIN {{ ref('stats__ez_bridge_metrics_daily') }}
    b USING(
        block_date,
        blockchain
    )
    LEFT JOIN {{ ref('stats__ez_dex_metrics_daily') }} C USING(
        block_date,
        blockchain
    )
    LEFT JOIN {{ ref('stats__ez_stablecoin_flows_daily') }}
    d USING(
        block_date,
        blockchain
    )
    LEFT JOIN {{ ref('stats__ez_cex_flows_daily') }}
    e USING(
        block_date,
        blockchain
    )
    LEFT JOIN {{ ref('stats__ez_tvl_chain_metrics_daily') }}
    f USING(
        block_date,
        blockchain
    )
    LEFT JOIN {{ ref('stats__ez_transfer_metrics_daily') }}
    g USING(
        block_date,
        blockchain
    )
WHERE
    LOWER(
        A.blockchain
    ) = 'arbitrum'
