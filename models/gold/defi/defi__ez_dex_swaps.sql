{{ config(
    materialized = 'view',
    persist_docs ={ "relation": true,
    "columns": true }
) }}

SELECT
    dex.blockchain,
    dex.platform,
    dex.block_number,
    dex.block_timestamp,
    dex.tx_hash,
    dex.trader,
    dex.token_in,
    p_in.symbol AS symbol_in,
    dex.amount_in_raw,
    CASE
        WHEN dex.blockchain = 'solana' THEN dex.amount_in_raw
        WHEN p_in.decimals IS NOT NULL THEN dex.amount_in_raw / power(
            10,
            p_in.decimals
        )
    END amount_in,
    ROUND(
        p_in.price * amount_in,
        2
    ) AS amount_in_usd,
    dex.token_out,
    p_out.symbol AS symbol_out,
    dex.amount_out_raw,
    CASE
        WHEN dex.blockchain = 'solana' THEN dex.amount_out_raw
        WHEN p_out.decimals IS NOT NULL THEN dex.amount_out_raw / power(
            10,
            p_out.decimals
        )
    END amount_out,
    ROUND(
        p_out.price * amount_out,
        2
    ) AS amount_out_usd,
    dex._log_id
FROM
    {{ ref('core__fact_dex_swaps') }}
    dex
    LEFT JOIN {{ ref('core__ez_hourly_prices') }}
    p_in
    ON REPLACE(
        dex.blockchain,
        'osmosis',
        'cosmos'
    ) = p_in.blockchain
    AND dex.token_in = p_in.token_address
    AND DATE_TRUNC(
        'hour',
        dex.block_timestamp
    ) = p_in.hour
    LEFT JOIN {{ ref('core__ez_hourly_prices') }}
    p_out
    ON REPLACE(
        dex.blockchain,
        'osmosis',
        'cosmos'
    ) = p_out.blockchain
    AND dex.token_out = p_out.token_address
    AND DATE_TRUNC(
        'hour',
        dex.block_timestamp
    ) = p_out.hour