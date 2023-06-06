{{ config(
    materialized = 'view',
    persist_docs ={ "relation": true,
    "columns": true }
) }}

WITH base AS (

    SELECT
        'ethereum' AS blockchain,
        platform,
        block_number,
        block_timestamp,
        tx_hash,
        origin_from_address AS trader,
        token_in,
        amount_in_unadj AS amount_in_raw,
        token_out,
        amount_out_unadj AS amount_out_raw,
        _log_id
    FROM
        {{ source(
            'ethereum_silver_dex',
            'complete_dex_swaps'
        ) }}
    UNION ALL
    SELECT
        'optimism' AS blockchain,
        platform,
        block_number,
        block_timestamp,
        tx_hash,
        origin_from_address AS trader,
        token_in,
        amount_in_unadj AS amount_in_raw,
        token_out,
        amount_out_unadj AS amount_out_raw,
        _log_id
    FROM
        {{ source(
            'optimism_silver_dex',
            'complete_dex_swaps'
        ) }}
    UNION ALL
    SELECT
        'avalanche' AS blockchain,
        platform,
        block_number,
        block_timestamp,
        tx_hash,
        origin_from_address AS trader,
        token_in,
        amount_in_unadj AS amount_in_raw,
        token_out,
        amount_out_unadj AS amount_out_raw,
        _log_id
    FROM
        {{ source(
            'avalanche_silver_dex',
            'complete_dex_swaps'
        ) }}
    UNION ALL
    SELECT
        'polygon' AS blockchain,
        platform,
        block_number,
        block_timestamp,
        tx_hash,
        origin_from_address AS trader,
        token_in,
        amount_in_unadj AS amount_in_raw,
        token_out,
        amount_out_unadj AS amount_out_raw,
        _log_id
    FROM
        {{ source(
            'polygon_silver_dex',
            'complete_dex_swaps'
        ) }}
    UNION ALL
    SELECT
        'bsc' AS blockchain,
        platform,
        block_number,
        block_timestamp,
        tx_hash,
        origin_from_address AS trader,
        token_in,
        amount_in_unadj AS amount_in_raw,
        token_out,
        amount_out_unadj AS amount_out_raw,
        _log_id
    FROM
        {{ source(
            'bsc_silver_dex',
            'complete_dex_swaps'
        ) }}
    UNION ALL
    SELECT
        'osmosis' AS blockchain,
        'osmosis' platform,
        block_id AS block_number,
        block_timestamp,
        tx_id AS tx_hash,
        trader AS trader,
        from_currency AS token_in,
        from_amount AS amount_in,
        to_currency AS token_out,
        to_amount amount_out,
        CONCAT(
            tx_id,
            '-',
            _BODY_INDEX
        ) AS _log_id
    FROM
        {{ source(
            'osmosis_silver',
            'swaps'
        ) }}
    UNION ALL
    SELECT
        'solana' AS blockchain,
        swap_program AS platform,
        block_id AS block_number,
        block_timestamp,
        tx_id AS tx_hash,
        swapper AS trader,
        LOWER(swap_from_mint) AS token_in,
        swap_from_amount AS amount_in_raw,
        LOWER(swap_to_mint) AS token_out,
        swap_to_amount AS amount_out_raw,
        _log_id
    FROM
        {{ source(
            'solana_core',
            'fact_swaps'
        ) }}
    WHERE
        succeeded
    UNION ALL
    SELECT
        'near' AS blockchain,
        platform,
        block_id AS block_number,
        block_timestamp,
        tx_hash,
        trader,
        token_in,
        amount_in_raw,
        token_out,
        amount_out_raw,
        swap_id AS _log_id
    from {{ source(
        'near_silver',
        'dex_swaps_s3'
    ) }}
)
SELECT
    blockchain,
    platform,
    block_number,
    block_timestamp,
    tx_hash,
    trader,
    token_in,
    amount_in_raw,
    token_out,
    amount_out_raw,
    _log_id
FROM
    base
