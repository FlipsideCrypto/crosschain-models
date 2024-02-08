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
        contract_address,
        origin_from_address AS trader,
        token_in,
        amount_in_unadj AS amount_in_raw,
        token_out,
        amount_out_unadj AS amount_out_raw,
        _log_id,
        COALESCE(inserted_timestamp,'2000-01-01') as inserted_timestamp,
        COALESCE(modified_timestamp,'2000-01-01') as modified_timestamp,
        COALESCE (ez_dex_swaps_id, {{ dbt_utils.generate_surrogate_key(['tx_hash','event_index']) }}) AS fact_dex_swaps_id
    FROM
        {{ source(
            'ethereum_defi',
            'ez_dex_swaps'
        ) }}
    UNION ALL
    SELECT
        'optimism' AS blockchain,
        platform,
        block_number,
        block_timestamp,
        tx_hash,
        contract_address,
        origin_from_address AS trader,
        token_in,
        amount_in_unadj AS amount_in_raw,
        token_out,
        amount_out_unadj AS amount_out_raw,
        _log_id,
        COALESCE(inserted_timestamp,'2000-01-01') as inserted_timestamp,
        COALESCE(modified_timestamp,'2000-01-01') as modified_timestamp,
        COALESCE (ez_dex_swaps_id, {{ dbt_utils.generate_surrogate_key(['tx_hash','event_index']) }}) AS fact_dex_swaps_id
    FROM
        {{ source(
            'optimism_defi',
            'ez_dex_swaps'
        ) }}
    UNION ALL
    SELECT
        'avalanche' AS blockchain,
        platform,
        block_number,
        block_timestamp,
        tx_hash,
        contract_address,
        origin_from_address AS trader,
        token_in,
        amount_in_unadj AS amount_in_raw,
        token_out,
        amount_out_unadj AS amount_out_raw,
        _log_id,
        COALESCE(inserted_timestamp,'2000-01-01') as inserted_timestamp,
        COALESCE(modified_timestamp,'2000-01-01') as modified_timestamp,
        COALESCE (ez_dex_swaps_id, {{ dbt_utils.generate_surrogate_key(['tx_hash','event_index']) }}) AS fact_dex_swaps_id
    FROM
        {{ source(
            'avalanche_defi',
            'ez_dex_swaps'
        ) }}
    UNION ALL
    SELECT
        'polygon' AS blockchain,
        platform,
        block_number,
        block_timestamp,
        tx_hash,
        contract_address,
        origin_from_address AS trader,
        token_in,
        amount_in_unadj AS amount_in_raw,
        token_out,
        amount_out_unadj AS amount_out_raw,
        _log_id,
        COALESCE(inserted_timestamp,'2000-01-01') as inserted_timestamp,
        COALESCE(modified_timestamp,'2000-01-01') as modified_timestamp,
        COALESCE (ez_dex_swaps_id, {{ dbt_utils.generate_surrogate_key(['tx_hash','event_index']) }}) AS fact_dex_swaps_id
    FROM
        {{ source(
            'polygon_defi',
            'ez_dex_swaps'
        ) }}
    UNION ALL
    SELECT
        'bsc' AS blockchain,
        platform,
        block_number,
        block_timestamp,
        tx_hash,
        contract_address,
        origin_from_address AS trader,
        token_in,
        amount_in_unadj AS amount_in_raw,
        token_out,
        amount_out_unadj AS amount_out_raw,
        _log_id,
        COALESCE(inserted_timestamp,'2000-01-01') as inserted_timestamp,
        COALESCE(modified_timestamp,'2000-01-01') as modified_timestamp,
        COALESCE (ez_dex_swaps_id, {{ dbt_utils.generate_surrogate_key(['tx_hash','event_index']) }}) AS fact_dex_swaps_id
    FROM
        {{ source(
            'bsc_defi',
            'ez_dex_swaps'
        ) }}
    UNION ALL
    SELECT
        'arbitrum' AS blockchain,
        platform,
        block_number,
        block_timestamp,
        tx_hash,
        contract_address,
        origin_from_address AS trader,
        token_in,
        amount_in_unadj AS amount_in_raw,
        token_out,
        amount_out_unadj AS amount_out_raw,
        _log_id,
        COALESCE(inserted_timestamp,'2000-01-01') as inserted_timestamp,
        COALESCE(modified_timestamp,'2000-01-01') as modified_timestamp,
        COALESCE (ez_dex_swaps_id, {{ dbt_utils.generate_surrogate_key(['tx_hash','event_index']) }}) AS fact_dex_swaps_id
    FROM
        {{ source(
            'arbitrum_defi',
            'ez_dex_swaps'
        ) }}
    UNION ALL
    SELECT
        'base' AS blockchain,
        platform,
        block_number,
        block_timestamp,
        tx_hash,
        contract_address,
        origin_from_address AS trader,
        token_in,
        amount_in_unadj AS amount_in_raw,
        token_out,
        amount_out_unadj AS amount_out_raw,
        _log_id,
        COALESCE(inserted_timestamp,'2000-01-01') as inserted_timestamp,
        COALESCE(modified_timestamp,'2000-01-01') as modified_timestamp,
        COALESCE (ez_dex_swaps_id, {{ dbt_utils.generate_surrogate_key(['tx_hash','event_index']) }}) AS fact_dex_swaps_id
    FROM
        {{ source(
            'base_defi',
            'ez_dex_swaps'
        ) }}
    UNION ALL
    SELECT
        'gnosis' AS blockchain,
        platform,
        block_number,
        block_timestamp,
        tx_hash,
        contract_address,
        origin_from_address AS trader,
        token_in,
        amount_in_unadj AS amount_in_raw,
        token_out,
        amount_out_unadj AS amount_out_raw,
        _log_id,
        COALESCE(inserted_timestamp,'2000-01-01') as inserted_timestamp,
        COALESCE(modified_timestamp,'2000-01-01') as modified_timestamp,
        COALESCE (ez_dex_swaps_id, {{ dbt_utils.generate_surrogate_key(['tx_hash','event_index']) }}) AS fact_dex_swaps_id
    FROM
        {{ source(
            'gnosis_defi',
            'ez_dex_swaps'
        ) }}
    UNION ALL
    SELECT
        'osmosis' AS blockchain,
        'osmosis' platform,
        s.block_id AS block_number,
        s.block_timestamp,
        s.tx_id AS tx_hash,
        pool_address AS contract_address,
        trader AS trader,
        from_currency AS token_in,
        from_amount AS amount_in_raw,
        to_currency AS token_out,
        to_amount AS amount_out_raw,
        CONCAT(
            s.tx_id,
            '-',
            s._BODY_INDEX
        ) AS _log_id,
        COALESCE(s.inserted_timestamp,'2000-01-01') as inserted_timestamp,
        COALESCE(s.modified_timestamp,'2000-01-01') as modified_timestamp,
        COALESCE(fact_swaps_id, {{ dbt_utils.generate_surrogate_key(['tx_hash','s._body_index']) }}) AS fact_dex_swaps_id
    FROM
        {{ source(
            'osmosis_defi',
            'fact_swaps'
        ) }}
        s
        LEFT JOIN {{ source(
            'osmosis_defi',
            'dim_liquidity_pools'
        ) }}
        p
        ON s.pool_id = p.pool_id
    UNION ALL
    SELECT
        'solana' AS blockchain,
        swap_program AS platform,
        block_id AS block_number,
        block_timestamp,
        tx_id AS tx_hash,
        program_id AS contract_address,
        swapper AS trader,
        LOWER(swap_from_mint) AS token_in,
        swap_from_amount AS amount_in_raw,
        LOWER(swap_to_mint) AS token_out,
        swap_to_amount AS amount_out_raw,
        _log_id,
        COALESCE(inserted_timestamp,'2000-01-01') as inserted_timestamp,
        COALESCE(modified_timestamp,'2000-01-01') as modified_timestamp,
        _log_id AS fact_dex_swaps_id
    FROM
        {{ source(
            'solana_defi',
            'fact_swaps'
        ) }}
    WHERE
        succeeded
    UNION ALL
    SELECT
        'near' AS blockchain,
        receiver_id AS platform,
        block_id AS block_number,
        block_timestamp,
        tx_hash,
        NULL AS contract_address,
        signer_id AS trader,
        token_in,
        amount_in_raw,
        token_out,
        amount_out_raw,
        fact_dex_swaps_id AS _log_id,
        COALESCE(inserted_timestamp,'2000-01-01') as inserted_timestamp,
        COALESCE(modified_timestamp,'2000-01-01') as modified_timestamp,
        fact_dex_swaps_id AS fact_dex_swaps_id
    FROM
        {{ source(
            'near_defi',
            'fact_dex_swaps'
        ) }}
    UNION ALL
    SELECT
        'flow' AS blockchain,
        NULL as platform,
        block_height AS block_number,
        block_timestamp,
        tx_id AS tx_hash,
        swap_contract AS contract_address,
        trader,
        token_in_contract AS token_in,
        token_in_amount AS amount_in_raw,
        token_out_contract AS token_out,
        token_out_amount AS amount_out_raw,
        ez_swaps_id AS _log_id,
        COALESCE(inserted_timestamp,'2000-01-01') as inserted_timestamp,
        COALESCE(modified_timestamp,'2000-01-01') as modified_timestamp,
        ez_swaps_id AS fact_dex_swaps_id
    FROM
        {{ source(
            'flow_defi',
            'ez_swaps'
        ) }}
)
SELECT
    blockchain,
    platform,
    block_number,
    block_timestamp,
    tx_hash,
    contract_address,
    trader,
    token_in,
    amount_in_raw,
    token_out,
    amount_out_raw,
    _log_id,
    inserted_timestamp,
    modified_timestamp,
    fact_dex_swaps_id
FROM
    base
