{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = ['blockchain','block_number','platform'],
    cluster_by = ['block_timestamp::DATE']
) }}

WITH ethereum AS (

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
        inserted_timestamp,
        modified_timestamp,
        modified_timestamp AS _inserted_timestamp,
        {{ dbt_utils.generate_surrogate_key(['ez_dex_swaps_id','blockchain']) }} AS complete_dex_swaps_id
    FROM
        {{ source(
            'ethereum_defi',
            'ez_dex_swaps'
        ) }}

{% if is_incremental() and 'ethereum' not in var('HEAL_CURATED_MODEL') %}
WHERE
    _inserted_timestamp >= (
        SELECT
            MAX(_inserted_timestamp) - INTERVAL '36 hours' -- long lookback for reorgs (and delete)
        FROM
            {{ this }}
    )
{% endif %}
),
optimism AS (
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
        inserted_timestamp,
        modified_timestamp,
        modified_timestamp AS _inserted_timestamp,
        {{ dbt_utils.generate_surrogate_key(['ez_dex_swaps_id','blockchain']) }} AS complete_dex_swaps_id
    FROM
        {{ source(
            'optimism_defi',
            'ez_dex_swaps'
        ) }}

{% if is_incremental() and 'optimism' not in var('HEAL_CURATED_MODEL') %}
WHERE
    _inserted_timestamp >= (
        SELECT
            MAX(_inserted_timestamp) - INTERVAL '36 hours'
        FROM
            {{ this }}
    )
{% endif %}
),
avalanche AS (
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
        inserted_timestamp,
        modified_timestamp,
        modified_timestamp AS _inserted_timestamp,
        {{ dbt_utils.generate_surrogate_key(['ez_dex_swaps_id','blockchain']) }} AS complete_dex_swaps_id
    FROM
        {{ source(
            'avalanche_defi',
            'ez_dex_swaps'
        ) }}

{% if is_incremental() and 'avalanche' not in var('HEAL_CURATED_MODEL') %}
WHERE
    _inserted_timestamp >= (
        SELECT
            MAX(_inserted_timestamp) - INTERVAL '36 hours'
        FROM
            {{ this }}
    )
{% endif %}
),
polygon AS (
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
        inserted_timestamp,
        modified_timestamp,
        modified_timestamp AS _inserted_timestamp,
        {{ dbt_utils.generate_surrogate_key(['ez_dex_swaps_id','blockchain']) }} AS complete_dex_swaps_id
    FROM
        {{ source(
            'polygon_defi',
            'ez_dex_swaps'
        ) }}

{% if is_incremental() and 'polygon' not in var('HEAL_CURATED_MODEL') %}
WHERE
    _inserted_timestamp >= (
        SELECT
            MAX(_inserted_timestamp) - INTERVAL '36 hours'
        FROM
            {{ this }}
    )
{% endif %}
),
bsc AS (
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
        inserted_timestamp,
        modified_timestamp,
        modified_timestamp AS _inserted_timestamp,
        {{ dbt_utils.generate_surrogate_key(['ez_dex_swaps_id','blockchain']) }} AS complete_dex_swaps_id
    FROM
        {{ source(
            'bsc_defi',
            'ez_dex_swaps'
        ) }}

{% if is_incremental() and 'bsc' not in var('HEAL_CURATED_MODEL') %}
WHERE
    _inserted_timestamp >= (
        SELECT
            MAX(_inserted_timestamp) - INTERVAL '36 hours'
        FROM
            {{ this }}
    )
{% endif %}
),
arbitrum AS (
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
        inserted_timestamp,
        modified_timestamp,
        modified_timestamp AS _inserted_timestamp,
        {{ dbt_utils.generate_surrogate_key(['ez_dex_swaps_id','blockchain']) }} AS complete_dex_swaps_id
    FROM
        {{ source(
            'arbitrum_defi',
            'ez_dex_swaps'
        ) }}

{% if is_incremental() and 'arbitrum' not in var('HEAL_CURATED_MODEL') %}
WHERE
    _inserted_timestamp >= (
        SELECT
            MAX(_inserted_timestamp) - INTERVAL '36 hours'
        FROM
            {{ this }}
    )
{% endif %}
),
base AS (
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
        inserted_timestamp,
        modified_timestamp,
        modified_timestamp AS _inserted_timestamp,
        {{ dbt_utils.generate_surrogate_key(['ez_dex_swaps_id','blockchain']) }} AS complete_dex_swaps_id
    FROM
        {{ source(
            'base_defi',
            'ez_dex_swaps'
        ) }}

{% if is_incremental() and 'base' not in var('HEAL_CURATED_MODEL') %}
WHERE
    _inserted_timestamp >= (
        SELECT
            MAX(_inserted_timestamp) - INTERVAL '36 hours'
        FROM
            {{ this }}
    )
{% endif %}
),
gnosis AS (
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
        inserted_timestamp,
        modified_timestamp,
        modified_timestamp AS _inserted_timestamp,
        {{ dbt_utils.generate_surrogate_key(['ez_dex_swaps_id','blockchain']) }} AS complete_dex_swaps_id
    FROM
        {{ source(
            'gnosis_defi',
            'ez_dex_swaps'
        ) }}

{% if is_incremental() and 'gnosis' not in var('HEAL_CURATED_MODEL') %}
WHERE
    _inserted_timestamp >= (
        SELECT
            MAX(_inserted_timestamp) - INTERVAL '36 hours'
        FROM
            {{ this }}
    )
{% endif %}
),
osmosis AS (
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
        s.inserted_timestamp,
        s.modified_timestamp,
        s.modified_timestamp AS _inserted_timestamp,
        {{ dbt_utils.generate_surrogate_key(['fact_swaps_id','blockchain']) }} AS complete_dex_swaps_id
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

{% if is_incremental() and 'osmosis' not in var('HEAL_CURATED_MODEL') %}
WHERE
    _inserted_timestamp >= (
        SELECT
            MAX(_inserted_timestamp) - INTERVAL '36 hours'
        FROM
            {{ this }}
    )
{% endif %}
),
solana AS (
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
        inserted_timestamp,
        modified_timestamp,
        modified_timestamp AS _inserted_timestamp,
        {{ dbt_utils.generate_surrogate_key(['_log_id','blockchain']) }} AS complete_dex_swaps_id
    FROM
        {{ source(
            'solana_defi',
            'fact_swaps'
        ) }}
    WHERE
        succeeded

{% if is_incremental() and 'solana' not in var('HEAL_CURATED_MODEL') %}
WHERE
    _inserted_timestamp >= (
        SELECT
            MAX(_inserted_timestamp) - INTERVAL '36 hours'
        FROM
            {{ this }}
    )
{% endif %}
),
near AS (
    SELECT
        'near' AS blockchain,
        platform,
        block_id AS block_number,
        block_timestamp,
        tx_hash,
        NULL AS contract_address,
        trader,
        token_in,
        amount_in_raw,
        token_out,
        amount_out_raw,
        swap_id AS _log_id,
        inserted_timestamp,
        modified_timestamp,
        modified_timestamp AS _inserted_timestamp,
        {{ dbt_utils.generate_surrogate_key(['swap_id','blockchain']) }} AS complete_dex_swaps_id
    FROM
        {{ source(
            'near_defi',
            'ez_dex_swaps'
        ) }}

{% if is_incremental() and 'near' not in var('HEAL_CURATED_MODEL') %}
WHERE
    _inserted_timestamp >= (
        SELECT
            MAX(_inserted_timestamp) - INTERVAL '36 hours'
        FROM
            {{ this }}
    )
{% endif %}
),
all_chains_dex AS (
    SELECT
        *
    FROM
        ethereum
    UNION ALL
    SELECT
        *
    FROM
        optimism
    UNION ALL
    SELECT
        *
    FROM
        avalanche
    UNION ALL
    SELECT
        *
    FROM
        polygon
    UNION ALL
    SELECT
        *
    FROM
        bsc
    UNION ALL
    SELECT
        *
    FROM
        arbitrum
    UNION ALL
    SELECT
        *
    FROM
        base
    UNION ALL
    SELECT
        *
    FROM
        gnosis
    UNION ALL
    SELECT
        *
    FROM
        osmosis
    UNION ALL
    SELECT
        *
    FROM
        solana
    UNION ALL
    SELECT
        *
    FROM
        near
)
SELECT
    d.blockchain,
    d.platform,
    d.block_number,
    d.block_timestamp,
    d.tx_hash,
    d.contract_address,
    d.trader,
    d.token_in,
    COALESCE(
        p_in.symbol,
        c_in.symbol
    ) AS symbol_in,
    d.amount_in_raw,
    CASE
        WHEN d.blockchain = 'solana' THEN d.amount_in_raw
        WHEN COALESCE(
            p_in.decimals,
            c_in.decimals
        ) IS NOT NULL THEN d.amount_in_raw / power(
            10,
            COALESCE(
                p_in.decimals,
                c_in.decimals
            )
        )
    END amount_in,
    ROUND(
        p_in.price * amount_in,
        2
    ) AS amount_in_usd,
    d.token_out,
    COALESCE(
        p_out.symbol,
        c_out.symbol
    ) AS symbol_out,
    d.amount_out_raw,
    CASE
        WHEN d.blockchain = 'solana' THEN d.amount_out_raw
        WHEN COALESCE(
            p_out.decimals,
            c_out.decimals
        ) IS NOT NULL THEN d.amount_out_raw / power(
            10,
            COALESCE(
                p_out.decimals,
                c_out.decimals
            )
        )
    END amount_out,
    ROUND(
        p_out.price * amount_out,
        2
    ) AS amount_out_usd,
    d._log_id,
    GREATEST(d.inserted_timestamp, p_in.inserted_timestamp, p_out.inserted_timestamp, c_in.inserted_timestamp, c_out.inserted_timestamp) AS inserted_timestamp,
    GREATEST(d.modified_timestamp, p_in.modified_timestamp, p_out.modified_timestamp, c_in.modified_timestamp, c_out.modified_timestamp) AS modified_timestamp,
    d._inserted_timestamp,
    complete_dex_swaps_id
FROM
    all_chains_dex d
    LEFT JOIN {{ ref('price__ez_hourly_token_prices') }}
    p_in
    ON REPLACE(
        d.blockchain,
        'osmosis',
        'cosmos'
    ) = p_in.blockchain
    AND d.token_in = p_in.token_address
    AND DATE_TRUNC(
        'hour',
        d.block_timestamp
    ) = p_in.hour
    LEFT JOIN {{ ref('price__ez_hourly_token_prices') }}
    p_out
    ON REPLACE(
        d.blockchain,
        'osmosis',
        'cosmos'
    ) = p_out.blockchain
    AND d.token_out = p_out.token_address
    AND DATE_TRUNC(
        'hour',
        d.block_timestamp
    ) = p_out.hour
    LEFT JOIN {{ ref('core__dim_contracts') }}
    c_in
    ON REPLACE(
        d.blockchain,
        'osmosis',
        'cosmos'
    ) = c_in.blockchain
    AND d.token_in = c_in.address
    LEFT JOIN {{ ref('core__dim_contracts') }}
    c_out
    ON REPLACE(
        d.blockchain,
        'osmosis',
        'cosmos'
    ) = c_out.blockchain
    AND d.token_out = c_out.address
