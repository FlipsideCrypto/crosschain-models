{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = ['_unique_key'],
    cluster_by = ['block_timestamp::DATE','blockchain','platform'],
    post_hook = "ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION ON EQUALITY(tx_hash, contract_address, trader, token_in, token_out, symbol_in, symbol_out, _unique_key), SUBSTRING(trader, token_in, token_out, symbol_in, symbol_out)",
    tags = ['hourly','dex']
) }}

WITH ethereum AS (

    SELECT
        'ethereum' AS blockchain,
        platform,
        protocol,
        protocol_version,
        block_number,
        block_timestamp,
        tx_hash,
        contract_address,
        origin_from_address AS trader,
        token_in,
        symbol_in,
        amount_in_unadj AS amount_in_raw,
        amount_in,
        amount_in_usd,
        token_out,
        symbol_out,
        amount_out_unadj AS amount_out_raw,
        amount_out,
        amount_out_usd,
        token_in_is_verified,
        token_out_is_verified,
        CONCAT(
            tx_hash :: STRING,
            '-',
            event_index :: STRING
        ) AS _log_id,
        modified_timestamp AS _inserted_timestamp,
        {{ dbt_utils.generate_surrogate_key(['ez_dex_swaps_id','blockchain']) }} AS complete_dex_swaps_id,
        {{ dbt_utils.generate_surrogate_key(['blockchain','block_number','platform']) }} AS _unique_key
    FROM
        {{ source(
            'ethereum_defi',
            'ez_dex_swaps'
        ) }}

{% if is_incremental() and 'ethereum' not in var('HEAL_MODELS') %}
WHERE
    _inserted_timestamp >= (
        SELECT
            MAX(_inserted_timestamp) - INTERVAL '{{ var("LOOKBACK", "6 hours") }}'
        FROM
            {{ this }}
    )
{% endif %}
),
optimism AS (
    SELECT
        'optimism' AS blockchain,
        platform,
        protocol,
        protocol_version,
        block_number,
        block_timestamp,
        tx_hash,
        contract_address,
        origin_from_address AS trader,
        token_in,
        symbol_in,
        amount_in_unadj AS amount_in_raw,
        amount_in,
        amount_in_usd,
        token_out,
        symbol_out,
        amount_out_unadj AS amount_out_raw,
        amount_out,
        amount_out_usd,
        token_in_is_verified,
        token_out_is_verified,
        CONCAT(
            tx_hash :: STRING,
            '-',
            event_index :: STRING
        ) AS _log_id,
        modified_timestamp AS _inserted_timestamp,
        {{ dbt_utils.generate_surrogate_key(['ez_dex_swaps_id','blockchain']) }} AS complete_dex_swaps_id,
        {{ dbt_utils.generate_surrogate_key(['blockchain','block_number','platform']) }} AS _unique_key
    FROM
        {{ source(
            'optimism_defi',
            'ez_dex_swaps'
        ) }}

{% if is_incremental() and 'optimism' not in var('HEAL_MODELS') %}
WHERE
    _inserted_timestamp >= (
        SELECT
            MAX(_inserted_timestamp) - INTERVAL '{{ var("LOOKBACK", "6 hours") }}'
        FROM
            {{ this }}
    )
{% endif %}
),
avalanche AS (
    SELECT
        'avalanche' AS blockchain,
        platform,
        protocol,
        protocol_version,
        block_number,
        block_timestamp,
        tx_hash,
        contract_address,
        origin_from_address AS trader,
        token_in,
        symbol_in,
        amount_in_unadj AS amount_in_raw,
        amount_in,
        amount_in_usd,
        token_out,
        symbol_out,
        amount_out_unadj AS amount_out_raw,
        amount_out,
        amount_out_usd,
        token_in_is_verified,
        token_out_is_verified,
        CONCAT(
            tx_hash :: STRING,
            '-',
            event_index :: STRING
        ) AS _log_id,
        modified_timestamp AS _inserted_timestamp,
        {{ dbt_utils.generate_surrogate_key(['ez_dex_swaps_id','blockchain']) }} AS complete_dex_swaps_id,
        {{ dbt_utils.generate_surrogate_key(['blockchain','block_number','platform']) }} AS _unique_key
    FROM
        {{ source(
            'avalanche_defi',
            'ez_dex_swaps'
        ) }}

{% if is_incremental() and 'avalanche' not in var('HEAL_MODELS') %}
WHERE
    _inserted_timestamp >= (
        SELECT
            MAX(_inserted_timestamp) - INTERVAL '{{ var("LOOKBACK", "6 hours") }}'
        FROM
            {{ this }}
    )
{% endif %}
),
polygon AS (
    SELECT
        'polygon' AS blockchain,
        platform,
        protocol,
        protocol_version,
        block_number,
        block_timestamp,
        tx_hash,
        contract_address,
        origin_from_address AS trader,
        token_in,
        symbol_in,
        amount_in_unadj AS amount_in_raw,
        amount_in,
        amount_in_usd,
        token_out,
        symbol_out,
        amount_out_unadj AS amount_out_raw,
        amount_out,
        amount_out_usd,
        token_in_is_verified,
        token_out_is_verified,
        CONCAT(
            tx_hash :: STRING,
            '-',
            event_index :: STRING
        ) AS _log_id,
        modified_timestamp AS _inserted_timestamp,
        {{ dbt_utils.generate_surrogate_key(['ez_dex_swaps_id','blockchain']) }} AS complete_dex_swaps_id,
        {{ dbt_utils.generate_surrogate_key(['blockchain','block_number','platform']) }} AS _unique_key
    FROM
        {{ source(
            'polygon_defi',
            'ez_dex_swaps'
        ) }}

{% if is_incremental() and 'polygon' not in var('HEAL_MODELS') %}
WHERE
    _inserted_timestamp >= (
        SELECT
            MAX(_inserted_timestamp) - INTERVAL '{{ var("LOOKBACK", "6 hours") }}'
        FROM
            {{ this }}
    )
{% endif %}
),
bsc AS (
    SELECT
        'bsc' AS blockchain,
        platform,
        protocol,
        protocol_version,
        block_number,
        block_timestamp,
        tx_hash,
        contract_address,
        origin_from_address AS trader,
        token_in,
        symbol_in,
        amount_in_unadj AS amount_in_raw,
        amount_in,
        amount_in_usd,
        token_out,
        symbol_out,
        amount_out_unadj AS amount_out_raw,
        amount_out,
        amount_out_usd,
        token_in_is_verified,
        token_out_is_verified,
        CONCAT(
            tx_hash :: STRING,
            '-',
            event_index :: STRING
        ) AS _log_id,
        modified_timestamp AS _inserted_timestamp,
        {{ dbt_utils.generate_surrogate_key(['ez_dex_swaps_id','blockchain']) }} AS complete_dex_swaps_id,
        {{ dbt_utils.generate_surrogate_key(['blockchain','block_number','platform']) }} AS _unique_key
    FROM
        {{ source(
            'bsc_defi',
            'ez_dex_swaps'
        ) }}

{% if is_incremental() and 'bsc' not in var('HEAL_MODELS') %}
WHERE
    _inserted_timestamp >= (
        SELECT
            MAX(_inserted_timestamp) - INTERVAL '{{ var("LOOKBACK", "6 hours") }}'
        FROM
            {{ this }}
    )
{% endif %}
),
arbitrum AS (
    SELECT
        'arbitrum' AS blockchain,
        platform,
        protocol,
        protocol_version,
        block_number,
        block_timestamp,
        tx_hash,
        contract_address,
        origin_from_address AS trader,
        token_in,
        symbol_in,
        amount_in_unadj AS amount_in_raw,
        amount_in,
        amount_in_usd,
        token_out,
        symbol_out,
        amount_out_unadj AS amount_out_raw,
        amount_out,
        amount_out_usd,
        token_in_is_verified,
        token_out_is_verified,
        CONCAT(
            tx_hash :: STRING,
            '-',
            event_index :: STRING
        ) AS _log_id,
        modified_timestamp AS _inserted_timestamp,
        {{ dbt_utils.generate_surrogate_key(['ez_dex_swaps_id','blockchain']) }} AS complete_dex_swaps_id,
        {{ dbt_utils.generate_surrogate_key(['blockchain','block_number','platform']) }} AS _unique_key
    FROM
        {{ source(
            'arbitrum_defi',
            'ez_dex_swaps'
        ) }}

{% if is_incremental() and 'arbitrum' not in var('HEAL_MODELS') %}
WHERE
    _inserted_timestamp >= (
        SELECT
            MAX(_inserted_timestamp) - INTERVAL '{{ var("LOOKBACK", "6 hours") }}'
        FROM
            {{ this }}
    )
{% endif %}
),
base AS (
    SELECT
        'base' AS blockchain,
        platform,
        protocol,
        protocol_version,
        block_number,
        block_timestamp,
        tx_hash,
        contract_address,
        origin_from_address AS trader,
        token_in,
        symbol_in,
        amount_in_unadj AS amount_in_raw,
        amount_in,
        amount_in_usd,
        token_out,
        symbol_out,
        amount_out_unadj AS amount_out_raw,
        amount_out,
        amount_out_usd,
        token_in_is_verified,
        token_out_is_verified,
        CONCAT(
            tx_hash :: STRING,
            '-',
            event_index :: STRING
        ) AS _log_id,
        modified_timestamp AS _inserted_timestamp,
        {{ dbt_utils.generate_surrogate_key(['ez_dex_swaps_id','blockchain']) }} AS complete_dex_swaps_id,
        {{ dbt_utils.generate_surrogate_key(['blockchain','block_number','platform']) }} AS _unique_key
    FROM
        {{ source(
            'base_defi',
            'ez_dex_swaps'
        ) }}

{% if is_incremental() and 'base' not in var('HEAL_MODELS') %}
WHERE
    _inserted_timestamp >= (
        SELECT
            MAX(_inserted_timestamp) - INTERVAL '{{ var("LOOKBACK", "6 hours") }}'
        FROM
            {{ this }}
    )
{% endif %}
),
core AS (
    SELECT
        'core' AS blockchain,
        platform,
        protocol,
        protocol_version,
        block_number,
        block_timestamp,
        tx_hash,
        contract_address,
        origin_from_address AS trader,
        token_in,
        symbol_in,
        amount_in_unadj AS amount_in_raw,
        amount_in,
        amount_in_usd,
        token_out,
        symbol_out,
        amount_out_unadj AS amount_out_raw,
        amount_out,
        amount_out_usd,
        token_in_is_verified,
        token_out_is_verified,
        CONCAT(
            tx_hash :: STRING,
            '-',
            event_index :: STRING
        ) AS _log_id,
        modified_timestamp AS _inserted_timestamp,
        {{ dbt_utils.generate_surrogate_key(['ez_dex_swaps_id','blockchain']) }} AS complete_dex_swaps_id,
        {{ dbt_utils.generate_surrogate_key(['blockchain','block_number','platform']) }} AS _unique_key
    FROM
        {{ source(
            'core_defi',
            'ez_dex_swaps'
        ) }}

{% if is_incremental() and 'core' not in var('HEAL_MODELS') %}
WHERE
    _inserted_timestamp >= (
        SELECT
            MAX(_inserted_timestamp) - INTERVAL '{{ var("LOOKBACK", "6 hours") }}'
        FROM
            {{ this }}
    )
{% endif %}
),
gnosis AS (
    SELECT
        'gnosis' AS blockchain,
        platform,
        protocol,
        protocol_version,
        block_number,
        block_timestamp,
        tx_hash,
        contract_address,
        origin_from_address AS trader,
        token_in,
        symbol_in,
        amount_in_unadj AS amount_in_raw,
        amount_in,
        amount_in_usd,
        token_out,
        symbol_out,
        amount_out_unadj AS amount_out_raw,
        amount_out,
        amount_out_usd,
        token_in_is_verified,
        token_out_is_verified,
        CONCAT(
            tx_hash :: STRING,
            '-',
            event_index :: STRING
        ) AS _log_id,
        modified_timestamp AS _inserted_timestamp,
        {{ dbt_utils.generate_surrogate_key(['ez_dex_swaps_id','blockchain']) }} AS complete_dex_swaps_id,
        {{ dbt_utils.generate_surrogate_key(['blockchain','block_number','platform']) }} AS _unique_key
    FROM
        {{ source(
            'gnosis_defi',
            'ez_dex_swaps'
        ) }}

{% if is_incremental() and 'gnosis' not in var('HEAL_MODELS') %}
WHERE
    _inserted_timestamp >= (
        SELECT
            MAX(_inserted_timestamp) - INTERVAL '{{ var("LOOKBACK", "6 hours") }}'
        FROM
            {{ this }}
    )
{% endif %}
),
{# osmosis AS (
SELECT
    'osmosis' AS blockchain,
    'osmosis' platform,
    s.block_id AS block_number,
    s.block_timestamp,
    s.tx_id AS tx_hash,
    pool_address AS contract_address,
    trader AS trader,
    from_currency AS token_in,
    NULL AS symbol_in,
    from_amount AS amount_in_raw,
    CASE
        WHEN s.from_decimal IS NOT NULL THEN from_amount / power(
            10,
            s.from_decimal
        )
    END AS amount_in,
    NULL AS amount_in_usd,
    to_currency AS token_out,
    NULL AS symbol_out,
    to_amount AS amount_out_raw,
    CASE
        WHEN s.to_decimal IS NOT NULL THEN to_amount / power(
            10,
            s.to_decimal
        )
    END AS amount_out,
    NULL AS amount_out_usd,
    FALSE AS token_in_is_verified,
    FALSE AS token_out_is_verified,
    CONCAT(
        s.tx_id,
        '-',
        s._BODY_INDEX
    ) AS _log_id,
    s.modified_timestamp AS _inserted_timestamp,
    {{ dbt_utils.generate_surrogate_key(['fact_swaps_id','blockchain']) }} AS complete_dex_swaps_id,
    {{ dbt_utils.generate_surrogate_key(['blockchain','block_number','platform']) }} AS _unique_key
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

{% if is_incremental() and 'osmosis' not in var('HEAL_MODELS') %}
WHERE
    _inserted_timestamp >= (
        SELECT
            MAX(_inserted_timestamp) - INTERVAL '{{ var("LOOKBACK", "24 hours") }}'
        FROM
            {{ this }}
    )
{% endif %}
),
#}
solana AS (
    SELECT
        'solana' AS blockchain,
        swap_program AS platform,
        platform AS protocol,
        NULL AS protocol_version,
        block_id AS block_number,
        block_timestamp,
        tx_id AS tx_hash,
        program_id AS contract_address,
        swapper AS trader,
        swap_from_mint AS token_in,
        swap_from_symbol AS symbol_in,
        NULL AS amount_in_raw,
        swap_from_amount AS amount_in,
        swap_from_amount_usd AS amount_in_usd,
        swap_to_mint AS token_out,
        swap_to_symbol AS symbol_out,
        NULL AS amount_out_raw,
        swap_to_amount AS amount_out,
        swap_to_amount_usd AS amount_out_usd,
        swap_from_is_verified AS token_in_is_verified,
        swap_to_is_verified AS token_out_is_verified,
        ez_swaps_id AS _log_id,
        modified_timestamp AS _inserted_timestamp,
        {{ dbt_utils.generate_surrogate_key(['ez_swaps_id','blockchain']) }} AS complete_dex_swaps_id,
        {{ dbt_utils.generate_surrogate_key(['ez_swaps_id','blockchain']) }} AS _unique_key
    FROM
        {{ source(
            'solana_defi',
            'ez_dex_swaps'
        ) }}

{% if is_incremental() and 'solana' not in var('HEAL_MODELS') %}
WHERE
    _inserted_timestamp >= (
        SELECT
            MAX(_inserted_timestamp) - INTERVAL '{{ var("LOOKBACK", "24 hours") }}'
        FROM
            {{ this }}
    )
{% endif %}
),
near AS (
    SELECT
        'near' AS blockchain,
        platform,
        platform AS protocol,
        NULL AS protocol_version,
        block_id AS block_number,
        block_timestamp,
        tx_hash,
        NULL AS contract_address,
        trader,
        LOWER(token_in_contract) AS token_in,
        symbol_in,
        amount_in_raw,
        amount_in,
        amount_in_usd AS amount_in_usd,
        LOWER(token_out_contract) AS token_out,
        symbol_out,
        amount_out_raw,
        amount_out,
        amount_out_usd AS amount_out_usd,
        token_in_is_verified,
        token_out_is_verified,
        ez_dex_swaps_id AS _log_id,
        modified_timestamp AS _inserted_timestamp,
        {{ dbt_utils.generate_surrogate_key(['_log_id','blockchain']) }} AS complete_dex_swaps_id,
        {{ dbt_utils.generate_surrogate_key(['_log_id','blockchain']) }} AS _unique_key
    FROM
        {{ source(
            'near_defi',
            'ez_dex_swaps'
        ) }}

{% if is_incremental() and 'near' not in var('HEAL_MODELS') %}
WHERE
    _inserted_timestamp >= (
        SELECT
            MAX(_inserted_timestamp) - INTERVAL '{{ var("LOOKBACK", "24 hours") }}'
        FROM
            {{ this }}
    )
{% endif %}
),
aptos AS (
    SELECT
        'aptos' AS blockchain,
        platform,
        platform AS protocol,
        NULL AS protocol_version,
        block_number,
        block_timestamp,
        tx_hash,
        event_address AS contract_address,
        swapper,
        token_in,
        symbol_in,
        amount_in_unadj AS amount_in_raw,
        amount_in,
        amount_in_usd,
        token_out,
        symbol_out,
        amount_out_unadj AS amount_out_raw,
        amount_out,
        amount_out_usd,
        token_in_is_verified,
        token_out_is_verified,
        ez_dex_swaps_id AS _log_id,
        modified_timestamp AS _inserted_timestamp,
        {{ dbt_utils.generate_surrogate_key(['_log_id','blockchain']) }} AS complete_dex_swaps_id,
        {{ dbt_utils.generate_surrogate_key(['_log_id','blockchain']) }} AS _unique_key
    FROM
        {{ source(
            'aptos_defi',
            'ez_dex_swaps'
        ) }}

{% if is_incremental() and 'aptos' not in var('HEAL_MODELS') %}
WHERE
    _inserted_timestamp >= (
        SELECT
            MAX(_inserted_timestamp) - INTERVAL '{{ var("LOOKBACK", "24 hours") }}'
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
        core
    UNION ALL
    SELECT
        *
    FROM
        gnosis
    UNION ALL
        {# SELECT
        *
    FROM
        osmosis
    UNION ALL
        #}
    SELECT
        *
    FROM
        solana
    UNION ALL
    SELECT
        *
    FROM
        near
    UNION ALL
    SELECT
        *
    FROM
        aptos
)
SELECT
    d.blockchain,
    d.platform,
    d.protocol,
    d.protocol_version,
    d.block_number,
    d.block_timestamp,
    d.tx_hash,
    d.contract_address,
    d.trader,
    d.token_in,
    d.symbol_in,
    token_in_is_verified,
    d.amount_in_raw,
    d.amount_in,
    d.amount_in_usd,
    d.token_out,
    d.symbol_out,
    token_out_is_verified,
    d.amount_out_raw,
    d.amount_out,
    d.amount_out_usd,
    d._log_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    d._inserted_timestamp,
    complete_dex_swaps_id,
    d._unique_key
FROM
    all_chains_dex d
