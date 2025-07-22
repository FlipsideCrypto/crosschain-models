{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = ['_unique_key'],
    cluster_by = ['block_timestamp::DATE','blockchain','platform'],
    post_hook = "ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION ON EQUALITY(tx_hash, bridge_address, source_address, destination_address, source_chain, destination_chain, token_address, token_symbol), SUBSTRING(bridge_address, source_address, destination_address, source_chain, destination_chain, token_address, token_symbol)",
    tags = ['hourly','bridge']
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
        blockchain AS source_chain,
        destination_chain,
        bridge_address,
        sender AS source_address,
        destination_chain_receiver AS destination_address,
        'outbound' AS direction,
        token_address,
        token_symbol,
        amount_unadj AS amount_raw,
        amount,
        amount_usd,
        token_is_verified,
        modified_timestamp AS _inserted_timestamp,
        {{ dbt_utils.generate_surrogate_key(['ez_bridge_activity_id','blockchain']) }} AS complete_bridge_activity_id,
        {{ dbt_utils.generate_surrogate_key(['blockchain','block_number','platform']) }} AS _unique_key
    FROM
        {{ source(
            'ethereum_defi',
            'ez_bridge_activity'
        ) }}

{% if is_incremental() and 'ethereum' not in var('HEAL_MODELS') %}
WHERE
    _inserted_timestamp >= (
        SELECT
            MAX(_inserted_timestamp) - INTERVAL '{{ var("LOOKBACK", "24 hours") }}'
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
        blockchain AS source_chain,
        destination_chain,
        bridge_address,
        sender AS source_address,
        destination_chain_receiver AS destination_address,
        'outbound' AS direction,
        token_address,
        token_symbol,
        amount_unadj AS amount_raw,
        amount,
        amount_usd,
        token_is_verified,
        modified_timestamp AS _inserted_timestamp,
        {{ dbt_utils.generate_surrogate_key(['ez_bridge_activity_id','blockchain']) }} AS complete_bridge_activity_id,
        {{ dbt_utils.generate_surrogate_key(['blockchain','block_number','platform']) }} AS _unique_key
    FROM
        {{ source(
            'optimism_defi',
            'ez_bridge_activity'
        ) }}

{% if is_incremental() and 'optimism' not in var('HEAL_MODELS') %}
WHERE
    _inserted_timestamp >= (
        SELECT
            MAX(_inserted_timestamp) - INTERVAL '{{ var("LOOKBACK", "24 hours") }}'
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
        blockchain AS source_chain,
        destination_chain,
        bridge_address,
        sender AS source_address,
        destination_chain_receiver AS destination_address,
        'outbound' AS direction,
        token_address,
        token_symbol,
        amount_unadj AS amount_raw,
        amount,
        amount_usd,
        token_is_verified,
        modified_timestamp AS _inserted_timestamp,
        {{ dbt_utils.generate_surrogate_key(['ez_bridge_activity_id','blockchain']) }} AS complete_bridge_activity_id,
        {{ dbt_utils.generate_surrogate_key(['blockchain','block_number','platform']) }} AS _unique_key
    FROM
        {{ source(
            'core_defi',
            'ez_bridge_activity'
        ) }}

{% if is_incremental() and 'core' not in var('HEAL_MODELS') %}
WHERE
    _inserted_timestamp >= (
        SELECT
            MAX(_inserted_timestamp) - INTERVAL '{{ var("LOOKBACK", "24 hours") }}'
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
        blockchain AS source_chain,
        destination_chain,
        bridge_address,
        sender AS source_address,
        destination_chain_receiver AS destination_address,
        'outbound' AS direction,
        token_address,
        token_symbol,
        amount_unadj AS amount_raw,
        amount,
        amount_usd,
        token_is_verified,
        modified_timestamp AS _inserted_timestamp,
        {{ dbt_utils.generate_surrogate_key(['ez_bridge_activity_id','blockchain']) }} AS complete_bridge_activity_id,
        {{ dbt_utils.generate_surrogate_key(['blockchain','block_number','platform']) }} AS _unique_key
    FROM
        {{ source(
            'avalanche_defi',
            'ez_bridge_activity'
        ) }}

{% if is_incremental() and 'avalanche' not in var('HEAL_MODELS') %}
WHERE
    _inserted_timestamp >= (
        SELECT
            MAX(_inserted_timestamp) - INTERVAL '{{ var("LOOKBACK", "24 hours") }}'
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
        blockchain AS source_chain,
        destination_chain,
        bridge_address,
        sender AS source_address,
        destination_chain_receiver AS destination_address,
        'outbound' AS direction,
        token_address,
        token_symbol,
        amount_unadj AS amount_raw,
        amount,
        amount_usd,
        token_is_verified,
        modified_timestamp AS _inserted_timestamp,
        {{ dbt_utils.generate_surrogate_key(['ez_bridge_activity_id','blockchain']) }} AS complete_bridge_activity_id,
        {{ dbt_utils.generate_surrogate_key(['blockchain','block_number','platform']) }} AS _unique_key
    FROM
        {{ source(
            'polygon_defi',
            'ez_bridge_activity'
        ) }}

{% if is_incremental() and 'polygon' not in var('HEAL_MODELS') %}
WHERE
    _inserted_timestamp >= (
        SELECT
            MAX(_inserted_timestamp) - INTERVAL '{{ var("LOOKBACK", "24 hours") }}'
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
        blockchain AS source_chain,
        destination_chain,
        bridge_address,
        sender AS source_address,
        destination_chain_receiver AS destination_address,
        'outbound' AS direction,
        token_address,
        token_symbol,
        amount_unadj AS amount_raw,
        amount,
        amount_usd,
        token_is_verified,
        modified_timestamp AS _inserted_timestamp,
        {{ dbt_utils.generate_surrogate_key(['ez_bridge_activity_id','blockchain']) }} AS complete_bridge_activity_id,
        {{ dbt_utils.generate_surrogate_key(['blockchain','block_number','platform']) }} AS _unique_key
    FROM
        {{ source(
            'bsc_defi',
            'ez_bridge_activity'
        ) }}

{% if is_incremental() and 'bsc' not in var('HEAL_MODELS') %}
WHERE
    _inserted_timestamp >= (
        SELECT
            MAX(_inserted_timestamp) - INTERVAL '{{ var("LOOKBACK", "24 hours") }}'
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
        blockchain AS source_chain,
        destination_chain,
        bridge_address,
        sender AS source_address,
        destination_chain_receiver AS destination_address,
        'outbound' AS direction,
        token_address,
        token_symbol,
        amount_unadj AS amount_raw,
        amount,
        amount_usd,
        token_is_verified,
        modified_timestamp AS _inserted_timestamp,
        {{ dbt_utils.generate_surrogate_key(['ez_bridge_activity_id','blockchain']) }} AS complete_bridge_activity_id,
        {{ dbt_utils.generate_surrogate_key(['blockchain','block_number','platform']) }} AS _unique_key
    FROM
        {{ source(
            'arbitrum_defi',
            'ez_bridge_activity'
        ) }}

{% if is_incremental() and 'arbitrum' not in var('HEAL_MODELS') %}
WHERE
    _inserted_timestamp >= (
        SELECT
            MAX(_inserted_timestamp) - INTERVAL '{{ var("LOOKBACK", "24 hours") }}'
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
        blockchain AS source_chain,
        destination_chain,
        bridge_address,
        sender AS source_address,
        destination_chain_receiver AS destination_address,
        'outbound' AS direction,
        token_address,
        token_symbol,
        amount_unadj AS amount_raw,
        amount,
        amount_usd,
        token_is_verified,
        modified_timestamp AS _inserted_timestamp,
        {{ dbt_utils.generate_surrogate_key(['ez_bridge_activity_id','blockchain']) }} AS complete_bridge_activity_id,
        {{ dbt_utils.generate_surrogate_key(['blockchain','block_number','platform']) }} AS _unique_key
    FROM
        {{ source(
            'base_defi',
            'ez_bridge_activity'
        ) }}

{% if is_incremental() and 'base' not in var('HEAL_MODELS') %}
WHERE
    _inserted_timestamp >= (
        SELECT
            MAX(_inserted_timestamp) - INTERVAL '{{ var("LOOKBACK", "24 hours") }}'
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
        blockchain AS source_chain,
        destination_chain,
        bridge_address,
        sender AS source_address,
        destination_chain_receiver AS destination_address,
        'outbound' AS direction,
        token_address,
        token_symbol,
        amount_unadj AS amount_raw,
        amount,
        amount_usd,
        token_is_verified,
        modified_timestamp AS _inserted_timestamp,
        {{ dbt_utils.generate_surrogate_key(['ez_bridge_activity_id','blockchain']) }} AS complete_bridge_activity_id,
        {{ dbt_utils.generate_surrogate_key(['blockchain','block_number','platform']) }} AS _unique_key
    FROM
        {{ source(
            'gnosis_defi',
            'ez_bridge_activity'
        ) }}

{% if is_incremental() and 'gnosis' not in var('HEAL_MODELS') %}
WHERE
    _inserted_timestamp >= (
        SELECT
            MAX(_inserted_timestamp) - INTERVAL '{{ var("LOOKBACK", "24 hours") }}'
        FROM
            {{ this }}
    )
{% endif %}
),
ink AS (
    SELECT
        'ink' AS blockchain,
        platform,
        protocol,
        protocol_version,
        block_number,
        block_timestamp,
        tx_hash,
        blockchain AS source_chain,
        destination_chain,
        bridge_address,
        sender AS source_address,
        destination_chain_receiver AS destination_address,
        'outbound' AS direction,
        token_address,
        token_symbol,
        amount_unadj AS amount_raw,
        amount,
        amount_usd,
        token_is_verified,
        modified_timestamp AS _inserted_timestamp,
        {{ dbt_utils.generate_surrogate_key(['ez_bridge_activity_id','blockchain']) }} AS complete_bridge_activity_id,
        {{ dbt_utils.generate_surrogate_key(['blockchain','block_number','platform']) }} AS _unique_key
    FROM
        {{ source(
            'ink_defi',
            'ez_bridge_activity'
        ) }}

{% if is_incremental() and 'ink' not in var('HEAL_MODELS') %}
WHERE
    _inserted_timestamp >= (
        SELECT
            MAX(_inserted_timestamp) - INTERVAL '{{ var("LOOKBACK", "24 hours") }}'
        FROM
            {{ this }}
    )
{% endif %}
),
solana AS (
    SELECT
        'solana' AS blockchain,
        platform,
        platform AS protocol,
        NULL AS protocol_version,
        block_id AS block_number,
        block_timestamp,
        tx_id AS tx_hash,
        source_chain,
        destination_chain,
        program_id AS bridge_address,
        source_address,
        destination_address,
        direction,
        mint AS token_address,
        symbol AS token_symbol,
        amount AS amount_raw,
        amount,
        amount_usd,
        token_is_verified,
        modified_timestamp AS _inserted_timestamp,
        {{ dbt_utils.generate_surrogate_key(['ez_bridge_activity_id','blockchain']) }} AS complete_bridge_activity_id,
        {{ dbt_utils.generate_surrogate_key(['blockchain','block_number','platform']) }} AS _unique_key
    FROM
        {{ source(
            'solana_defi',
            'ez_bridge_activity'
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
aptos AS (
    SELECT
        'aptos' AS blockchain,
        platform,
        platform AS protocol,
        NULL AS protocol_version,
        block_number,
        block_timestamp,
        tx_hash,
        LOWER(source_chain) AS source_chain,
        LOWER(destination_chain) AS destination_chain,
        bridge_address,
        sender AS source_address,
        receiver AS destination_address,
        direction,
        token_address,
        symbol AS token_symbol,
        amount_unadj AS amount_raw,
        amount AS amount,
        amount_in_usd AS amount_usd,
        token_is_verified,
        modified_timestamp AS _inserted_timestamp,
        {{ dbt_utils.generate_surrogate_key(['ez_bridge_activity_id','blockchain']) }} AS complete_bridge_activity_id,
        {{ dbt_utils.generate_surrogate_key(['blockchain','block_number','platform']) }} AS _unique_key
    FROM
        {{ source(
            'aptos_defi',
            'ez_bridge_activity'
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
near AS (
    SELECT
        'near' AS blockchain,
        platform,
        platform AS protocol,
        NULL AS protocol_version,
        block_id AS block_number,
        block_timestamp,
        tx_hash,
        LOWER(source_chain) AS source_chain,
        LOWER(destination_chain) AS destination_chain,
        bridge_address,
        source_address,
        destination_address,
        direction,
        token_address,
        symbol AS token_symbol,
        amount_unadj AS amount_raw,
        amount,
        amount_usd,
        token_is_verified,
        modified_timestamp AS _inserted_timestamp,
        {{ dbt_utils.generate_surrogate_key(['ez_bridge_activity_id','blockchain']) }} AS complete_bridge_activity_id,
        complete_bridge_activity_id AS _unique_key
    FROM
        {{ source(
            'near_defi',
            'ez_bridge_activity'
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
all_chains_bridge AS (
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
        core
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
        ink
    UNION ALL
    SELECT
        *
    FROM
        solana
    UNION ALL
    SELECT
        *
    FROM
        aptos
    UNION ALL
    SELECT
        *
    FROM
        near
)
SELECT
    b.blockchain,
    b.platform,
    b.protocol,
    b.protocol_version,
    b.block_number,
    b.block_timestamp,
    b.tx_hash,
    b.source_chain,
    b.destination_chain,
    b.bridge_address,
    b.source_address,
    b.destination_address,
    b.direction,
    b.token_address,
    b.token_symbol,
    b.amount_raw,
    amount,
    amount_usd,
    b.token_is_verified,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    b._inserted_timestamp,
    b._unique_key,
    complete_bridge_activity_id
FROM
    all_chains_bridge b
