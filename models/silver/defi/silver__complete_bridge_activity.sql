{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = ['blockchain','block_number','platform'],
    cluster_by = ['block_timestamp::DATE','blockchain','platform'],
    post_hook = "ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION ON EQUALITY(tx_hash, bridge_address, source_address, destination_address, source_chain, destination_chain, token_address, token_symbol), SUBSTRING(bridge_address, source_address, destination_address, source_chain, destination_chain, token_address, token_symbol)",
    tags = ['hourly']
) }}

WITH ethereum AS (

    SELECT
        'ethereum' AS blockchain,
        platform,
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
        modified_timestamp AS _inserted_timestamp,
        {{ dbt_utils.generate_surrogate_key(['ez_bridge_activity_id','blockchain']) }} AS complete_bridge_activity_id
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
        modified_timestamp AS _inserted_timestamp,
        {{ dbt_utils.generate_surrogate_key(['ez_bridge_activity_id','blockchain']) }} AS complete_bridge_activity_id
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
avalanche AS (
    SELECT
        'avalanche' AS blockchain,
        platform,
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
        modified_timestamp AS _inserted_timestamp,
        {{ dbt_utils.generate_surrogate_key(['ez_bridge_activity_id','blockchain']) }} AS complete_bridge_activity_id
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
        modified_timestamp AS _inserted_timestamp,
        {{ dbt_utils.generate_surrogate_key(['ez_bridge_activity_id','blockchain']) }} AS complete_bridge_activity_id
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
        modified_timestamp AS _inserted_timestamp,
        {{ dbt_utils.generate_surrogate_key(['ez_bridge_activity_id','blockchain']) }} AS complete_bridge_activity_id
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
        modified_timestamp AS _inserted_timestamp,
        {{ dbt_utils.generate_surrogate_key(['ez_bridge_activity_id','blockchain']) }} AS complete_bridge_activity_id
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
        modified_timestamp AS _inserted_timestamp,
        {{ dbt_utils.generate_surrogate_key(['ez_bridge_activity_id','blockchain']) }} AS complete_bridge_activity_id
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
        modified_timestamp AS _inserted_timestamp,
        {{ dbt_utils.generate_surrogate_key(['ez_bridge_activity_id','blockchain']) }} AS complete_bridge_activity_id
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
solana AS (
    SELECT
        'solana' AS blockchain,
        platform,
        block_id AS block_number,
        block_timestamp,
        tx_id AS tx_hash,
        CASE
            WHEN direction = 'outbound' THEN 'solana'
            ELSE NULL
        END AS source_chain,
        CASE
            WHEN direction = 'inbound' THEN 'solana'
            ELSE NULL
        END AS destination_chain,
        program_id AS bridge_address,
        CASE
            WHEN direction = 'outbound' THEN user_address
            ELSE NULL
        END AS source_address,
        CASE
            WHEN direction = 'inbound' THEN user_address
            ELSE NULL
        END AS destination_address,
        direction,
        mint AS token_address,
        NULL AS token_symbol,
        amount AS amount_raw,
        amount,
        NULL AS amount_usd,
        modified_timestamp AS _inserted_timestamp,
        {{ dbt_utils.generate_surrogate_key(['fact_bridge_activity_id','blockchain']) }} AS complete_bridge_activity_id
    FROM
        {{ source(
            'solana_defi',
            'fact_bridge_activity'
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
        block_number,
        block_timestamp,
        tx_hash,
        LOWER(source_chain_name) AS source_chain,
        LOWER(destination_chain_name) AS destination_chain,
        bridge_address,
        sender AS source_address,
        receiver AS destination_address,
        direction,
        token_address,
        NULL AS token_symbol,
        amount_unadj AS amount_raw,
        NULL AS amount,
        NULL AS amount_usd,
        modified_timestamp AS _inserted_timestamp,
        {{ dbt_utils.generate_surrogate_key(['fact_bridge_activity_id','blockchain']) }} AS complete_bridge_activity_id
    FROM
        {{ source(
            'aptos_defi',
            'fact_bridge_activity'
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
        NULL AS amount_usd,
        modified_timestamp AS _inserted_timestamp,
        {{ dbt_utils.generate_surrogate_key(['ez_bridge_activity_id','blockchain']) }} AS complete_bridge_activity_id
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
    COALESCE(
        b.token_symbol,
        p.symbol
    ) AS token_symbol,
    b.amount_raw,
    CASE
        WHEN b.blockchain = 'aptos'
        AND p.decimals IS NOT NULL THEN b.amount_raw / power(
            10,
            p.decimals
        )
        ELSE b.amount
    END AS amount,
    CASE
        WHEN b.blockchain IN (
            'ethereum',
            'optimism',
            'base',
            'arbitrum',
            'polygon',
            'bsc',
            'avalanche',
            'gnosis'
        ) THEN b.amount_usd
        ELSE ROUND(
            p.price * amount,
            2
        )
    END AS amount_usd,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    b._inserted_timestamp,
    complete_bridge_activity_id
FROM
    all_chains_bridge b
    LEFT JOIN {{ ref('price__ez_prices_hourly') }}
    p
    ON b.blockchain = p.blockchain
    AND b.token_address = p.token_address
    AND DATE_TRUNC(
        'hour',
        b.block_timestamp
    ) = p.hour
