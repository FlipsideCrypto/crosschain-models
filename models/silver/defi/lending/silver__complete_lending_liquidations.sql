{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = ['_unique_key'],
    cluster_by = ['block_timestamp::DATE'],
    tags = ['hourly']
) }}

WITH ethereum AS (

    SELECT
        'ethereum' AS blockchain,
        platform,
        block_number,
        block_timestamp,
        tx_hash,
        contract_address,
        protocol_market,
        borrower,
        liquidator,
        collateral_token,
        collateral_token_symbol,
        debt_token,
        debt_token_symbol,
        amount_unadj AS amount_raw,
        amount,
        amount_usd,
        modified_timestamp AS _inserted_timestamp,
        {{ dbt_utils.generate_surrogate_key(
            ['ez_lending_liquidations_id', 'blockchain']
        ) }} AS complete_lending_liquidations_id,
        {{ dbt_utils.generate_surrogate_key(['blockchain','block_number','platform']) }} AS _unique_key
    FROM
        {{ source(
            'ethereum_defi',
            'ez_lending_liquidations'
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
arbitrum AS (
    SELECT
        'arbitrum' AS blockchain,
        platform,
        block_number,
        block_timestamp,
        tx_hash,
        contract_address,
        protocol_market,
        borrower,
        liquidator,
        collateral_token,
        collateral_token_symbol,
        debt_token,
        debt_token_symbol,
        amount_unadj AS amount_raw,
        amount,
        amount_usd,
        modified_timestamp AS _inserted_timestamp,
        {{ dbt_utils.generate_surrogate_key(
            ['ez_lending_liquidations_id', 'blockchain']
        ) }} AS complete_lending_liquidations_id,
        {{ dbt_utils.generate_surrogate_key(['blockchain','block_number','platform']) }} AS _unique_key
    FROM
        {{ source(
            'arbitrum_defi',
            'ez_lending_liquidations'
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
optimism AS (
    SELECT
        'optimism' AS blockchain,
        platform,
        block_number,
        block_timestamp,
        tx_hash,
        contract_address,
        protocol_market,
        borrower,
        liquidator,
        collateral_token,
        collateral_token_symbol,
        debt_token,
        debt_token_symbol,
        amount_unadj AS amount_raw,
        amount,
        amount_usd,
        modified_timestamp AS _inserted_timestamp,
        {{ dbt_utils.generate_surrogate_key(
            ['ez_lending_liquidations_id', 'blockchain']
        ) }} AS complete_lending_liquidations_id,
        {{ dbt_utils.generate_surrogate_key(['blockchain','block_number','platform']) }} AS _unique_key
    FROM
        {{ source(
            'optimism_defi',
            'ez_lending_liquidations'
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
bsc AS (
    SELECT
        'bsc' AS blockchain,
        platform,
        block_number,
        block_timestamp,
        tx_hash,
        contract_address,
        protocol_market,
        borrower,
        liquidator,
        collateral_token,
        collateral_token_symbol,
        debt_token,
        debt_token_symbol,
        amount_unadj AS amount_raw,
        amount,
        amount_usd,
        modified_timestamp AS _inserted_timestamp,
        {{ dbt_utils.generate_surrogate_key(
            ['ez_lending_liquidations_id', 'blockchain']
        ) }} AS complete_lending_liquidations_id,
        {{ dbt_utils.generate_surrogate_key(['blockchain','block_number','platform']) }} AS _unique_key
    FROM
        {{ source(
            'bsc_defi',
            'ez_lending_liquidations'
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
polygon AS (
    SELECT
        'polygon' AS blockchain,
        platform,
        block_number,
        block_timestamp,
        tx_hash,
        contract_address,
        protocol_market,
        borrower,
        liquidator,
        collateral_token,
        collateral_token_symbol,
        debt_token,
        debt_token_symbol,
        amount_unadj AS amount_raw,
        amount,
        amount_usd,
        modified_timestamp AS _inserted_timestamp,
        {{ dbt_utils.generate_surrogate_key(
            ['ez_lending_liquidations_id', 'blockchain']
        ) }} AS complete_lending_liquidations_id,
        {{ dbt_utils.generate_surrogate_key(['blockchain','block_number','platform']) }} AS _unique_key
    FROM
        {{ source(
            'polygon_defi',
            'ez_lending_liquidations'
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
base AS (
    SELECT
        'base' AS blockchain,
        platform,
        block_number,
        block_timestamp,
        tx_hash,
        contract_address,
        protocol_market,
        borrower,
        liquidator,
        collateral_token,
        collateral_token_symbol,
        debt_token,
        debt_token_symbol,
        amount_unadj AS amount_raw,
        amount,
        amount_usd,
        modified_timestamp AS _inserted_timestamp,
        {{ dbt_utils.generate_surrogate_key(
            ['ez_lending_liquidations_id', 'blockchain']
        ) }} AS complete_lending_liquidations_id,
        {{ dbt_utils.generate_surrogate_key(['blockchain','block_number','platform']) }} AS _unique_key
    FROM
        {{ source(
            'base_defi',
            'ez_lending_liquidations'
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
avalanche AS (
    SELECT
        'avalanche' AS blockchain,
        platform,
        block_number,
        block_timestamp,
        tx_hash,
        contract_address,
        protocol_market,
        borrower,
        liquidator,
        collateral_token,
        collateral_token_symbol,
        debt_token,
        debt_token_symbol,
        amount_unadj AS amount_raw,
        amount,
        amount_usd,
        modified_timestamp AS _inserted_timestamp,
        {{ dbt_utils.generate_surrogate_key(
            ['ez_lending_liquidations_id', 'blockchain']
        ) }} AS complete_lending_liquidations_id,
        {{ dbt_utils.generate_surrogate_key(['blockchain','block_number','platform']) }} AS _unique_key
    FROM
        {{ source(
            'avalanche_defi',
            'ez_lending_liquidations'
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
gnosis AS (
    SELECT
        'gnosis' AS blockchain,
        platform,
        block_number,
        block_timestamp,
        tx_hash,
        contract_address,
        protocol_market,
        borrower,
        liquidator,
        collateral_token,
        collateral_token_symbol,
        debt_token,
        debt_token_symbol,
        amount_unadj AS amount_raw,
        amount,
        amount_usd,
        modified_timestamp AS _inserted_timestamp,
        {{ dbt_utils.generate_surrogate_key(
            ['ez_lending_liquidations_id', 'blockchain']
        ) }} AS complete_lending_liquidations_id,
        {{ dbt_utils.generate_surrogate_key(['blockchain','block_number','platform']) }} AS _unique_key
    FROM
        {{ source(
            'gnosis_defi',
            'ez_lending_liquidations'
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
all_chains_liquidations AS (
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
)
SELECT
    blockchain,
    platform,
    block_number,
    block_timestamp,
    tx_hash,
    contract_address,
    protocol_market,
    borrower,
    liquidator,
    collateral_token,
    collateral_token_symbol,
    debt_token,
    debt_token_symbol,
    amount_raw,
    amount,
    amount_usd,
    _inserted_timestamp,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    complete_lending_liquidations_id,
    _unique_key
FROM
    all_chains_liquidations d
