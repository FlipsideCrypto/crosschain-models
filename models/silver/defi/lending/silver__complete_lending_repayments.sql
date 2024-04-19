{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = ['_unique_key'],
    cluster_by = ['block_timestamp::DATE']
) }}

WITH ethereum as (

    SELECT
        'ethereum' as blockchain,
        platform,
        block_number,
        block_timestamp,
        tx_hash,
        contract_address,
        protocol_market,
        borrower,
        payer,
        token_address,
        token_symbol,
        amount_unadj as amount_raw,
        amount,
        amount_usd,
        modified_timestamp as _inserted_timestamp,
        {{ dbt_utils.generate_surrogate_key(
                ['ez_lending_repayments_id', 'blockchain']
        )}} AS complete_lending_repayments_id,
        {{ dbt_utils.generate_surrogate_key(['blockchain','block_number','platform']) }} AS _unique_key
    FROM 
        {{ source(
            'ethereum_defi',
            'ez_lending_repayments'
        ) }}
{% if is_incremental() and 'ethereum' not in var('HEAL_CURATED_MODEL') %}
WHERE
    _inserted_timestamp >= (
        SELECT
            MAX(_inserted_timestamp) - INTERVAL '36 hours'
        FROM
            {{ this }}
    )
{% endif %}

),
arbitrum as (

    SELECT
        'arbitrum' as blockchain,
        platform,
        block_number,
        block_timestamp,
        tx_hash,
        contract_address,
        protocol_market,
        borrower,
        payer,
        token_address,
        token_symbol,
        amount_unadj as amount_raw,
        amount,
        amount_usd,
        modified_timestamp as _inserted_timestamp,
        {{ dbt_utils.generate_surrogate_key(
                ['ez_lending_repayments_id', 'blockchain']
        )}} AS complete_lending_repayments_id,
        {{ dbt_utils.generate_surrogate_key(['blockchain','block_number','platform']) }} AS _unique_key
    FROM 
        {{ source(
            'arbitrum_defi',
            'ez_lending_repayments'
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
optimism as (

    SELECT
        'optimism' as blockchain,
        platform,
        block_number,
        block_timestamp,
        tx_hash,
        contract_address,
        protocol_market,
        borrower,
        payer,
        token_address,
        token_symbol,
        amount_unadj as amount_raw,
        amount,
        amount_usd,
        modified_timestamp as _inserted_timestamp,
        {{ dbt_utils.generate_surrogate_key(
                ['ez_lending_repayments_id', 'blockchain']
        )}} AS complete_lending_repayments_id,
        {{ dbt_utils.generate_surrogate_key(['blockchain','block_number','platform']) }} AS _unique_key
    FROM 
        {{ source(
            'optimism_defi',
            'ez_lending_repayments'
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
bsc as (

    SELECT
        'bsc' as blockchain,
        platform,
        block_number,
        block_timestamp,
        tx_hash,
        contract_address,
        protocol_market,
        borrower,
        payer,
        token_address,
        token_symbol,
        amount_unadj as amount_raw,
        amount,
        amount_usd,
        modified_timestamp as _inserted_timestamp,
        {{ dbt_utils.generate_surrogate_key(
                ['ez_lending_repayments_id', 'blockchain']
        )}} AS complete_lending_repayments_id,
        {{ dbt_utils.generate_surrogate_key(['blockchain','block_number','platform']) }} AS _unique_key
    FROM 
        {{ source(
            'bsc_defi',
            'ez_lending_repayments'
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
polygon as (

    SELECT
        'polygon' as blockchain,
        platform,
        block_number,
        block_timestamp,
        tx_hash,
        contract_address,
        protocol_market,
        borrower,
        payer,
        token_address,
        token_symbol,
        amount_unadj as amount_raw,
        amount,
        amount_usd,
        modified_timestamp as _inserted_timestamp,
        {{ dbt_utils.generate_surrogate_key(
                ['ez_lending_repayments_id', 'blockchain']
        )}} AS complete_lending_repayments_id,
        {{ dbt_utils.generate_surrogate_key(['blockchain','block_number','platform']) }} AS _unique_key
    FROM 
        {{ source(
            'polygon_defi',
            'ez_lending_repayments'
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
base as (

    SELECT
        'base' as blockchain,
        platform,
        block_number,
        block_timestamp,
        tx_hash,
        contract_address,
        protocol_market,
        borrower,
        payer,
        token_address,
        token_symbol,
        amount_unadj as amount_raw,
        amount,
        amount_usd,
        modified_timestamp as _inserted_timestamp,
        {{ dbt_utils.generate_surrogate_key(
                ['ez_lending_repayments_id', 'blockchain']
        )}} AS complete_lending_repayments_id,
        {{ dbt_utils.generate_surrogate_key(['blockchain','block_number','platform']) }} AS _unique_key
    FROM 
        {{ source(
            'base_defi',
            'ez_lending_repayments'
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
avalanche as (

    SELECT
        'avalanche' as blockchain,
        platform,
        block_number,
        block_timestamp,
        tx_hash,
        contract_address,
        protocol_market,
        borrower,
        payer,
        token_address,
        token_symbol,
        amount_unadj as amount_raw,
        amount,
        amount_usd,
        modified_timestamp as _inserted_timestamp,
        {{ dbt_utils.generate_surrogate_key(
                ['ez_lending_repayments_id', 'blockchain']
        )}} AS complete_lending_repayments_id,
        {{ dbt_utils.generate_surrogate_key(['blockchain','block_number','platform']) }} AS _unique_key
    FROM 
        {{ source(
            'avalanche_defi',
            'ez_lending_repayments'
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
gnosis as (

    SELECT
        'gnosis' as blockchain,
        platform,
        block_number,
        block_timestamp,
        tx_hash,
        contract_address,
        protocol_market,
        borrower,
        payer,
        token_address,
        token_symbol,
        amount_unadj as amount_raw,
        amount,
        amount_usd,
        modified_timestamp as _inserted_timestamp,
        {{ dbt_utils.generate_surrogate_key(
                ['ez_lending_repayments_id', 'blockchain']
        )}} AS complete_lending_repayments_id,
        {{ dbt_utils.generate_surrogate_key(['blockchain','block_number','platform']) }} AS _unique_key
    FROM 
        {{ source(
            'gnosis_defi',
            'ez_lending_repayments'
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
all_chains_repayments AS (
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
    origin_from_address,
    origin_to_address,
    protocol_market,
    borrower,
    payer,
    token_address,
    token_symbol,
    amount_raw,
    amount,
    amount_usd,
    _inserted_timestamp,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    complete_lending_repayments_id,
    _unique_key
FROM
    all_chains_repayments d