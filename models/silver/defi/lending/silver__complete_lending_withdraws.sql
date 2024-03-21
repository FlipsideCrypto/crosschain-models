{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = ['blockchain','block_number','platform'],
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
        origin_from_address,
        origin_to_address,
        protocol_market,
        depositor,
        token_address,
        token_symbol,
        amount_unadj as amount_raw,
        amount,
        amount_usd,
        {{ dbt_utils.generate_surrogate_key(
                ['ez_lending_withdraws_id', 'blockchain']
        )}} AS complete_lending_withdraws_id,
        inserted_timestamp,
        modified_timestamp as _inserted_timestamp
    FROM 
        {{ source(
            'ethereum_defi',
            'ez_lending_withdraws'
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
        origin_from_address,
        origin_to_address,
        protocol_market,
        depositor,
        token_address,
        token_symbol,
        amount_unadj as amount_raw,
        amount,
        amount_usd,
        {{ dbt_utils.generate_surrogate_key(
                ['ez_lending_withdraws_id', 'blockchain']
        )}} AS complete_lending_withdraws_id,
        inserted_timestamp,
        modified_timestamp as _inserted_timestamp
    FROM 
        {{ source(
            'arbitrum_defi',
            'ez_lending_withdraws'
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
        origin_from_address,
        origin_to_address,
        protocol_market,
        depositor,
        token_address,
        token_symbol,
        amount_unadj as amount_raw,
        amount,
        amount_usd,
        {{ dbt_utils.generate_surrogate_key(
                ['ez_lending_withdraws_id', 'blockchain']
        )}} AS complete_lending_withdraws_id,
        inserted_timestamp,
        modified_timestamp as _inserted_timestamp
    FROM 
        {{ source(
            'optimism_defi',
            'ez_lending_withdraws'
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
        origin_from_address,
        origin_to_address,
        protocol_market,
        depositor,
        token_address,
        token_symbol,
        amount_unadj as amount_raw,
        amount,
        amount_usd,
        {{ dbt_utils.generate_surrogate_key(
                ['ez_lending_withdraws_id', 'blockchain']
        )}} AS complete_lending_withdraws_id,
        inserted_timestamp,
        modified_timestamp as _inserted_timestamp
    FROM 
        {{ source(
            'bsc_defi',
            'ez_lending_withdraws'
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
        origin_from_address,
        origin_to_address,
        protocol_market,
        depositor,
        token_address,
        token_symbol,
        amount_unadj as amount_raw,
        amount,
        amount_usd,
        {{ dbt_utils.generate_surrogate_key(
                ['ez_lending_withdraws_id', 'blockchain']
        )}} AS complete_lending_withdraws_id,
        inserted_timestamp,
        modified_timestamp as _inserted_timestamp
    FROM 
        {{ source(
            'polygon_defi',
            'ez_lending_withdraws'
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
        origin_from_address,
        origin_to_address,
        protocol_market,
        depositor,
        token_address,
        token_symbol,
        amount_unadj as amount_raw,
        amount,
        amount_usd,
        {{ dbt_utils.generate_surrogate_key(
                ['ez_lending_withdraws_id', 'blockchain']
        )}} AS complete_lending_withdraws_id,
        inserted_timestamp,
        modified_timestamp as _inserted_timestamp
    FROM 
        {{ source(
            'base_defi',
            'ez_lending_withdraws'
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
        origin_from_address,
        origin_to_address,
        protocol_market,
        depositor,
        token_address,
        token_symbol,
        amount_unadj as amount_raw,
        amount,
        amount_usd,
        {{ dbt_utils.generate_surrogate_key(
                ['ez_lending_withdraws_id', 'blockchain']
        )}} AS complete_lending_withdraws_id,
        inserted_timestamp,
        modified_timestamp as _inserted_timestamp
    FROM 
        {{ source(
            'avalanche_defi',
            'ez_lending_withdraws'
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
        origin_from_address,
        origin_to_address,
        protocol_market,
        depositor,
        token_address,
        token_symbol,
        amount_unadj as amount_raw,
        amount,
        amount_usd,
        {{ dbt_utils.generate_surrogate_key(
                ['ez_lending_withdraws_id', 'blockchain']
        )}} AS complete_lending_withdraws_id,
        inserted_timestamp,
        modified_timestamp as _inserted_timestamp
    FROM 
        {{ source(
            'gnosis_defi',
            'ez_lending_withdraws'
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
all_chains_withdraws AS (
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
    depositor,
    token_address,
    token_symbol,
    amount_raw,
    amount,
    amount_usd,
    _inserted_timestamp,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    complete_lending_withdraws_id
FROM
    all_chains_withdraws d