{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = ['_unique_key'],
    cluster_by = ['block_timestamp::DATE','platform'],
    post_hook = "ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION ON EQUALITY(tx_hash, contract_address, flashloan_token, flashloan_token_symbol, protocol_market), SUBSTRING(flashloan_token, flashloan_token_symbol, protocol_market)",
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
        initiator,
        target,
        flashloan_token,
        flashloan_token_symbol,
        flashloan_amount_unadj AS flashloan_amount_raw,
        flashloan_amount,
        flashloan_amount_usd,
        premium_amount_unadj AS premium_amount_raw,
        premium_amount,
        premium_amount_usd,
        modified_timestamp AS _inserted_timestamp,
        {{ dbt_utils.generate_surrogate_key(
            ['ez_lending_flashloans_id', 'blockchain']
        ) }} AS complete_lending_flashloans_id,
        {{ dbt_utils.generate_surrogate_key(['blockchain','block_number','platform']) }} AS _unique_key
    FROM
        {{ source(
            'ethereum_defi',
            'ez_lending_flashloans'
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
        initiator,
        target,
        flashloan_token,
        flashloan_token_symbol,
        flashloan_amount_unadj AS flashloan_amount_raw,
        flashloan_amount,
        flashloan_amount_usd,
        premium_amount_unadj AS premium_amount_raw,
        premium_amount,
        premium_amount_usd,
        modified_timestamp AS _inserted_timestamp,
        {{ dbt_utils.generate_surrogate_key(
            ['ez_lending_flashloans_id', 'blockchain']
        ) }} AS complete_lending_flashloans_id,
        {{ dbt_utils.generate_surrogate_key(['blockchain','block_number','platform']) }} AS _unique_key
    FROM
        {{ source(
            'arbitrum_defi',
            'ez_lending_flashloans'
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
        initiator,
        target,
        flashloan_token,
        flashloan_token_symbol,
        flashloan_amount_unadj AS flashloan_amount_raw,
        flashloan_amount,
        flashloan_amount_usd,
        premium_amount_unadj AS premium_amount_raw,
        premium_amount,
        premium_amount_usd,
        modified_timestamp AS _inserted_timestamp,
        {{ dbt_utils.generate_surrogate_key(
            ['ez_lending_flashloans_id', 'blockchain']
        ) }} AS complete_lending_flashloans_id,
        {{ dbt_utils.generate_surrogate_key(['blockchain','block_number','platform']) }} AS _unique_key
    FROM
        {{ source(
            'optimism_defi',
            'ez_lending_flashloans'
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
        initiator,
        target,
        flashloan_token,
        flashloan_token_symbol,
        flashloan_amount_unadj AS flashloan_amount_raw,
        flashloan_amount,
        flashloan_amount_usd,
        premium_amount_unadj AS premium_amount_raw,
        premium_amount,
        premium_amount_usd,
        modified_timestamp AS _inserted_timestamp,
        {{ dbt_utils.generate_surrogate_key(
            ['ez_lending_flashloans_id', 'blockchain']
        ) }} AS complete_lending_flashloans_id,
        {{ dbt_utils.generate_surrogate_key(['blockchain','block_number','platform']) }} AS _unique_key
    FROM
        {{ source(
            'bsc_defi',
            'ez_lending_flashloans'
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
        initiator,
        target,
        flashloan_token,
        flashloan_token_symbol,
        flashloan_amount_unadj AS flashloan_amount_raw,
        flashloan_amount,
        flashloan_amount_usd,
        premium_amount_unadj AS premium_amount_raw,
        premium_amount,
        premium_amount_usd,
        modified_timestamp AS _inserted_timestamp,
        {{ dbt_utils.generate_surrogate_key(
            ['ez_lending_flashloans_id', 'blockchain']
        ) }} AS complete_lending_flashloans_id,
        {{ dbt_utils.generate_surrogate_key(['blockchain','block_number','platform']) }} AS _unique_key
    FROM
        {{ source(
            'polygon_defi',
            'ez_lending_flashloans'
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
        initiator,
        target,
        flashloan_token,
        flashloan_token_symbol,
        flashloan_amount_unadj AS flashloan_amount_raw,
        flashloan_amount,
        flashloan_amount_usd,
        premium_amount_unadj AS premium_amount_raw,
        premium_amount,
        premium_amount_usd,
        modified_timestamp AS _inserted_timestamp,
        {{ dbt_utils.generate_surrogate_key(
            ['ez_lending_flashloans_id', 'blockchain']
        ) }} AS complete_lending_flashloans_id,
        {{ dbt_utils.generate_surrogate_key(['blockchain','block_number','platform']) }} AS _unique_key
    FROM
        {{ source(
            'base_defi',
            'ez_lending_flashloans'
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
        initiator,
        target,
        flashloan_token,
        flashloan_token_symbol,
        flashloan_amount_unadj AS flashloan_amount_raw,
        flashloan_amount,
        flashloan_amount_usd,
        premium_amount_unadj AS premium_amount_raw,
        premium_amount,
        premium_amount_usd,
        modified_timestamp AS _inserted_timestamp,
        {{ dbt_utils.generate_surrogate_key(
            ['ez_lending_flashloans_id', 'blockchain']
        ) }} AS complete_lending_flashloans_id,
        {{ dbt_utils.generate_surrogate_key(['blockchain','block_number','platform']) }} AS _unique_key
    FROM
        {{ source(
            'avalanche_defi',
            'ez_lending_flashloans'
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
        initiator,
        target,
        flashloan_token,
        flashloan_token_symbol,
        flashloan_amount_unadj AS flashloan_amount_raw,
        flashloan_amount,
        flashloan_amount_usd,
        premium_amount_unadj AS premium_amount_raw,
        premium_amount,
        premium_amount_usd,
        modified_timestamp AS _inserted_timestamp,
        {{ dbt_utils.generate_surrogate_key(
            ['ez_lending_flashloans_id', 'blockchain']
        ) }} AS complete_lending_flashloans_id,
        {{ dbt_utils.generate_surrogate_key(['blockchain','block_number','platform']) }} AS _unique_key
    FROM
        {{ source(
            'gnosis_defi',
            'ez_lending_flashloans'
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
all_chains_flashloans AS (
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
    CASE 
        WHEN platform = 'Morpho Blue' THEN 'morpho_blue'
        ELSE lower(split_part(platform, ' ', 1))
    END AS protocol,
    CASE 
        WHEN platform = 'Morpho Blue' THEN 'v1'
        WHEN platform = 'Aave AMM' THEN 'v2'
        WHEN NULLIF(TRIM(SPLIT_PART(platform, ' ',2)), '') IS NULL THEN 'v1'
        ELSE lower(SPLIT_PART(platform,' ',2))
    END AS version,
    block_number,
    block_timestamp,
    tx_hash,
    contract_address,
    protocol_market,
    initiator,
    target,
    flashloan_token,
    flashloan_token_symbol,
    flashloan_amount_raw,
    flashloan_amount,
    flashloan_amount_usd,
    premium_amount_raw,
    premium_amount,
    premium_amount_usd,
    _inserted_timestamp,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    complete_lending_flashloans_id,
    _unique_key
FROM
    all_chains_flashloans d
