{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = ['blockchain', 'block_timestamp_hour'],
    cluster_by = ['block_timestamp_hour::DATE']
) }}

WITH ethereum AS (

    SELECT
        'ethereum' AS blockchain,
        block_timestamp_hour,
        block_number_min,
        block_number_max,
        block_count,
        transaction_count,
        transaction_count_success,
        transaction_count_failed,
        unique_from_count AS unique_initiator_count,
        total_fees_native,
        total_fees_usd,
        modified_timestamp AS _inserted_timestamp,
        {{ dbt_utils.generate_surrogate_key(
            ['ez_core_metrics_hourly_id','blockchain']
        ) }} AS core_metrics_hourly_id
    FROM
        {{ source(
            'ethereum_stats',
            'ez_core_metrics_hourly'
        ) }}

{% if is_incremental() and 'ethereum' not in var('HEAL_CURATED_MODEL') %}
WHERE
    DATE_TRUNC(
        'hour',
        _inserted_timestamp
    ) >= (
        SELECT
            MAX(DATE_TRUNC('hour', _inserted_timestamp)) - INTERVAL '12 hours'
        FROM
            {{ this }}
    )
{% endif %}
),
optimism AS (
    SELECT
        'optimism' AS blockchain,
        block_timestamp_hour,
        block_number_min,
        block_number_max,
        block_count,
        transaction_count,
        transaction_count_success,
        transaction_count_failed,
        unique_from_count AS unique_initiator_count,
        total_fees_native,
        total_fees_usd,
        modified_timestamp AS _inserted_timestamp,
        {{ dbt_utils.generate_surrogate_key(
            ['ez_core_metrics_hourly_id','blockchain']
        ) }} AS core_metrics_hourly_id
    FROM
        {{ source(
            'optimism_stats',
            'ez_core_metrics_hourly'
        ) }}

{% if is_incremental() and 'optimism' not in var('HEAL_CURATED_MODEL') %}
WHERE
    DATE_TRUNC(
        'hour',
        _inserted_timestamp
    ) >= (
        SELECT
            MAX(DATE_TRUNC('hour', _inserted_timestamp)) - INTERVAL '12 hours'
        FROM
            {{ this }}
    )
{% endif %}
),
arbitrum AS (
    SELECT
        'arbitrum' AS blockchain,
        block_timestamp_hour,
        block_number_min,
        block_number_max,
        block_count,
        transaction_count,
        transaction_count_success,
        transaction_count_failed,
        unique_from_count AS unique_initiator_count,
        total_fees_native,
        total_fees_usd,
        modified_timestamp AS _inserted_timestamp,
        {{ dbt_utils.generate_surrogate_key(
            ['ez_core_metrics_hourly_id','blockchain']
        ) }} AS core_metrics_hourly_id
    FROM
        {{ source(
            'arbitrum_stats',
            'ez_core_metrics_hourly'
        ) }}

{% if is_incremental() and 'arbitrum' not in var('HEAL_CURATED_MODEL') %}
WHERE
    DATE_TRUNC(
        'hour',
        _inserted_timestamp
    ) >= (
        SELECT
            MAX(DATE_TRUNC('hour', _inserted_timestamp)) - INTERVAL '12 hours'
        FROM
            {{ this }}
    )
{% endif %}
),
base AS (
    SELECT
        'base' AS blockchain,
        block_timestamp_hour,
        block_number_min,
        block_number_max,
        block_count,
        transaction_count,
        transaction_count_success,
        transaction_count_failed,
        unique_from_count AS unique_initiator_count,
        total_fees_native,
        total_fees_usd,
        modified_timestamp AS _inserted_timestamp,
        {{ dbt_utils.generate_surrogate_key(
            ['ez_core_metrics_hourly_id','blockchain']
        ) }} AS core_metrics_hourly_id
    FROM
        {{ source(
            'base_stats',
            'ez_core_metrics_hourly'
        ) }}

{% if is_incremental() and 'base' not in var('HEAL_CURATED_MODEL') %}
WHERE
    DATE_TRUNC(
        'hour',
        _inserted_timestamp
    ) >= (
        SELECT
            MAX(DATE_TRUNC('hour', _inserted_timestamp)) - INTERVAL '12 hours'
        FROM
            {{ this }}
    )
{% endif %}
),
avalanche AS (
    SELECT
        'avalanche' AS blockchain,
        block_timestamp_hour,
        block_number_min,
        block_number_max,
        block_count,
        transaction_count,
        transaction_count_success,
        transaction_count_failed,
        unique_from_count AS unique_initiator_count,
        total_fees_native,
        total_fees_usd,
        modified_timestamp AS _inserted_timestamp,
        {{ dbt_utils.generate_surrogate_key(
            ['ez_core_metrics_hourly_id','blockchain']
        ) }} AS core_metrics_hourly_id
    FROM
        {{ source(
            'avalanche_stats',
            'ez_core_metrics_hourly'
        ) }}

{% if is_incremental() and 'avalanche' not in var('HEAL_CURATED_MODEL') %}
WHERE
    DATE_TRUNC(
        'hour',
        _inserted_timestamp
    ) >= (
        SELECT
            MAX(DATE_TRUNC('hour', _inserted_timestamp)) - INTERVAL '12 hours'
        FROM
            {{ this }}
    )
{% endif %}
),
polygon AS (
    SELECT
        'polygon' AS blockchain,
        block_timestamp_hour,
        block_number_min,
        block_number_max,
        block_count,
        transaction_count,
        transaction_count_success,
        transaction_count_failed,
        unique_from_count AS unique_initiator_count,
        total_fees_native,
        total_fees_usd,
        modified_timestamp AS _inserted_timestamp,
        {{ dbt_utils.generate_surrogate_key(
            ['ez_core_metrics_hourly_id','blockchain']
        ) }} AS core_metrics_hourly_id
    FROM
        {{ source(
            'polygon_stats',
            'ez_core_metrics_hourly'
        ) }}

{% if is_incremental() and 'polygon' not in var('HEAL_CURATED_MODEL') %}
WHERE
    DATE_TRUNC(
        'hour',
        _inserted_timestamp
    ) >= (
        SELECT
            MAX(DATE_TRUNC('hour', _inserted_timestamp)) - INTERVAL '12 hours'
        FROM
            {{ this }}
    )
{% endif %}
),
bsc AS (
    SELECT
        'bsc' AS blockchain,
        block_timestamp_hour,
        block_number_min,
        block_number_max,
        block_count,
        transaction_count,
        transaction_count_success,
        transaction_count_failed,
        unique_from_count AS unique_initiator_count,
        total_fees_native,
        total_fees_usd,
        modified_timestamp AS _inserted_timestamp,
        {{ dbt_utils.generate_surrogate_key(
            ['ez_core_metrics_hourly_id','blockchain']
        ) }} AS core_metrics_hourly_id
    FROM
        {{ source(
            'bsc_stats',
            'ez_core_metrics_hourly'
        ) }}

{% if is_incremental() and 'bsc' not in var('HEAL_CURATED_MODEL') %}
WHERE
    DATE_TRUNC(
        'hour',
        _inserted_timestamp
    ) >= (
        SELECT
            MAX(DATE_TRUNC('hour', _inserted_timestamp)) - INTERVAL '12 hours'
        FROM
            {{ this }}
    )
{% endif %}
),
gnosis AS (
    SELECT
        'gnosis' AS blockchain,
        block_timestamp_hour,
        block_number_min,
        block_number_max,
        block_count,
        transaction_count,
        transaction_count_success,
        transaction_count_failed,
        unique_from_count AS unique_initiator_count,
        total_fees_native,
        total_fees_usd,
        modified_timestamp AS _inserted_timestamp,
        {{ dbt_utils.generate_surrogate_key(
            ['ez_core_metrics_hourly_id','blockchain']
        ) }} AS core_metrics_hourly_id
    FROM
        {{ source(
            'gnosis_stats',
            'ez_core_metrics_hourly'
        ) }}

{% if is_incremental() and 'gnosis' not in var('HEAL_CURATED_MODEL') %}
WHERE
    DATE_TRUNC(
        'hour',
        _inserted_timestamp
    ) >= (
        SELECT
            MAX(DATE_TRUNC('hour', _inserted_timestamp)) - INTERVAL '12 hours'
        FROM
            {{ this }}
    )
{% endif %}
),
sei AS (
    SELECT
        'sei' AS blockchain,
        block_timestamp_hour,
        block_number_min,
        block_number_max,
        block_count,
        transaction_count,
        transaction_count_success,
        transaction_count_failed,
        unique_from_count AS unique_initiator_count,
        total_fees_native,
        total_fees_usd,
        modified_timestamp AS _inserted_timestamp,
        {{ dbt_utils.generate_surrogate_key(
            ['ez_core_metrics_hourly_id','blockchain']
        ) }} AS core_metrics_hourly_id
    FROM
        {{ source(
            'sei_stats',
            'ez_core_metrics_hourly'
        ) }}

{% if is_incremental() and 'sei' not in var('HEAL_CURATED_MODEL') %}
WHERE
    DATE_TRUNC(
        'hour',
        _inserted_timestamp
    ) >= (
        SELECT
            MAX(DATE_TRUNC('hour', _inserted_timestamp)) - INTERVAL '12 hours'
        FROM
            {{ this }}
    )
{% endif %}
),
cosmos AS (
    SELECT
        'cosmos' AS blockchain,
        block_timestamp_hour,
        block_number_min,
        block_number_max,
        block_count,
        transaction_count,
        transaction_count_success,
        transaction_count_failed,
        unique_from_count AS unique_initiator_count,
        total_fees_native,
        total_fees_usd,
        modified_timestamp AS _inserted_timestamp,
        {{ dbt_utils.generate_surrogate_key(
            ['ez_core_metrics_hourly_id','blockchain']
        ) }} AS core_metrics_hourly_id
    FROM
        {{ source(
            'cosmos_stats',
            'ez_core_metrics_hourly'
        ) }}

{% if is_incremental() and 'cosmos' not in var('HEAL_CURATED_MODEL') %}
WHERE
    DATE_TRUNC(
        'hour',
        _inserted_timestamp
    ) >= (
        SELECT
            MAX(DATE_TRUNC('hour', _inserted_timestamp)) - INTERVAL '12 hours'
        FROM
            {{ this }}
    )
{% endif %}
),
terra AS (
    SELECT
        'terra' AS blockchain,
        block_timestamp_hour,
        block_number_min,
        block_number_max,
        block_count,
        transaction_count,
        transaction_count_success,
        transaction_count_failed,
        unique_from_count AS unique_initiator_count,
        total_fees_native,
        total_fees_usd,
        modified_timestamp AS _inserted_timestamp,
        {{ dbt_utils.generate_surrogate_key(
            ['ez_core_metrics_hourly_id','blockchain']
        ) }} AS core_metrics_hourly_id
    FROM
        {{ source(
            'terra_stats',
            'ez_core_metrics_hourly'
        ) }}

{% if is_incremental() and 'terra' not in var('HEAL_CURATED_MODEL') %}
WHERE
    DATE_TRUNC(
        'hour',
        _inserted_timestamp
    ) >= (
        SELECT
            MAX(DATE_TRUNC('hour', _inserted_timestamp)) - INTERVAL '12 hours'
        FROM
            {{ this }}
    )
{% endif %}
),
osmosis AS (
    SELECT
        'osmosis' AS blockchain,
        block_timestamp_hour,
        block_number_min,
        block_number_max,
        block_count,
        transaction_count,
        transaction_count_success,
        transaction_count_failed,
        unique_from_count AS unique_initiator_count,
        total_fees_native,
        total_fees_usd,
        modified_timestamp AS _inserted_timestamp,
        {{ dbt_utils.generate_surrogate_key(
            ['ez_core_metrics_hourly_id','blockchain']
        ) }} AS core_metrics_hourly_id
    FROM
        {{ source(
            'osmosis_stats',
            'ez_core_metrics_hourly'
        ) }}

{% if is_incremental() and 'osmosis' not in var('HEAL_CURATED_MODEL') %}
WHERE
    DATE_TRUNC(
        'hour',
        _inserted_timestamp
    ) >= (
        SELECT
            MAX(DATE_TRUNC('hour', _inserted_timestamp)) - INTERVAL '12 hours'
        FROM
            {{ this }}
    )
{% endif %}
),
flow AS (
    SELECT
        'flow' AS blockchain,
        block_timestamp_hour,
        block_number_min,
        block_number_max,
        block_count,
        transaction_count,
        transaction_count_success,
        transaction_count_failed,
        unique_from_count AS unique_initiator_count,
        total_fees_native,
        total_fees_usd,
        modified_timestamp AS _inserted_timestamp,
        {{ dbt_utils.generate_surrogate_key(
            ['ez_core_metrics_hourly_id','blockchain']
        ) }} AS core_metrics_hourly_id
    FROM
        {{ source(
            'flow_stats',
            'ez_core_metrics_hourly'
        ) }}

{% if is_incremental() and 'flow' not in var('HEAL_CURATED_MODEL') %}
WHERE
    DATE_TRUNC(
        'hour',
        _inserted_timestamp
    ) >= (
        SELECT
            MAX(DATE_TRUNC('hour', _inserted_timestamp)) - INTERVAL '12 hours'
        FROM
            {{ this }}
    )
{% endif %}
),
solana AS (
    SELECT
        'solana' AS blockchain,
        block_timestamp_hour,
        block_number_min,
        block_number_max,
        block_count,
        transaction_count,
        transaction_count_success,
        transaction_count_failed,
        unique_signers_count AS unique_initiator_count,
        total_fees_native,
        total_fees_usd,
        modified_timestamp AS _inserted_timestamp,
        {{ dbt_utils.generate_surrogate_key(
            ['ez_core_metrics_hourly_id','blockchain']
        ) }} AS core_metrics_hourly_id
    FROM
        {{ source(
            'solana_stats',
            'ez_core_metrics_hourly'
        ) }}

{% if is_incremental() and 'solana' not in var('HEAL_CURATED_MODEL') %}
WHERE
    DATE_TRUNC(
        'hour',
        _inserted_timestamp
    ) >= (
        SELECT
            MAX(DATE_TRUNC('hour', _inserted_timestamp)) - INTERVAL '12 hours'
        FROM
            {{ this }}
    )
{% endif %}
),
aptos AS (
    SELECT
        'aptos' AS blockchain,
        block_timestamp_hour,
        block_number_min,
        block_number_max,
        block_count,
        transaction_count,
        transaction_count_success,
        transaction_count_failed,
        unique_sender_count AS unique_initiator_count,
        total_fees_native,
        total_fees_usd,
        modified_timestamp AS _inserted_timestamp,
        {{ dbt_utils.generate_surrogate_key(
            ['ez_core_metrics_hourly_id','blockchain']
        ) }} AS core_metrics_hourly_id
    FROM
        {{ source(
            'aptos_stats',
            'ez_core_metrics_hourly'
        ) }}

{% if is_incremental() and 'aptos' not in var('HEAL_CURATED_MODEL') %}
WHERE
    DATE_TRUNC(
        'hour',
        _inserted_timestamp
    ) >= (
        SELECT
            MAX(DATE_TRUNC('hour', _inserted_timestamp)) - INTERVAL '12 hours'
        FROM
            {{ this }}
    )
{% endif %}
),
bitcoin AS (
    SELECT
        'bitcoin' AS blockchain,
        block_timestamp_hour,
        block_number_min,
        block_number_max,
        block_count,
        transaction_count,
        NULL AS transaction_count_success,
        NULL AS transaction_count_failed,
        unique_address_count AS unique_initiator_count,
        total_fees_native,
        total_fees_usd,
        modified_timestamp AS _inserted_timestamp,
        {{ dbt_utils.generate_surrogate_key(
            ['ez_core_metrics_hourly_id','blockchain']
        ) }} AS core_metrics_hourly_id
    FROM
        {{ source(
            'bitcoin_stats',
            'ez_core_metrics_hourly'
        ) }}

{% if is_incremental() and 'bitcoin' not in var('HEAL_CURATED_MODEL') %}
WHERE
    DATE_TRUNC(
        'hour',
        _inserted_timestamp
    ) >= (
        SELECT
            MAX(DATE_TRUNC('hour', _inserted_timestamp)) - INTERVAL '12 hours'
        FROM
            {{ this }}
    )
{% endif %}
),
aurora AS (
    SELECT
        'aurora' AS blockchain,
        block_timestamp_hour,
        block_number_min,
        block_number_max,
        block_count,
        transaction_count,
        transaction_count_success,
        transaction_count_failed,
        unique_from_count AS unique_initiator_count,
        total_fees_native,
        total_fees_usd,
        modified_timestamp AS _inserted_timestamp,
        {{ dbt_utils.generate_surrogate_key(
            ['ez_core_metrics_hourly_id','blockchain']
        ) }} AS core_metrics_hourly_id
    FROM
        {{ source(
            'aurora_stats',
            'ez_core_metrics_hourly'
        ) }}

{% if is_incremental() and 'aurora' not in var('HEAL_CURATED_MODEL') %}
WHERE
    DATE_TRUNC(
        'hour',
        _inserted_timestamp
    ) >= (
        SELECT
            MAX(DATE_TRUNC('hour', _inserted_timestamp)) - INTERVAL '12 hours'
        FROM
            {{ this }}
    )
{% endif %}
),
near AS (
    SELECT
        'near' AS blockchain,
        block_timestamp_hour,
        block_number_min,
        block_number_max,
        block_count,
        transaction_count,
        transaction_count_success,
        transaction_count_failed,
        unique_from_count AS unique_initiator_count,
        total_fees_native,
        total_fees_usd,
        modified_timestamp AS _inserted_timestamp,
        {{ dbt_utils.generate_surrogate_key(
            ['ez_core_metrics_hourly_id','blockchain']
        ) }} AS core_metrics_hourly_id
    FROM
        {{ source(
            'near_stats',
            'ez_core_metrics_hourly'
        ) }}

{% if is_incremental() and 'near' not in var('HEAL_CURATED_MODEL') %}
WHERE
    DATE_TRUNC(
        'hour',
        _inserted_timestamp
    ) >= (
        SELECT
            MAX(DATE_TRUNC('hour', _inserted_timestamp)) - INTERVAL '12 hours'
        FROM
            {{ this }}
    )
{% endif %}
),
axelar AS (
    SELECT
        'axelar' AS blockchain,
        block_timestamp_hour,
        block_number_min,
        block_number_max,
        block_count,
        transaction_count,
        transaction_count_success,
        transaction_count_failed,
        unique_from_count AS unique_initiator_count,
        total_fees_native,
        total_fees_usd,
        modified_timestamp AS _inserted_timestamp,
        {{ dbt_utils.generate_surrogate_key(
            ['ez_core_metrics_hourly_id','blockchain']
        ) }} AS core_metrics_hourly_id
    FROM
        {{ source(
            'axelar_stats',
            'ez_core_metrics_hourly'
        ) }}

{% if is_incremental() and 'axelar' not in var('HEAL_CURATED_MODEL') %}
WHERE
    DATE_TRUNC(
        'hour',
        _inserted_timestamp
    ) >= (
        SELECT
            MAX(DATE_TRUNC('hour', _inserted_timestamp)) - INTERVAL '12 hours'
        FROM
            {{ this }}
    )
{% endif %}
),
all_chains AS (
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
        gnosis
    UNION ALL
    SELECT
        *
    FROM
        sei
    UNION ALL
    SELECT
        *
    FROM
        cosmos
    UNION ALL
    SELECT
        *
    FROM
        terra
    UNION ALL
    SELECT
        *
    FROM
        osmosis
    UNION ALL
    SELECT
        *
    FROM   
        flow
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
        bitcoin
    UNION ALL
    SELECT
        *
    FROM
        aurora
    UNION ALL
    SELECT
        *
    FROM
        near
    UNION ALL
    SELECT
        *
    FROM
        axelar
)
SELECT
    blockchain,
    block_timestamp_hour,
    block_number_min,
    block_number_max,
    block_count,
    transaction_count,
    transaction_count_success,
    transaction_count_failed,
    unique_initiator_count,
    total_fees_native,
    total_fees_usd,
    _inserted_timestamp,
    core_metrics_hourly_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp
FROM
    all_chains