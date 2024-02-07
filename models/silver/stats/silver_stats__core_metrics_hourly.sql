{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = "CONCAT_WS('-', blockchain, block_timestamp_hour)",
    cluster_by = ['block_timestamp_hour::DATE']
) }}

WITH base AS (

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
        total_fees AS total_fees_native,
        _inserted_timestamp,
        {{ dbt_utils.generate_surrogate_key(
            ['core_metrics_hourly_id','blockchain']
        ) }} AS core_metrics_hourly_id,
        SYSDATE() AS inserted_timestamp,
        SYSDATE() AS modified_timestamp
    FROM
        {{ source(
            'ethereum_silver_stats',
            'core_metrics_hourly'
        ) }}

{% if is_incremental() %}
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
UNION ALL
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
    total_fees AS total_fees_native,
    _inserted_timestamp,
    {{ dbt_utils.generate_surrogate_key(
        ['core_metrics_hourly_id','blockchain']
    ) }} AS core_metrics_hourly_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp
FROM
    {{ source(
        'optimism_silver_stats',
        'core_metrics_hourly'
    ) }}

{% if is_incremental() %}
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
UNION ALL
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
    total_fees AS total_fees_native,
    _inserted_timestamp,
    {{ dbt_utils.generate_surrogate_key(
        ['core_metrics_hourly_id','blockchain']
    ) }} AS core_metrics_hourly_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp
FROM
    {{ source(
        'arbitrum_silver_stats',
        'core_metrics_hourly'
    ) }}

{% if is_incremental() %}
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
UNION ALL
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
    total_fees AS total_fees_native,
    _inserted_timestamp,
    {{ dbt_utils.generate_surrogate_key(
        ['core_metrics_hourly_id','blockchain']
    ) }} AS core_metrics_hourly_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp
FROM
    {{ source(
        'base_silver_stats',
        'core_metrics_hourly'
    ) }}

{% if is_incremental() %}
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
UNION ALL
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
    total_fees AS total_fees_native,
    _inserted_timestamp,
    {{ dbt_utils.generate_surrogate_key(
        ['core_metrics_hourly_id','blockchain']
    ) }} AS core_metrics_hourly_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp
FROM
    {{ source(
        'avalanche_silver_stats',
        'core_metrics_hourly'
    ) }}

{% if is_incremental() %}
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
UNION ALL
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
    total_fees AS total_fees_native,
    _inserted_timestamp,
    {{ dbt_utils.generate_surrogate_key(
        ['core_metrics_hourly_id','blockchain']
    ) }} AS core_metrics_hourly_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp
FROM
    {{ source(
        'polygon_silver_stats',
        'core_metrics_hourly'
    ) }}

{% if is_incremental() %}
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
UNION ALL
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
    total_fees AS total_fees_native,
    _inserted_timestamp,
    {{ dbt_utils.generate_surrogate_key(
        ['core_metrics_hourly_id','blockchain']
    ) }} AS core_metrics_hourly_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp
FROM
    {{ source(
        'bsc_silver_stats',
        'core_metrics_hourly'
    ) }}

{% if is_incremental() %}
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
UNION ALL
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
    total_fees AS total_fees_native,
    _inserted_timestamp,
    {{ dbt_utils.generate_surrogate_key(
        ['core_metrics_hourly_id','blockchain']
    ) }} AS core_metrics_hourly_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp
FROM
    {{ source(
        'gnosis_silver_stats',
        'core_metrics_hourly'
    ) }}

{% if is_incremental() %}
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
    _inserted_timestamp,
    core_metrics_hourly_id,
    inserted_timestamp,
    modified_timestamp
FROM
    base
