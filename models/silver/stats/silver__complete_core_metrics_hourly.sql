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
        total_fees AS total_fees_native,
        _inserted_timestamp,
        {{ dbt_utils.generate_surrogate_key(
            ['core_metrics_hourly_id','blockchain']
        ) }} AS core_metrics_hourly_id
    FROM
        {{ source(
            'ethereum_silver_stats',
            'core_metrics_hourly'
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
        total_fees AS total_fees_native,
        _inserted_timestamp,
        {{ dbt_utils.generate_surrogate_key(
            ['core_metrics_hourly_id','blockchain']
        ) }} AS core_metrics_hourly_id
    FROM
        {{ source(
            'optimism_silver_stats',
            'core_metrics_hourly'
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
        total_fees AS total_fees_native,
        _inserted_timestamp,
        {{ dbt_utils.generate_surrogate_key(
            ['core_metrics_hourly_id','blockchain']
        ) }} AS core_metrics_hourly_id
    FROM
        {{ source(
            'arbitrum_silver_stats',
            'core_metrics_hourly'
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
        total_fees AS total_fees_native,
        _inserted_timestamp,
        {{ dbt_utils.generate_surrogate_key(
            ['core_metrics_hourly_id','blockchain']
        ) }} AS core_metrics_hourly_id
    FROM
        {{ source(
            'base_silver_stats',
            'core_metrics_hourly'
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
        total_fees AS total_fees_native,
        _inserted_timestamp,
        {{ dbt_utils.generate_surrogate_key(
            ['core_metrics_hourly_id','blockchain']
        ) }} AS core_metrics_hourly_id
    FROM
        {{ source(
            'avalanche_silver_stats',
            'core_metrics_hourly'
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
        total_fees AS total_fees_native,
        _inserted_timestamp,
        {{ dbt_utils.generate_surrogate_key(
            ['core_metrics_hourly_id','blockchain']
        ) }} AS core_metrics_hourly_id
    FROM
        {{ source(
            'polygon_silver_stats',
            'core_metrics_hourly'
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
        total_fees AS total_fees_native,
        _inserted_timestamp,
        {{ dbt_utils.generate_surrogate_key(
            ['core_metrics_hourly_id','blockchain']
        ) }} AS core_metrics_hourly_id
    FROM
        {{ source(
            'bsc_silver_stats',
            'core_metrics_hourly'
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
        total_fees AS total_fees_native,
        _inserted_timestamp,
        {{ dbt_utils.generate_surrogate_key(
            ['core_metrics_hourly_id','blockchain']
        ) }} AS core_metrics_hourly_id
    FROM
        {{ source(
            'gnosis_silver_stats',
            'core_metrics_hourly'
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
)
SELECT
    s.blockchain,
    block_timestamp_hour,
    block_number_min,
    block_number_max,
    block_count,
    transaction_count,
    transaction_count_success,
    transaction_count_failed,
    unique_initiator_count,
    total_fees_native,
    ROUND(
        total_fees_native * p.price,
        2
    ) AS total_fees_usd,
    _inserted_timestamp,
    core_metrics_hourly_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp
FROM
    all_chains s
    LEFT JOIN {{ ref('price__ez_hourly_token_prices') }}
    p
    ON s.block_timestamp_hour = p.hour
    AND p.blockchain = (
        CASE
            WHEN s.blockchain IN (
                'arbitrum',
                'base',
                'optimism'
            ) THEN 'ethereum' --use ethereum WETH on L2s for better coverage
            ELSE s.blockchain
        END
    )
    AND p.token_address = (
        CASE
            WHEN s.blockchain = 'arbitrum' THEN '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2' --WETH
            WHEN s.blockchain = 'avalanche' THEN '0xb31f66aa3c1e785363f0875a1b74e27b85fd66c7' --WAVAX
            WHEN s.blockchain = 'base' THEN '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2' --WETH
            WHEN s.blockchain = 'bsc' THEN '0xbb4cdb9cbd36b01bd1cbaebf2de08d9173bc095c' --WBNB
            WHEN s.blockchain = 'ethereum' THEN '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2' --WETH
            WHEN s.blockchain = 'gnosis' THEN '0xe91d153e0b41518a2ce8dd3d7944fa863463a97d' --WXDAI
            WHEN s.blockchain = 'optimism' THEN '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2' --WETH
            WHEN s.blockchain = 'polygon' THEN '0x0d500b1d8e8ef31e21c99d1db9a6444d3adf1270' --WMATIC
        END
    )
