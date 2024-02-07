{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = "CONCAT_WS('-', blockchain, block_timestamp_hour)",
    cluster_by = ['block_timestamp_hour::DATE']
) }}

--update source to silver
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
        total_fees_native * p.price AS total_fees_usd,
        _inserted_timestamp,
        {{ dbt_utils.generate_surrogate_key(
            ['ez_core_metrics_hourly_id','blockchain']
        ) }} AS core_metrics_hourly_id,
        SYSDATE() AS inserted_timestamp,
        SYSDATE() AS modified_timestamp
    FROM
        {{ source(
            'ethereum_stats',
            'ez_core_metrics_hourly'
        ) }}
        s
        LEFT JOIN {{ ref('price__ez_hourly_token_prices') }}
        p
        ON s.block_timestamp_hour = p.hour
        AND p.token_address = '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2' --WETH
        AND p.blockchain = 'ethereum'

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
    total_fees_native * p.price AS total_fees_usd,
    _inserted_timestamp,
    {{ dbt_utils.generate_surrogate_key(
        ['ez_core_metrics_hourly_id','blockchain']
    ) }} AS core_metrics_hourly_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp
FROM
    {{ source(
        'optimism_stats',
        'ez_core_metrics_hourly'
    ) }}
    s
    LEFT JOIN {{ ref('price__ez_hourly_token_prices') }}
    p
    ON s.block_timestamp_hour = p.hour
    AND p.token_address = '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2' --WETH
    AND p.blockchain = 'ethereum' --using prices from Ethereum for better WETH coverage on L2s

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
    total_fees_native * p.price AS total_fees_usd,
    _inserted_timestamp,
    {{ dbt_utils.generate_surrogate_key(
        ['ez_core_metrics_hourly_id','blockchain']
    ) }} AS core_metrics_hourly_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp
FROM
    {{ source(
        'arbitrum_stats',
        'ez_core_metrics_hourly'
    ) }}
    s
    LEFT JOIN {{ ref('price__ez_hourly_token_prices') }}
    p
    ON s.block_timestamp_hour = p.hour
    AND p.token_address = '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2' --WETH
    AND p.blockchain = 'ethereum' --using prices from Ethereum for better WETH coverage on L2s

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
    total_fees_native * p.price AS total_fees_usd,
    _inserted_timestamp,
    {{ dbt_utils.generate_surrogate_key(
        ['ez_core_metrics_hourly_id','blockchain']
    ) }} AS core_metrics_hourly_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp
FROM
    {{ source(
        'base_stats',
        'ez_core_metrics_hourly'
    ) }}
    s
    LEFT JOIN {{ ref('price__ez_hourly_token_prices') }}
    p
    ON s.block_timestamp_hour = p.hour
    AND p.token_address = '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2' --WETH
    AND p.blockchain = 'ethereum' --using prices from Ethereum for better WETH coverage on L2s

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
    total_fees_native * p.price AS total_fees_usd,
    _inserted_timestamp,
    {{ dbt_utils.generate_surrogate_key(
        ['ez_core_metrics_hourly_id','blockchain']
    ) }} AS core_metrics_hourly_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp
FROM
    {{ source(
        'avalanche_stats',
        'ez_core_metrics_hourly'
    ) }}
    s
    LEFT JOIN {{ ref('price__ez_hourly_token_prices') }}
    p
    ON s.block_timestamp_hour = p.hour
    AND p.token_address = '0xb31f66aa3c1e785363f0875a1b74e27b85fd66c7' --WAVAX
    AND p.blockchain = 'avalanche'

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
    total_fees_native * p.price AS total_fees_usd,
    _inserted_timestamp,
    {{ dbt_utils.generate_surrogate_key(
        ['ez_core_metrics_hourly_id','blockchain']
    ) }} AS core_metrics_hourly_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp
FROM
    {{ source(
        'polygon_stats',
        'ez_core_metrics_hourly'
    ) }}
    s
    LEFT JOIN {{ ref('price__ez_hourly_token_prices') }}
    p
    ON s.block_timestamp_hour = p.hour
    AND p.token_address = '0x0d500b1d8e8ef31e21c99d1db9a6444d3adf1270' --WMATIC
    AND p.blockchain = 'polygon'

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
    total_fees_native * p.price AS total_fees_usd,
    _inserted_timestamp,
    {{ dbt_utils.generate_surrogate_key(
        ['ez_core_metrics_hourly_id','blockchain']
    ) }} AS core_metrics_hourly_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp
FROM
    {{ source(
        'bsc_stats',
        'ez_core_metrics_hourly'
    ) }}
    s
    LEFT JOIN {{ ref('price__ez_hourly_token_prices') }}
    p
    ON s.block_timestamp_hour = p.hour
    AND p.token_address = '0xbb4cdb9cbd36b01bd1cbaebf2de08d9173bc095c' --WBNB
    AND p.blockchain = 'bsc'

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
    total_fees_native * p.price AS total_fees_usd,
    _inserted_timestamp,
    {{ dbt_utils.generate_surrogate_key(
        ['ez_core_metrics_hourly_id','blockchain']
    ) }} AS core_metrics_hourly_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp
FROM
    {{ source(
        'gnosis_stats',
        'ez_core_metrics_hourly'
    ) }}
    s
    LEFT JOIN {{ ref('price__ez_hourly_token_prices') }}
    p
    ON s.block_timestamp_hour = p.hour
    AND p.token_address = '0xe91d153e0b41518a2ce8dd3d7944fa863463a97d' --WXDAI
    AND p.blockchain = 'gnosis'

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
    total_fees_usd,
    _inserted_timestamp,
    core_metrics_hourly_id,
    inserted_timestamp,
    modified_timestamp
FROM
    base
