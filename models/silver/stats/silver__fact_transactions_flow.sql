{{ config(
    materialized = 'incremental',
    incremental_strategy = 'merge',
    unique_key = ['block_timestamp::DATE','tx_hash'],
    cluster_by = ['block_timestamp::DATE'],
    tags = ['daily']
) }}

{% if execute %}

{% if is_incremental() %}
{% set query %}

SELECT
    MIN(block_timestamp) :: DATE
FROM
    {{ source(
        'flow_core',
        'fact_transactions'
    ) }}
WHERE
    modified_timestamp >= (
        SELECT
            MAX(modified_timestamp)
        FROM
            {{ this }}
    ) {% endset %}
    {% set min_block_date = run_query(query).columns [0].values() [0] %}
    {% if not min_block_date or min_block_date == 'None' %}
        {% set min_block_date = '2099-01-01' %}
    {% endif %}
{% endif %}
{% endif %}

WITH fees AS (
    SELECT
        tx_id,
        block_timestamp,
        SUM(
            event_data :amount :: FLOAT
        ) AS total_fees
    FROM
        {{ source(
            'flow_core',
            'fact_events'
        ) }}
    WHERE
        event_type = 'FeesDeducted'

{% if is_incremental() %}
AND block_timestamp :: DATE >= '{{ min_block_date }}'
{% else %}
    AND block_timestamp :: DATE >= '2025-01-01'
{% endif %}
GROUP BY
    tx_id,
    block_timestamp
),
transactions AS (
    SELECT
        tx_id,
        block_timestamp,
        tx_succeeded,
        COALESCE(
            authorizers [1],
            authorizers [0]
        ) AS sender
    FROM
        {{ source(
            'flow_core',
            'fact_transactions'
        ) }}

{% if is_incremental() %}
WHERE
    block_timestamp :: DATE >= '{{ min_block_date }}'
{% else %}
WHERE
    block_timestamp :: DATE >= '2025-01-01'
{% endif %}
)
SELECT
    tx.tx_id AS tx_hash,
    tx.block_timestamp,
    tx.tx_succeeded,
    tx.sender,
    COALESCE(
        total_fees,
        0
    ) AS fee_native,
    {{ dbt_utils.generate_surrogate_key(
        ['tx.tx_id']
    ) }} AS fact_transactions_flow_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    transactions AS tx
    LEFT JOIN fees
    ON tx.tx_id = fees.tx_id
    AND tx.block_timestamp = fees.block_timestamp
