{{ config(
    materialized = 'incremental',
    unique_key = "address",
    incremental_strategy = 'delete+insert',
    tags = ['snowflake', 'crosschain', 'labels']
) }}

WITH base_labels AS (

    SELECT
        DISTINCT tx_id,
        block_id,
        block_timestamp,
        succeeded,
        instruction :parsed :info :authority :: STRING AS from_address,
        instruction :parsed :info :programAccount :: STRING AS to_address,
        event_type,
        program_id,
        _inserted_timestamp
    FROM
        {{ source(
            'solana_silver',
            'events'
        ) }}
        e
    WHERE
        e.block_timestamp >= '2020-01-01'
        AND e.program_id = 'BPFLoaderUpgradeab1e11111111111111111111111'
        AND e.event_type = 'deployWithMaxDataLen'
        AND succeeded = 'TRUE'
        AND to_address NOT IN (
            SELECT
                DISTINCT address
            FROM
                {{ source(
                    'crosschain_core',
                    'address_labels'
                ) }}
            WHERE
                blockchain = 'solana'
        )

{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        MAX(
            _inserted_timestamp
        )
    FROM
        {{ this }}
)
{% endif %}
),
base_legacy_labels AS (
    SELECT
        DISTINCT system_created_at,
        insert_date,
        address,
        label_type AS l1_label,
        label_subtype AS l2_label,
        address_name,
        project_name
    FROM
        {{ source(
            'crosschain_core',
            'address_labels'
        ) }}
    WHERE
        blockchain = 'solana'
),
base_transacts AS (
    SELECT
        b.system_created_at,
        b.insert_date,
        A.tx_id,
        A.block_timestamp,
        A.from_address,
        A.to_address,
        b.l1_label,
        b.l2_label,
        b.address_name,
        b.project_name,
        A._inserted_timestamp
    FROM
        base_labels A
        INNER JOIN base_legacy_labels b
        ON A.from_address = b.address
    WHERE
        b.l1_label != 'flotsam'
)
SELECT
    DISTINCT system_created_at,
    insert_date,
    'solana' AS blockchain,
    to_address AS address,
    'flipside' AS creator,
    l1_label,
    'general_contract' AS l2_label,
    CONCAT(
        project_name,
        ': general contract'
    ) AS address_name,
    project_name,
    _inserted_timestamp
FROM
    base_transacts qualify(ROW_NUMBER() over(PARTITION BY address
ORDER BY
    l2_label DESC)) = 1
