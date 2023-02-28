{{ config(
    materialized = 'incremental',
    unique_key = "address",
    incremental_strategy = 'delete+insert',
    tags = ['snowflake', 'crosschain', 'labels']
) }}

WITH base_legacy_labels AS (

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
        blockchain = 'ethereum'
),
base_labels AS (
    SELECT
        tx_hash,
        block_number,
        block_timestamp,
        tx_status,
        from_address,
        to_address,
        TYPE,
        identifier,
        _inserted_timestamp
    FROM
        {{ source(
            'ethereum_silver',
            'traces'
        ) }}
    WHERE
        TYPE IN (
            'CREATE',
            'CREATE2'
        )
        AND tx_status = 'SUCCESS'
        AND to_address IS NOT NULL
        AND to_address NOT IN (
            SELECT
                DISTINCT address
            FROM
                base_legacy_labels
        )
        AND from_address IN (
            SELECT
                DISTINCT address
            FROM
                base_legacy_labels
        )
),
base_transacts AS (
    SELECT
        b.system_created_at,
        b.insert_date,
        A.tx_hash,
        A.block_timestamp,
        A.from_address,
        A.to_address,
        A.identifier,
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
),
base_logs AS (
    SELECT
        DISTINCT tx_hash,
        contract_name,
        event_name,
        _inserted_timestamp
    FROM
        {{ source(
            'ethereum_silver',
            'logs'
        ) }}
    WHERE
        tx_hash IN (
            SELECT
                DISTINCT tx_hash
            FROM
                base_transacts
        )
        AND (
            event_name IN (
                'NewOracle',
                'NewSwapPool',
                'PairCreated',
                'LogNewWallet',
                'LogUserAdded'
            )
            OR event_name ILIKE '%pool%'
            OR event_name ILIKE '%create%'
        )
        AND event_name != 'SetTokenCreated'
        AND event_name != 'PoolUpdate'
        AND contract_name IS NOT NULL
        AND event_name IS NOT NULL
        AND tx_hash IN (
            SELECT
                DISTINCT tx_hash
            FROM
                base_transacts
        )
),
final_base AS (
    SELECT
        A.system_created_at,
        A.insert_date,
        A.tx_hash,
        A.block_timestamp,
        A.from_address,
        A.to_address,
        A.identifier,
        A.l1_label,
        A.l2_label,
        CASE
            WHEN C.event_name IN (
                'PairCreated',
                'NewSwapPool'
            ) THEN 'pool'
            WHEN C.event_name ILIKE '%pool%'
            AND C.event_name ILIKE '%create%' THEN 'pool'
            WHEN C.event_name IN ('LOG_NEW_POOL') THEN 'pool'
            WHEN C.event_name IN (
                'LogNewWallet',
                'LogUserAdded'
            ) THEN 'deposit_wallet'
            WHEN C.event_name IN ('NewOracle') THEN 'oracle'
            WHEN A.l1_label = 'dapp'
            AND A.l2_label = 'governance' THEN 'governance'
            WHEN A.address_name ILIKE '%pool deployer%' THEN 'pool'
            ELSE 'general_contract'
        END AS l2_label_fixed,
        A.address_name,
        CASE
            WHEN C.event_name IN ('PairCreated') THEN CONCAT(
                A.project_name,
                ': pair'
            )
            WHEN C.event_name IN ('NewSwapPool') THEN CONCAT(
                A.project_name,
                ': pool'
            )
            WHEN C.event_name ILIKE '%pool%'
            AND C.event_name ILIKE '%create%' THEN CONCAT(
                A.project_name,
                ': pool'
            )
            WHEN A.l1_label = 'cex'
            AND C.event_name IN (
                'LogNewWallet',
                'LogUserAdded'
            ) THEN CONCAT(
                A.project_name,
                ': deposit wallet'
            )
            WHEN C.event_name IN ('NewOracle') THEN CONCAT(
                A.project_name,
                ': oracle'
            )
            WHEN C.event_name IN ('LOG_NEW_POOL') THEN CONCAT(
                A.project_name,
                ': pool'
            )
            WHEN A.address_name = ' registry'
            AND A.project_name = 'opensea' THEN 'opensea: proxy registry'
            WHEN A.address_name ILIKE '%pool deployer%' THEN CONCAT(
                A.project_name,
                ': pool'
            )
            ELSE CONCAT(
                A.project_name,
                ': general contract'
            )
        END AS address_name_fixed,
        A.project_name,
        C.contract_name,
        C.event_name,
        A._inserted_timestamp
    FROM
        base_transacts A
        LEFT JOIN base_logs C
        ON A.tx_hash = C.tx_hash
)
SELECT
    DISTINCT system_created_at,
    insert_date,
    'ethereum' AS blockchain,
    to_address AS address,
    'flipside' AS creator,
    l1_label,
    l2_label_fixed AS l2_label,
    address_name_fixed AS address_name,
    project_name,
    _inserted_timestamp
FROM
    final_base qualify(ROW_NUMBER() over(PARTITION BY address
ORDER BY
    l2_label_fixed DESC)) = 1
