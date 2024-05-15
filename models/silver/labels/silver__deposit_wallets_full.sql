{{ config(
    materialized = 'incremental',
    unique_key = "deposit_wallets_id",
    incremental_strategy = 'merge',
    tags = ['daily']
) }}

{% if is_incremental() %}
WITH new_addresses AS (

    SELECT
        A.system_created_at,
        A.insert_date,
        A.blockchain,
        A.address,
        A.creator,
        A.label_type,
        A.label_subtype,
        A.address_name,
        A.project_name,
        A.inserted_timestamp,
        A.modified_timestamp,
        A.deposit_wallets_id,
        A._invocation_id,
        FALSE AS _is_deleted
    FROM
        {{ ref('silver__deposit_wallets') }} A
        LEFT JOIN (
            SELECT
                blockchain,
                address,
                _is_deleted
            FROM
                {{ this }}
            WHERE
                _is_deleted = FALSE
        ) b
        ON A.blockchain = b.blockchain
        AND A.address = b.address
    WHERE
        b.address IS NULL
),
delete_addresses AS (
    SELECT
        A.system_created_at,
        A.insert_date,
        A.blockchain,
        A.address,
        NULL AS creator,
        NULL AS label_type,
        NULL AS label_subtype,
        NULL AS address_name,
        NULL AS project_name,
        A.inserted_timestamp AS inserted_timestamp,
        SYSDATE() AS modified_timestamp,
        A.deposit_wallets_id,
        '{{ invocation_id }}' AS _invocation_id,
        TRUE AS _is_deleted
    FROM
        (
            SELECT
                system_created_at,
                insert_date,
                blockchain,
                address,
                deposit_wallets_id,
                _is_deleted,
                inserted_timestamp
            FROM
                {{ this }}
            WHERE
                _is_deleted = FALSE
        ) A
        LEFT JOIN {{ ref('silver__deposit_wallets') }}
        b
        ON A.blockchain = b.blockchain
        AND A.address = b.address
    WHERE
        b.address IS NULL
)
SELECT
    *
FROM
    new_addresses
UNION ALL
SELECT
    *
FROM
    delete_addresses
{% else %}
SELECT
    *,
    FALSE AS _is_deleted
FROM
    {{ ref('silver__deposit_wallets') }}
{% endif %}
