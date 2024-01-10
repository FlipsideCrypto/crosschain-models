{{ config(
    materialized = 'incremental',
    unique_key = "CONCAT_WS('-', blockchain, address)",
    incremental_strategy = 'merge',
    tags = ['snowflake', 'crosschain', 'labels', 'silver__contract_autolabels']
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
        'N' AS _is_deleted
    FROM
        {{ ref('silver__deposit_wallets') }} A
        LEFT JOIN {{ this }}
        b
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
        'Y' AS _is_deleted
    FROM
        {{ this }} A
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
UNION
SELECT
    *
FROM
    delete_addresses
{% else %}
SELECT
    *,
    'N' AS _is_deleted
FROM
    {{ ref('silver__deposit_wallets') }}
{% endif %}
