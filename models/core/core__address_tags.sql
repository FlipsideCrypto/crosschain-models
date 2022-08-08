{{ config(
    materialized = 'view',
) }}

SELECT
    blockchain,
    creator,
    address,
    tag_name,
    tag_type,
    start_date,
    end_date,
    tag_created_at
FROM
    {{ ref('silver__tags_contract_address_eth') }}
UNION
SELECT
    blockchain,
    creator,
    address,
    tag_name,
    tag_type,
    start_date,
    end_date,
    tag_created_at
FROM
    {{ ref('silver__tags_gnosis_safe_address') }}
