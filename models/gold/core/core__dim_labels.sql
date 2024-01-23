{{ config(
    materialized = 'view',
    tags = ['snowflake', 'crosschain', 'labels', 'gold_address_labels'],
) }}

SELECT
    system_created_at,
    insert_date,
    blockchain,
    address,
    creator,
    label_type,
    label_subtype,
    address_name,
    project_name,
    inserted_timestamp,
    modified_timestamp,
    dim_labels_id
FROM
    {{ ref('silver__dim_labels') }}
where _is_deleted = FALSE
