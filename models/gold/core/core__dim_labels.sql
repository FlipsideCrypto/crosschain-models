{{ config(
    materialized = 'view'
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
    labels_combined_id AS dim_labels_id
FROM
    {{ ref('silver__labels_combined') }}
WHERE
    _is_deleted = FALSE
