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
    project_name
FROM
    {{ ref('silver__address_labels') }}
where delete_flag is null -- this should fix the issue with new pushes of deletes!
    -- deposit wallet algos
UNION ALL
SELECT
    system_created_at,
    insert_date,
    blockchain,
    address,
    creator,
    label_type,
    label_subtype,
    address_name,
    project_name
FROM
    {{ ref('silver__deposit_wallets') }}
UNION ALL
SELECT
  system_created_at,
  insert_date,
  blockchain,
  address,
  creator,
  label_type,
  label_subtype,
  address_name,
  project_name
FROM
  {{ ref('silver__contract_autolabels') }}

