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
    case when label_type = 'layer2' then 'bridge' else label_type end as label_type,
    label_subtype,
    address_name,
    project_name
FROM
    {{ ref('silver__address_labels') }}
where delete_flag is null 
UNION ALL
SELECT
    system_created_at,
    insert_date,
    blockchain,
    address,
    creator,
    case when label_type = 'layer2' then 'bridge' else label_type end as label_type,
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
  case when label_type = 'layer2' then 'bridge' else label_type end as label_type,
  label_subtype,
  address_name,
  project_name
FROM
  {{ ref('silver__contract_autolabels') }}
UNION ALL
SELECT
  system_created_at,
  insert_date,
  blockchain,
  address,
  creator,
  case when label_type = 'layer2' then 'bridge' else label_type end as label_type,
  label_subtype,
  address_name,
  project_name
FROM
  {{ ref('silver__labels_eth_contracts_table') }}

