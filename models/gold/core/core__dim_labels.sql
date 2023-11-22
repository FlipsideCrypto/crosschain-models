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
    project_name,
    COALESCE(inserted_timestamp,'2000-01-01') as inserted_timestamp,
    COALESCE(modified_timestamp,'2000-01-01') as modified_timestamp,
    COALESCE(address_labels_id,{{ dbt_utils.generate_surrogate_key(['blockchain','creator','address']) }}) AS dim_labels_id
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
    project_name,
    COALESCE(inserted_timestamp,'2000-01-01') as inserted_timestamp,
    COALESCE(modified_timestamp,'2000-01-01') as modified_timestamp,
    COALESCE(deposit_wallets_id,{{ dbt_utils.generate_surrogate_key(['blockchain','creator','address']) }}) AS dim_labels_id
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
  project_name,
  COALESCE(inserted_timestamp,'2000-01-01') as inserted_timestamp,
  COALESCE(modified_timestamp,'2000-01-01') as modified_timestamp,
  COALESCE(contract_autolabels_id,{{ dbt_utils.generate_surrogate_key(['blockchain','creator','address']) }}) AS dim_labels_id
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
  project_name,
  COALESCE(inserted_timestamp,'2000-01-01') as inserted_timestamp,
  COALESCE(modified_timestamp,'2000-01-01') as modified_timestamp,
  COALESCE(labels_eth_contracts_table_id,{{ dbt_utils.generate_surrogate_key(['address']) }}) AS dim_labels_id
FROM
  {{ ref('silver__labels_eth_contracts_table') }}

