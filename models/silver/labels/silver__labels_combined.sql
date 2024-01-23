{{ config(
    materialized = 'incremental',
    unique_key = ['blockchain','address'],
    cluster_by = ['blockchain','_is_deleted','modified_timestamp::DATE'],
    post_hook = "ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION ON EQUALITY(address)",
    tags = ['snowflake', 'crosschain', 'labels', 'gold_address_labels']
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
    COALESCE(address_labels_id,{{ dbt_utils.generate_surrogate_key(['blockchain','creator','address']) }}) AS dim_labels_id,
    'address_labels' as source,
    case when delete_flag is null then FALSE else TRUE end as _is_deleted
FROM
    {{ ref('silver__address_labels') }}
{% if is_incremental() %}
WHERE
  modified_timestamp >= (
    SELECT
      MAX(insert_date)
    FROM
      {{ this }}
    where source = 'address_labels'
  )
{% endif %}
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
    COALESCE(deposit_wallets_id,{{ dbt_utils.generate_surrogate_key(['blockchain','creator','address']) }}) AS dim_labels_id,
    'deposit' as source,
    _is_deleted
FROM
    {{ ref('silver__deposit_wallets_full') }}
{% if is_incremental() %}
WHERE
  modified_timestamp >= (
    SELECT
      MAX(insert_date)
    FROM
      {{ this }}
    where source = 'deposit'
  )
{% endif %}
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
  COALESCE(contract_autolabels_id,{{ dbt_utils.generate_surrogate_key(['blockchain','creator','address']) }}) AS dim_labels_id,
  'autolabel' as source,
  FALSE as _is_deleted
FROM
  {{ ref('silver__contract_autolabels') }}
{% if is_incremental() %}
WHERE
  modified_timestamp >= (
    SELECT
      MAX(insert_date)
    FROM
      {{ this }}
    where source = 'autolabel'
  )
{% endif %}
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
  COALESCE(labels_eth_contracts_table_id,{{ dbt_utils.generate_surrogate_key(['address']) }}) AS dim_labels_id,
  'eth_contracts' as source,
  FALSE as _is_deleted
FROM
  {{ ref('silver__labels_eth_contracts_table') }}
{% if is_incremental() %}
WHERE
  modified_timestamp >= (
    SELECT
      MAX(insert_date)
    FROM
      {{ this }}
    where source = 'eth_contracts'
  )
{% endif %}

