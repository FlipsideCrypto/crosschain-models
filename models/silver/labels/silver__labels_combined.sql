{{ config(
    materialized = 'incremental',
    incremental_strategy = 'merge',
    unique_key = ['blockchain','address'],
    cluster_by = ['blockchain','_is_deleted','modified_timestamp::DATE'],    
    merge_exclude_columns = ["inserted_timestamp"],
    post_hook = "ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION ON EQUALITY(address)",
    tags = ['snowflake', 'crosschain', 'labels', 'gold_address_labels','daily']
) }}

WITH base as (
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
    SYSDATE() as inserted_timestamp,
    SYSDATE() as modified_timestamp,
    {{ dbt_utils.generate_surrogate_key(['blockchain','address']) }} AS labels_combined_id,
    'address_labels' as source,
    case when delete_flag is null then FALSE else TRUE end as _is_deleted
FROM
    {{ ref('silver__address_labels') }}
{% if is_incremental() %}
WHERE
  modified_timestamp >= (
    SELECT
      MAX(modified_timestamp)
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
    SYSDATE() as inserted_timestamp,
    SYSDATE() as modified_timestamp,
    {{ dbt_utils.generate_surrogate_key(['blockchain','address']) }} AS labels_combined_id,
    'deposit' as source,
    _is_deleted
FROM
    {{ ref('silver__deposit_wallets_full') }}
{% if is_incremental() %}
WHERE
  modified_timestamp >= (
    SELECT
      MAX(modified_timestamp)
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
  SYSDATE() as inserted_timestamp,
  SYSDATE() as modified_timestamp,
  {{ dbt_utils.generate_surrogate_key(['blockchain','address']) }}  AS labels_combined_id,
  'autolabel' as source,
  FALSE as _is_deleted
FROM
  {{ ref('silver__contract_autolabels') }}
{% if is_incremental() %}
WHERE
  modified_timestamp >= (
    SELECT
      MAX(modified_timestamp)
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
  SYSDATE() as inserted_timestamp,
  SYSDATE() as modified_timestamp,
  {{ dbt_utils.generate_surrogate_key(['blockchain','address']) }}  AS labels_combined_id,
  'eth_contracts' as source,
  FALSE as _is_deleted
FROM
  {{ ref('silver__labels_eth_contracts_table') }}
{% if is_incremental() %}
WHERE
  modified_timestamp >= (
    SELECT
      MAX(modified_timestamp)
    FROM
      {{ this }}
    where source = 'eth_contracts'
  )
{% endif %}
)
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
  labels_combined_id,
  source,
  _is_deleted
FROM
  base qualify (ROW_NUMBER() over (PARTITION BY blockchain, address
ORDER BY
  modified_timestamp DESC) = 1)
