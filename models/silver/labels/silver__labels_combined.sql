{{ config(
  materialized = 'incremental',
  incremental_strategy = 'merge',
  unique_key = ['blockchain','address'],
  cluster_by = ['blockchain','_is_deleted','modified_timestamp::DATE'],
  merge_exclude_columns = ["inserted_timestamp"],
  post_hook = "ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION ON EQUALITY(address)",
  tags = ['snowflake', 'crosschain', 'labels', 'gold_address_labels']
) }}

WITH base AS (

  SELECT
    system_created_at,
    insert_date,
    blockchain,
    address,
    creator,
    CASE
      WHEN label_type = 'layer2' THEN 'bridge'
      ELSE label_type
    END AS label_type,
    label_subtype,
    address_name,
    project_name,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    {{ dbt_utils.generate_surrogate_key(['blockchain','address']) }} AS labels_combined_id,
    'address_labels' AS source,
    CASE
      WHEN delete_flag IS NULL THEN FALSE
      ELSE TRUE
    END AS _is_deleted
  FROM
    {{ ref('silver__address_labels') }}

{% if is_incremental() %}
WHERE
  modified_timestamp >= (
    SELECT
      MAX(modified_timestamp)
    FROM
      {{ this }}
    WHERE
      source = 'address_labels'
  )
{% endif %}
UNION ALL
SELECT
  system_created_at,
  insert_date,
  blockchain,
  address,
  creator,
  CASE
    WHEN label_type = 'layer2' THEN 'bridge'
    ELSE label_type
  END AS label_type,
  label_subtype,
  address_name,
  project_name,
  SYSDATE() AS inserted_timestamp,
  SYSDATE() AS modified_timestamp,
  {{ dbt_utils.generate_surrogate_key(['blockchain','address']) }} AS labels_combined_id,
  'deposit' AS source,
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
    WHERE
      source = 'deposit'
  )
{% endif %}
UNION ALL
SELECT
  system_created_at,
  insert_date,
  blockchain,
  address,
  creator,
  CASE
    WHEN label_type = 'layer2' THEN 'bridge'
    ELSE label_type
  END AS label_type,
  label_subtype,
  address_name,
  project_name,
  SYSDATE() AS inserted_timestamp,
  SYSDATE() AS modified_timestamp,
  {{ dbt_utils.generate_surrogate_key(['blockchain','address']) }} AS labels_combined_id,
  'autolabel' AS source,
  FALSE AS _is_deleted
FROM
  {{ ref('silver__contract_autolabels') }}

{% if is_incremental() %}
WHERE
  modified_timestamp >= (
    SELECT
      MAX(modified_timestamp)
    FROM
      {{ this }}
    WHERE
      source = 'autolabel'
  )
{% endif %}
UNION ALL
SELECT
  system_created_at,
  insert_date,
  blockchain,
  address,
  creator,
  CASE
    WHEN label_type = 'layer2' THEN 'bridge'
    ELSE label_type
  END AS label_type,
  label_subtype,
  address_name,
  project_name,
  SYSDATE() AS inserted_timestamp,
  SYSDATE() AS modified_timestamp,
  {{ dbt_utils.generate_surrogate_key(['blockchain','address']) }} AS labels_combined_id,
  'eth_contracts' AS source,
  FALSE AS _is_deleted
FROM
  {{ ref('silver__labels_eth_contracts_table') }}

{% if is_incremental() %}
WHERE
  modified_timestamp >= (
    SELECT
      MAX(modified_timestamp)
    FROM
      {{ this }}
    WHERE
      source = 'eth_contracts'
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
