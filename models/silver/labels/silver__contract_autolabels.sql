{{ config(
  materialized = 'incremental',
  unique_key = "CONCAT_WS('-', blockchain, address, creator)",
  incremental_strategy = 'delete+insert',
  tags = ['snowflake', 'crosschain', 'labels', 'silver__contract_autolabels'],
  post_hook = "delete from {{this}} a using (select distinct blockchain, address from {{ ref('silver__address_labels') }} where delete_flag is null union select distinct blockchain, address from {{ ref('silver__deposit_wallets') }}) b where a.blockchain = b.blockchain and a.address = b.address ",
) }}

WITH pre_final AS (

  SELECT
    system_created_at,
    insert_date,
    blockchain,
    address,
    creator,
    l1_label AS label_type,
    l2_label AS label_subtype,
    address_name,
    project_name,
    NULL AS delete_flag,
    _inserted_timestamp
  FROM
    {{ ref('silver__labels_contracts') }}
  UNION ALL
  SELECT
    system_created_at,
    insert_date,
    blockchain,
    address,
    creator,
    l1_label AS label_type,
    l2_label AS label_subtype,
    address_name,
    project_name,
    NULL AS delete_flag,
    _inserted_timestamp
  FROM
    {{ ref('silver__labels_contracts_arbitrum') }}
  UNION ALL
  SELECT
    system_created_at,
    insert_date,
    blockchain,
    address,
    creator,
    l1_label AS label_type,
    l2_label AS label_subtype,
    address_name,
    project_name,
    NULL AS delete_flag,
    _inserted_timestamp
  FROM
    {{ ref('silver__labels_contracts_avalanche') }}
  UNION ALL
  SELECT
    system_created_at,
    insert_date,
    blockchain,
    address,
    creator,
    l1_label AS label_type,
    l2_label AS label_subtype,
    address_name,
    project_name,
    NULL AS delete_flag,
    _inserted_timestamp
  FROM
    {{ ref('silver__labels_contracts_base') }}
  UNION ALL
  SELECT
    system_created_at,
    insert_date,
    blockchain,
    address,
    creator,
    l1_label AS label_type,
    l2_label AS label_subtype,
    address_name,
    project_name,
    NULL AS delete_flag,
    _inserted_timestamp
  FROM
    {{ ref('silver__labels_contracts_bsc') }}
  UNION ALL
  SELECT
    system_created_at,
    insert_date,
    blockchain,
    address,
    creator,
    l1_label AS label_type,
    l2_label AS label_subtype,
    address_name,
    project_name,
    NULL AS delete_flag,
    _inserted_timestamp
  FROM
    {{ ref('silver__labels_contracts_optimism') }}
  UNION ALL
  SELECT
    system_created_at,
    insert_date,
    blockchain,
    address,
    creator,
    l1_label AS label_type,
    l2_label AS label_subtype,
    address_name,
    project_name,
    NULL AS delete_flag,
    _inserted_timestamp
  FROM
    {{ ref('silver__labels_contracts_polygon') }}
  UNION ALL
  SELECT
    system_created_at,
    insert_date,
    blockchain,
    address,
    creator,
    l1_label AS label_type,
    l2_label AS label_subtype,
    address_name,
    project_name,
    NULL AS delete_flag,
    _inserted_timestamp
  FROM
    {{ ref('silver__labels_contracts_solana') }}
  UNION ALL
  SELECT
    system_created_at,
    insert_date,
    blockchain,
    address,
    creator,
    l1_label AS label_type,
    l2_label AS label_subtype,
    address_name,
    project_name,
    NULL AS delete_flag,
    _inserted_timestamp
  FROM
    {{ ref('silver__labels_tokens_solana') }}
)
SELECT
  *,
  SYSDATE() AS inserted_timestamp,
  SYSDATE() AS modified_timestamp,
  {{ dbt_utils.generate_surrogate_key(['blockchain','creator','address']) }} AS contract_autolabels_id,
  '{{ invocation_id }}' AS _invocation_id
FROM
  pre_final
