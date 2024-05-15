{{ config(
  materialized = 'view',
  meta ={ 'database_tags':{ 'table':{ 'PROTOCOL' :'COSMOS, AXELAR, OSMOSIS'
    } } }
) }}

SELECT
  'axelar' AS blockchain,
  address,
  creator,
  label_type,
  label_subtype,
  label,
  project_name,
  NULL AS account_address,
  delegator_shares,
  jailed,
  max_change_rate,
  max_rate,
  min_self_delegation,
  RANK,
  NULL AS missed_blocks,
  raw_metadata,
  COALESCE(inserted_timestamp,'2000-01-01') as inserted_timestamp,
  COALESCE(modified_timestamp,'2000-01-01') as modified_timestamp,
  COALESCE(fact_validators_id,{{ dbt_utils.generate_surrogate_key(['address','creator','blockchain']) }}) AS fact_validators_id
FROM
  {{ source(
    'axelar_gov',
    'fact_validators'
  ) }}
UNION ALL
SELECT
  'cosmos' AS blockchain,
  address,
  creator,
  label_type,
  label_subtype,
  label,
  project_name,
  account_address,
  delegator_shares,
  jailed,
  max_change_rate,
  max_rate,
  NULL AS min_self_delegation,
  RANK,
  NULL AS missed_blocks,
  raw_metadata,
  COALESCE(inserted_timestamp,'2000-01-01') as inserted_timestamp,
  COALESCE(modified_timestamp,'2000-01-01') as modified_timestamp,
  COALESCE(fact_validators_id,{{ dbt_utils.generate_surrogate_key(['address','creator','blockchain']) }}) AS fact_validators_id
FROM
  {{ source(
    'cosmos_gov',
    'fact_validators'
  ) }}
UNION ALL
SELECT
  'osmosis' AS blockchain,
  address,
  creator,
  label_type,
  label_subtype,
  label,
  project_name,
  account_address,
  delegator_shares,
  jailed,
  max_change_rate,
  max_rate,
  min_self_delegation,
  RANK,
  missed_blocks,
  raw_metadata,
  COALESCE(inserted_timestamp,'2000-01-01') as inserted_timestamp,
  COALESCE(modified_timestamp,'2000-01-01') as modified_timestamp,
  COALESCE(fact_validators_id,{{ dbt_utils.generate_surrogate_key(['address','creator','blockchain']) }}) AS fact_validators_id
FROM
  {{ source(
    'osmosis_gov',
    'fact_validators'
  ) }}
