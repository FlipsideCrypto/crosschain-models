{{ config(
  materialized = 'view'
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
  raw_metadata
FROM
  {{ source(
    'axelar_core',
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
  raw_metadata
FROM
  {{ source(
    'cosmos_core',
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
  raw_metadata
FROM
  {{ source(
    'osmosis_core',
    'fact_validators'
  ) }}
