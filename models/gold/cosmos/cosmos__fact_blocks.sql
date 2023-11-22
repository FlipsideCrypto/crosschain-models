{{ config(
  materialized = 'view'
) }}

SELECT
  'axelar' AS blockchain,
  block_id,
  block_timestamp,
  chain_id,
  tx_count,
  proposer_address,
  validator_hash,
  COALESCE(inserted_timestamp,'2000-01-01') as inserted_timestamp,
  COALESCE(modified_timestamp,'2000-01-01') as modified_timestamp,
  COALESCE(fact_blocks_id,{{ dbt_utils.generate_surrogate_key(['chain_id','block_id']) }}) AS fact_blocks_id
FROM
  {{ source(
    'axelar_core',
    'fact_blocks'
  ) }}
UNION ALL
SELECT
  'cosmos' AS blockchain,
  block_id,
  block_timestamp,
  chain_id,
  tx_count,
  proposer_address,
  validator_hash,
  COALESCE(inserted_timestamp,'2000-01-01') as inserted_timestamp,
  COALESCE(modified_timestamp,'2000-01-01') as modified_timestamp,
  COALESCE(fact_blocks_id,{{ dbt_utils.generate_surrogate_key(['chain_id','block_id']) }}) AS fact_blocks_id
FROM
  {{ source(
    'cosmos_core',
    'fact_blocks'
  ) }}
UNION ALL
SELECT
  'osmosis' AS blockchain,
  block_id,
  block_timestamp,
  chain_id,
  tx_count,
  proposer_address,
  validator_hash,
  COALESCE(inserted_timestamp,'2000-01-01') as inserted_timestamp,
  COALESCE(modified_timestamp,'2000-01-01') as modified_timestamp,
  COALESCE(fact_blocks_id,{{ dbt_utils.generate_surrogate_key(['chain_id','block_id']) }}) AS fact_blocks_id
FROM
  {{ source(
    'osmosis_core',
    'fact_blocks'
  ) }}
