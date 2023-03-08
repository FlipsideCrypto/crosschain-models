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
  validator_hash
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
  validator_hash
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
  validator_hash
FROM
  {{ source(
    'osmosis_core',
    'fact_blocks'
  ) }}
