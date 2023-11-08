{{ config(
  materialized = 'view'
) }}

SELECT
  'axelar' AS blockchain,
  block_id,
  block_timestamp,
  tx_id,
  tx_succeeded,
  voter,
  proposal_id,
  vote_option,
  vote_weight,
  NULL AS memo
FROM
  {{ source(
    'axelar_gov',
    'fact_governance_votes'
  ) }}
UNION ALL
SELECT
  'cosmos' AS blockchain,
  block_id,
  block_timestamp,
  tx_id,
  tx_succeeded,
  voter,
  proposal_id,
  vote_option,
  vote_weight,
  NULL AS memo
FROM
  {{ source(
    'cosmos_gov',
    'fact_governance_votes'
  ) }}
UNION ALL
SELECT
  'osmosis' AS blockchain,
  block_id,
  block_timestamp,
  tx_id,
  tx_succeeded,
  voter,
  proposal_id,
  vote_option,
  vote_weight,
  memo
FROM
  {{ source(
    'osmosis_gov',
    'fact_governance_votes'
  ) }}
