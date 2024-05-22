{{ config(
  materialized = 'view',
  meta ={ 'database_tags':{ 'table':{ 'PROTOCOL' :'COSMOS, AXELAR, OSMOSIS',
  'PURPOSE' :'GOVERNANCE' } } }
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
  COALESCE(
    inserted_timestamp,
    '2000-01-01'
  ) AS inserted_timestamp,
  COALESCE(
    modified_timestamp,
    '2000-01-01'
  ) AS modified_timestamp,
  {{ dbt_utils.generate_surrogate_key(['blockchain','tx_id','proposal_id','voter','vote_option']) }} AS fact_governance_votes_id
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
  COALESCE(
    inserted_timestamp,
    '2000-01-01'
  ) AS inserted_timestamp,
  COALESCE(
    modified_timestamp,
    '2000-01-01'
  ) AS modified_timestamp,
  {{ dbt_utils.generate_surrogate_key(['blockchain','tx_id','proposal_id','voter','vote_option']) }} AS fact_governance_votes_id
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
  COALESCE(
    inserted_timestamp,
    '2000-01-01'
  ) AS inserted_timestamp,
  COALESCE(
    modified_timestamp,
    '2000-01-01'
  ) AS modified_timestamp,
  {{ dbt_utils.generate_surrogate_key(['blockchain','tx_id','proposal_id','voter','vote_option']) }} AS fact_governance_votes_id
FROM
  {{ source(
    'osmosis_gov',
    'fact_governance_votes'
  ) }}
