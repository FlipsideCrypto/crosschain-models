{{ config(
  materialized = 'view'
) }}

SELECT
  'axelar' AS blockchain,
  block_id,
  block_timestamp,
  tx_id,
  tx_from,
  codespace,
  fee,
  fee_denom,
  gas_used,
  gas_wanted,
  tx_code,
  tx_log,
  msgs,
  tx_succeeded
FROM
  {{ source(
    'axelar_core',
    'fact_transactions'
  ) }}
UNION ALL
SELECT
  'cosmos' AS blockchain,
  block_id,
  block_timestamp,
  tx_id,
  tx_from,
  codespace,
  fee,
  fee_denom,
  gas_used,
  gas_wanted,
  tx_code,
  tx_log,
  msgs,
  tx_succeeded
FROM
  {{ source(
    'cosmos_core',
    'fact_transactions'
  ) }}
UNION ALL
SELECT
  'osmosis' AS blockchain,
  block_id,
  block_timestamp,
  tx_id,
  tx_from,
  codespace :: variant AS codespace,
  REGEXP_SUBSTR(
    fee,
    '[0-9]+'
  ) AS fee,
  REGEXP_SUBSTR(
    fee,
    '[a-z]+'
  ) AS fee_denom,
  gas_used,
  gas_wanted,
  tx_code,
  NULL AS tx_log,
  msgs,
  tx_succeeded
FROM
  {{ source(
    'osmosis_core',
    'fact_transactions'
  ) }}
