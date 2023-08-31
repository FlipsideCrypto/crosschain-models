{{ config(
  materialized = 'view'
) }}

SELECT
  address_base AS base_address,
  blockchain,
  address,
  block_timestamp
FROM
  {{ ref('silver__cosmos_base_address') }}
