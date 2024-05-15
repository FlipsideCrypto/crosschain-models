{{ config(
  materialized = 'view',
  meta ={ 'database_tags':{ 'table':{ 'PROTOCOL' :'COSMOS'
    } } }
) }}

SELECT
  address_base AS base_address,
  blockchain,
  address,
  block_timestamp,
  COALESCE(inserted_timestamp,'2000-01-01') as inserted_timestamp,
  COALESCE(modified_timestamp,'2000-01-01') as modified_timestamp,
  COALESCE(cosmos_base_address_id,{{ dbt_utils.generate_surrogate_key(['address']) }}) AS dim_base_address_id
FROM
  {{ ref('silver__cosmos_base_address') }}
