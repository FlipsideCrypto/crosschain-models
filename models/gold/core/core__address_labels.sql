{{ config(
  materialized = 'view',
  tags = ['snowflake', 'crosschain', 'labels', 'gold_address_labels'],
) }}

SELECT
  *
FROM
  {{ ref('core__dim_labels') }}
