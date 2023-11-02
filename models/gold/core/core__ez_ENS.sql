{{ config(
    materialized = 'view',
    tags = ['snowflake', 'crosschain', 'ens'],
    enabled = false
) }}

SELECT
    *
FROM
    {{ ref('ens__ez_ens_domains') }}
