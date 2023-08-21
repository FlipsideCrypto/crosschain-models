{{ config(
    materialized = 'view',
    tags = ['snowflake', 'crosschain', 'ens'],
) }}

SELECT
    *
FROM
    {{ ref('ens__ez_ens_domains') }}
