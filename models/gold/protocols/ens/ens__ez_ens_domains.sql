{{ config(
    materialized = 'view',
    tags = ['snowflake', 'crosschain', 'ens'],
) }}

SELECT
    *
FROM
    {{ source(
        'ethereum_ens',
        'ez_ens_domains'
    ) }}
