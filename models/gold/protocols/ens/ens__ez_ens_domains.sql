{{ config(
    materialized = 'view',
    persist_docs ={ "relation": true,
    "columns": true },
    meta ={ 'database_tags':{ 'table':{ 'PROTOCOL': 'ENS',
    'PURPOSE': 'DOMAINS',
    } } }
) }}

SELECT
    *
FROM
    {{ source(
        'ethereum_ens',
        'ez_ens_domains'
    ) }}
