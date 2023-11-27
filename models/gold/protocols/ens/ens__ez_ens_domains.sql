{{ config(
    materialized = 'view',
    tags = ['snowflake', 'crosschain', 'ens'],
) }}

SELECT
    *,
    COALESCE(inserted_timestamp,'2000-01-01') as inserted_timestamp,
    COALESCE(modified_timestamp,'2000-01-01') as modified_timestamp,
    COALESCE(ez_ens_domains_id,{{ dbt_utils.generate_surrogate_key(['ens_domain','label']) }}) as ez_ens_domains_id
FROM
    {{ source(
        'ethereum_ens',
        'ez_ens_domains'
    ) }}
