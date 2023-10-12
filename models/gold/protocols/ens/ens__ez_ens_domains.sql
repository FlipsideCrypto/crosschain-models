{{ config(
    materialized = 'view',
    tags = ['snowflake', 'crosschain', 'ens'],
) }}

SELECT
    last_registered_block,
    last_registered_timestamp,
    last_registered_tx_hash,
    last_registered_contract,
    manager,
    owner,
    set_address,
    ens_set,
    ens_domain,
    ens_subdomains,
    label,
    node,
    token_id,
    last_registered_cost,
    last_registered_premium,
    renewal_cost,
    expiration_timestamp,
    expired,
    resolver,
    profile,
    last_updated
FROM
    {{ source(
        'ethereum_ens',
        'ez_ens_domains'
    ) }}
