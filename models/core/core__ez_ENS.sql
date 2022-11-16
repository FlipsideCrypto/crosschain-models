{{ config(
    materialized = 'view',
    tags = ['snowflake', 'crosschain', 'ens'],
) }}

SELECT
    block_timestamp,
    tx_hash,
    owner,
    tokenid,
    ens_name,
    ens_set,
    cost,
    expiration_time,
    expiration_date,
    label,
    node,
    last_update_info,
    info_updater,
    twitter,
    avatar,
    discord,
    github,
    email,
    url,
    description,
    notice,
    keywords,
    reddit,
    telegram,
    opensea,
    rarible,
    superrare
FROM
    {{ ref('silver__ENS') }}