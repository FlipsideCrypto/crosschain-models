{{ config(
    materialized = 'view',
    persist_docs ={ "relation": true,
    "columns": true }
) }}

SELECT
    pool AS pool_id,
    project AS protocol,
    symbol,
    chain,
    CASE
        WHEN rewardtokens ILIKE 'none' THEN NULL
        ELSE LOWER(rewardtokens)
    END AS reward_tokens,
    CASE
        WHEN underlyingtokens ILIKE 'none' THEN NULL
        ELSE LOWER(underlyingtokens)
    END AS underlying_tokens,
    stablecoin AS is_stablecoin,
    ilrisk,
    exposure AS exposure_type,
    poolmeta AS pool_metadata
FROM
    {{ source(
        'crosschain_dev_silver',
        'defillama_api_pools_20221219_154038'
    ) }}
