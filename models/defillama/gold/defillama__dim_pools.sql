{{ config(
    materialized = 'view',
    persist_docs ={ "relation": true,
    "columns": true }
) }}

SELECT
    chain,
    project AS protocol,
    symbol,
    CASE
        WHEN rewardtokens ILIKE 'none' THEN NULL
        ELSE LOWER(rewardtokens)
    END AS reward_tokens,
    pool AS pool_id,
    stablecoin AS is_stablecoin,
    ilrisk,
    exposure AS exposure_type,
    poolmeta AS pool_metadata,
    CASE
        WHEN underlyingtokens ILIKE 'none' THEN NULL
        ELSE LOWER(underlyingtokens)
    END AS underlying_tokens
FROM
    {{ source(
        'crosschain_dev_silver',
        'defillama_api_pools_20221219_154038'
    ) }}
