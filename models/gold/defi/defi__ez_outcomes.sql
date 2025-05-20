{{ config(
    materialized = 'view',
    persist_docs ={ "relation": true,
    "columns": true },
    meta ={ 'database_tags':{ 'table':{ 'PURPOSE': 'DEFI, OUTCOMES',
    } } }   
) }}

WITH base AS (

    SELECT DISTINCT
        blockchain,
        platform,
        'lp' as action,
        MAX(creation_time) as last_action_timestamp
    FROM 
        {{ ref('defi__dim_dex_liquidity_pools') }}
    GROUP BY 1,2,3

    UNION ALL

    SELECT DISTINCT
        blockchain,
        platform,
        'swap' as action,
        MAX(block_timestamp) as last_action_timestamp
    FROM 
        {{ ref('defi__ez_dex_swaps') }}
    GROUP BY 1,2,3

    UNION ALL

    SELECT DISTINCT
        blockchain,
        platform,
        'bridge' as action,
        MAX(block_timestamp) as last_action_timestamp
    FROM 
        {{ ref('defi__fact_bridge_activity') }}
    GROUP BY 1,2,3

)

SELECT 
    blockchain,
    platform,
    action,
    last_action_timestamp
FROM 
    base
    
