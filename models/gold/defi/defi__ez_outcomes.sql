{{ config(
    materialized = 'incremental',
    unique_key = "concat(blockchain, '|', platform, '|', action)",
    persist_docs = { "relation": true, "columns": true },
    meta = { 'database_tags':{ 'table':{ 'PURPOSE': 'DEFI, OUTCOMES' } } }   
) }}

WITH base AS (
    SELECT
         blockchain,
         platform,
         'lp'    AS action,
         MAX(creation_time) AS last_action_timestamp
    FROM 
        {{ ref('defi__dim_dex_liquidity_pools') }}
    GROUP BY 1,2,3

    UNION ALL
    SELECT
         blockchain,
         platform,
         'swap'  AS action,
         MAX(block_timestamp) AS last_action_timestamp
    FROM 
        {{ ref('defi__ez_dex_swaps') }}
    GROUP BY 1,2,3

    UNION ALL
    SELECT
         blockchain,
         platform,
         'bridge' AS action,
         MAX(block_timestamp) AS last_action_timestamp
    FROM 
        {{ ref('defi__fact_bridge_activity') }}
    GROUP BY 1,2,3
)
, outcomes_base AS (
    SELECT 
        blockchain,
        platform,
        CASE WHEN action IN('lp', 'swap') THEN 'Dexes' ELSE 'Bridge' END AS defillama_category,
        action,
        last_action_timestamp
    FROM 
        base
    {% if is_incremental() %}
    WHERE last_action_timestamp > (
        SELECT COALESCE(MAX(last_action_timestamp), '1970-01-01'::timestamp)
        FROM {{ this }}
        WHERE 
            blockchain = base.blockchain AND
            platform = base.platform AND
            action = base.action
    )
    {% endif %}
)
-- Step 1: Try exact match join
, exact_match AS (
    SELECT 
        o.*,
        p.protocol_id,
        p.protocol_slug,
        FALSE AS is_imputed,
        NULL AS imputed_protocol_id,
        NULL AS imputed_protocol_slug
    FROM outcomes_base o
    LEFT JOIN external_dev.defillama.dim_protocols p 
        ON p.protocol_slug = o.platform
)
-- Get exact match platforms to exclude from further matching
, exact_match_platforms AS (
    SELECT DISTINCT platform 
    FROM exact_match 
    WHERE protocol_id IS NOT NULL
)
-- Step 2: Try category + name match for remaining platforms
, category_match AS (
    SELECT
        o.*,
        MIN(p.protocol_id) AS protocol_id,
        MIN(p.protocol_slug) AS protocol_slug,
        TRUE AS is_imputed,
        ARRAY_AGG(DISTINCT p.protocol_id) WITHIN GROUP (ORDER BY p.protocol_id) AS imputed_protocol_id,
        ARRAY_AGG(DISTINCT p.protocol_slug) WITHIN GROUP (ORDER BY p.protocol_slug) AS imputed_protocol_slug
    FROM outcomes_base o
    LEFT JOIN external_dev.defillama.dim_protocols p
        ON SPLIT_PART(p.protocol_slug, '-', 1) = SPLIT_PART(o.platform, '-', 1)
        AND p.category = o.defillama_category
    WHERE o.platform NOT IN (SELECT platform FROM exact_match_platforms)
    GROUP BY o.blockchain, o.platform, o.defillama_category, o.action, o.last_action_timestamp
)
-- Get category match platforms to exclude from final matching
, category_match_platforms AS (
    SELECT DISTINCT platform 
    FROM category_match 
    WHERE protocol_id IS NOT NULL
)
-- Step 3: Try name-only match for remaining platforms
, name_match AS (
    SELECT
        o.*,
        MIN(p.protocol_id) AS protocol_id,
        MIN(p.protocol_slug) AS protocol_slug,
        TRUE AS is_imputed,
        ARRAY_AGG(DISTINCT p.protocol_id) WITHIN GROUP (ORDER BY p.protocol_id) AS imputed_protocol_id,
        ARRAY_AGG(DISTINCT p.protocol_slug) WITHIN GROUP (ORDER BY p.protocol_slug) AS imputed_protocol_slug
    FROM outcomes_base o
    LEFT JOIN external_dev.defillama.dim_protocols p
        ON SPLIT_PART(p.protocol_slug, '-', 1) = SPLIT_PART(o.platform, '-', 1)
    WHERE o.platform NOT IN (SELECT platform FROM exact_match_platforms)
      AND o.platform NOT IN (SELECT platform FROM category_match_platforms)
    GROUP BY o.blockchain, o.platform, o.defillama_category, o.action, o.last_action_timestamp
)

-- Combine all matches
SELECT * FROM exact_match WHERE protocol_id IS NOT NULL
UNION ALL 
SELECT * FROM category_match WHERE protocol_id IS NOT NULL
UNION ALL
SELECT * FROM name_match WHERE protocol_id IS NOT NULL