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
         max(creation_time) AS last_action_timestamp
    FROM 
        {{ ref('defi__dim_dex_liquidity_pools') }}
    GROUP BY 1,2,3

    UNION ALL
    SELECT
         blockchain,
         platform,
         'swap'  AS action,
         max(block_timestamp) AS last_action_timestamp
    FROM 
        {{ ref('defi__ez_dex_swaps') }}
    GROUP BY 1,2,3

    UNION ALL
    SELECT
         blockchain,
         platform,
         'bridge' AS action,
         max(block_timestamp) AS last_action_timestamp
    FROM 
        {{ ref('defi__fact_bridge_activity') }}
    GROUP BY 1,2,3
)
, outcomes_base AS (
    SELECT 
        blockchain,
        platform,
        -- Extract version information from platform name
        REGEXP_SUBSTR(platform, 'v[0-9]+$') AS platform_version,
        -- Remove version from platform for better matching
        REGEXP_REPLACE(REGEXP_REPLACE(platform, 'v[0-9]+$', ''), '-', '') AS platform_base_name,
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
-- Load and parse DeFiLlama protocol data
, defillama_protocols AS (
    SELECT
        protocol_id,
        protocol_slug,
        category,
        -- Extract version information from protocol slug
        REGEXP_SUBSTR(protocol_slug, 'v[0-9]+$') AS protocol_version,
        -- Remove version from protocol slug for better matching
        REGEXP_REPLACE(REGEXP_REPLACE(protocol_slug, 'v[0-9]+$', ''), '-', '') AS protocol_base_name,
        -- Get first part of slug for partial matching
        SPLIT_PART(protocol_slug, '-', 1) AS protocol_first_part
    FROM external_dev.defillama.dim_protocols
)
-- Step 1: Try exact match join
, exact_match AS (
    SELECT 
        o.*,
        p.protocol_id,
        p.protocol_slug,
        p.protocol_version,
        FALSE AS is_imputed,
        NULL AS imputed_protocol_id,
        NULL AS imputed_protocol_slug,
        'exact_slug_match' AS match_type
    FROM outcomes_base o
    LEFT JOIN defillama_protocols p 
        ON p.protocol_slug = o.platform
)
-- Get exact match platforms to exclude from further matching
, exact_match_platforms AS (
    SELECT DISTINCT platform 
    FROM exact_match 
    WHERE protocol_id IS NOT NULL
)
-- Step 1.5: Try base name + version match (still considered exact)
, base_version_match AS (
    SELECT 
        o.*,
        p.protocol_id,
        p.protocol_slug,
        p.protocol_version,
        FALSE AS is_imputed, -- This is still considered an exact match
        NULL AS imputed_protocol_id,
        NULL AS imputed_protocol_slug,
        'exact_version_match' AS match_type
    FROM outcomes_base o
    LEFT JOIN defillama_protocols p
        ON p.protocol_first_part = o.platform_base_name
        AND p.protocol_version = o.platform_version
        AND o.platform_version IS NOT NULL  -- Only match when version is available
    WHERE o.platform NOT IN (SELECT platform FROM exact_match_platforms)
)
-- Get base+version match platforms to exclude
, base_version_match_platforms AS (
    SELECT DISTINCT platform 
    FROM base_version_match 
    WHERE protocol_id IS NOT NULL
)
-- Combined exact matches for exclusion
, all_exact_match_platforms AS (
    SELECT platform FROM exact_match_platforms
    UNION
    SELECT platform FROM base_version_match_platforms
)
-- Step 2: Try category + base name match for remaining platforms
, category_match AS (
    SELECT
        o.*,
        MIN(p.protocol_id) AS protocol_id,
        MIN(p.protocol_slug) AS protocol_slug,
        MIN(p.protocol_version) AS protocol_version,
        TRUE AS is_imputed,
        ARRAY_AGG(DISTINCT p.protocol_id) WITHIN GROUP (ORDER BY p.protocol_id) AS imputed_protocol_id,
        ARRAY_AGG(DISTINCT p.protocol_slug) WITHIN GROUP (ORDER BY p.protocol_slug) AS imputed_protocol_slug,
        'category_match' AS match_type
    FROM outcomes_base o
    LEFT JOIN defillama_protocols p
        ON p.protocol_base_name = o.platform_base_name
        AND p.category = o.defillama_category
        -- Match versions if both have them
        AND (
            (o.platform_version IS NULL AND p.protocol_version IS NULL)
            OR o.platform_version = p.protocol_version
        )
    WHERE o.platform NOT IN (SELECT platform FROM all_exact_match_platforms)
    GROUP BY o.blockchain, o.platform, o.platform_version, o.platform_base_name, o.defillama_category, o.action, o.last_action_timestamp
)
-- Get category match platforms to exclude from final matching
, category_match_platforms AS (
    SELECT DISTINCT platform 
    FROM category_match 
    WHERE protocol_id IS NOT NULL
)
-- Step 3: Try first-part match for remaining platforms
, name_match AS (
    SELECT
        o.*,
        MIN(p.protocol_id) AS protocol_id,
        MIN(p.protocol_slug) AS protocol_slug,
        MIN(p.protocol_version) AS protocol_version,
        TRUE AS is_imputed,
        ARRAY_AGG(DISTINCT p.protocol_id) WITHIN GROUP (ORDER BY p.protocol_id) AS imputed_protocol_id,
        ARRAY_AGG(DISTINCT p.protocol_slug) WITHIN GROUP (ORDER BY p.protocol_slug) AS imputed_protocol_slug,
        'name_match' AS match_type
    FROM outcomes_base o
    LEFT JOIN defillama_protocols p
        ON SPLIT_PART(o.platform_base_name, '-', 1) = p.protocol_first_part
    WHERE o.platform NOT IN (SELECT platform FROM all_exact_match_platforms)
      AND o.platform NOT IN (SELECT platform FROM category_match_platforms)
    GROUP BY o.blockchain, o.platform, o.platform_version, o.platform_base_name, o.defillama_category, o.action, o.last_action_timestamp
)
-- Get name match platforms to exclude from fuzzy matching
, name_match_platforms AS (
    SELECT DISTINCT platform 
    FROM name_match 
    WHERE protocol_id IS NOT NULL
)
-- Step 4: Try fuzzy matching for remaining platforms
, fuzzy_match AS (
    SELECT
        o.*,
        MIN(p.protocol_id) AS protocol_id,
        MIN(p.protocol_slug) AS protocol_slug,
        MIN(p.protocol_version) AS protocol_version,
        TRUE AS is_imputed,
        ARRAY_AGG(DISTINCT p.protocol_id) WITHIN GROUP (ORDER BY p.protocol_id) AS imputed_protocol_id,
        ARRAY_AGG(DISTINCT p.protocol_slug) WITHIN GROUP (ORDER BY p.protocol_slug) AS imputed_protocol_slug,
        'fuzzy_match' AS match_type
    FROM outcomes_base o
    JOIN defillama_protocols p
      ON (
          -- Sound-alike matching
          SOUNDEX(o.platform_base_name) = SOUNDEX(p.protocol_base_name)
          -- Substring matching
          OR p.protocol_slug ILIKE '%' || o.platform_base_name || '%'
          OR o.platform_base_name ILIKE '%' || p.protocol_first_part || '%'
          -- Regex pattern matching with flexible separators
          OR REGEXP_LIKE(p.protocol_slug, REPLACE(o.platform_base_name, '-', '[_\\s-]?'), 'i')
      )
      -- Only match within the same category when possible
      AND (p.category = o.defillama_category OR o.defillama_category IS NULL)
    WHERE o.platform NOT IN (SELECT platform FROM all_exact_match_platforms)
      AND o.platform NOT IN (SELECT platform FROM category_match_platforms)
      AND o.platform NOT IN (SELECT platform FROM name_match_platforms)
    GROUP BY o.blockchain, o.platform, o.platform_version, o.platform_base_name, o.defillama_category, o.action, o.last_action_timestamp
)
-- Get all platforms that have been matched so far
, all_matched_platforms AS (
    SELECT platform FROM exact_match WHERE protocol_id IS NOT NULL
    UNION ALL
    SELECT platform FROM base_version_match WHERE protocol_id IS NOT NULL
    UNION ALL
    SELECT platform FROM category_match WHERE protocol_id IS NOT NULL
    UNION ALL 
    SELECT platform FROM name_match WHERE protocol_id IS NOT NULL
    UNION ALL
    SELECT platform FROM fuzzy_match WHERE protocol_id IS NOT NULL
)
-- Find unmatched records
, unmatched AS (
    SELECT
        o.*,
        NULL AS protocol_id,
        NULL AS protocol_slug,
        NULL AS protocol_version,
        FALSE AS is_imputed,
        NULL AS imputed_protocol_id,
        NULL AS imputed_protocol_slug,
        'no_match' AS match_type
    FROM outcomes_base o
    WHERE o.platform NOT IN (SELECT platform FROM all_matched_platforms)
)

-- Combine all matches
SELECT 
    blockchain,
    platform,
    defillama_category,
    action,
    last_action_timestamp,
    protocol_id,
    is_imputed,
    imputed_protocol_id,
    imputed_protocol_slug,
    match_type
FROM exact_match 
WHERE protocol_id IS NOT NULL

UNION ALL

SELECT 
    blockchain,
    platform,
    defillama_category,
    action,
    last_action_timestamp,
    protocol_id,
    is_imputed,
    imputed_protocol_id,
    imputed_protocol_slug,
    match_type
FROM base_version_match
WHERE protocol_id IS NOT NULL

UNION ALL 

SELECT 
    blockchain,
    platform,
    defillama_category,
    action,
    last_action_timestamp,
    protocol_id,
    is_imputed,
    imputed_protocol_id,
    imputed_protocol_slug,
    match_type
FROM category_match 
WHERE protocol_id IS NOT NULL

UNION ALL

SELECT 
    blockchain,
    platform,
    defillama_category,
    action,
    last_action_timestamp,
    protocol_id,
    is_imputed,
    imputed_protocol_id,
    imputed_protocol_slug,
    match_type
FROM name_match 
WHERE protocol_id IS NOT NULL

UNION ALL

SELECT 
    blockchain,
    platform,
    defillama_category,
    action,
    last_action_timestamp,
    protocol_id,
    is_imputed,
    imputed_protocol_id,
    imputed_protocol_slug,
    match_type
FROM fuzzy_match
WHERE protocol_id IS NOT NULL

UNION ALL

SELECT 
    blockchain,
    platform,
    defillama_category,
    action,
    last_action_timestamp,
    protocol_id,
    is_imputed,
    imputed_protocol_id,
    imputed_protocol_slug,
    match_type
FROM unmatched