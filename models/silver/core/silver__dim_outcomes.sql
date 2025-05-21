{{ config(
    materialized = 'incremental',
    unique_key = ['blockchain', 'platform', 'action'],
    merge_exclude_columns = ['inserted_timestamp'],
    tags = ['daily']
) }}

{% if execute %}

{% if is_incremental() %}
{% set max_mod_query %}

SELECT
    MAX(modified_timestamp) :: DATE AS modified_timestamp
FROM
    {{ this }}

    {% endset %}
    {% set max_mod = run_query(max_mod_query) [0] [0] %}
{% endif %}
{% endif %}


-- Gather latest activity timestamps by platform and action type
WITH base AS (
    SELECT
        blockchain,
        platform,
        'lp' AS action,
        MAX(creation_time) AS last_action_timestamp
    FROM
        {{ ref('defi__dim_dex_liquidity_pools') }}
{% if is_incremental() %}
WHERE modified_timestamp :: DATE >= '{{ max_mod }}'
{% endif %}
    GROUP BY
        blockchain,
        platform,
        action
    UNION ALL
    SELECT
        blockchain,
        platform,
        'swap' AS action,
        MAX(block_timestamp) AS last_action_timestamp
    FROM
        {{ ref('defi__ez_dex_swaps') }}
{% if is_incremental() %}
WHERE modified_timestamp :: DATE >= '{{ max_mod }}'
{% endif %}
    GROUP BY
        blockchain,
        platform,
        action
    UNION ALL
    SELECT
        blockchain,
        platform,
        'bridge' AS action,
        MAX(block_timestamp) AS last_action_timestamp
    FROM
        {{ ref('defi__fact_bridge_activity') }}
{% if is_incremental() %}
WHERE modified_timestamp :: DATE >= '{{ max_mod }}'
{% endif %}
    GROUP BY
        blockchain,
        platform,
        action
),

-- Prepare base data with platform naming variations
outcomes_base AS (
    SELECT
        blockchain,
        platform,
        REGEXP_SUBSTR(
            platform,
            'v[0-9]+$'
        ) AS platform_version,
        REGEXP_REPLACE(REGEXP_REPLACE(platform, 'v[0-9]+$', ''), '-', '') AS platform_base_name,
        CASE
            WHEN action IN(
                'lp',
                'swap'
            ) THEN 'Dexes'
            ELSE 'Bridge'
        END AS defillama_category,
        action,
        last_action_timestamp
    FROM
        base b
),

-- Manual mappings from seed file
manual_mappings AS (
    SELECT
        platform,
        protocol_id,
        protocol_slug,
        'seed_file_chatgpt' AS match_type
    FROM
        {{ ref('silver__outcomes_protocol_mappings') }}
),

-- Platforms with manual mappings
manual_mapping_platforms AS (
    SELECT
        DISTINCT platform
    FROM
        manual_mappings
),

-- DeFiLlama protocol reference data
defillama_protocols AS (
    SELECT
        protocol_id,
        protocol_slug,
        category,
        REGEXP_SUBSTR(
            protocol_slug,
            'v[0-9]+$'
        ) AS protocol_version,
        REGEXP_REPLACE(REGEXP_REPLACE(protocol_slug, 'v[0-9]+$', ''), '-', '') AS protocol_base_name,
        SPLIT_PART(
            protocol_slug,
            '-',
            1
        ) AS protocol_first_part
    FROM
        {{ source('external_defillama', 'dim_protocols') }}
),

-- Base data with potential manual mappings
outcomes_with_manual_mappings AS (
    SELECT
        o.*,
        m.protocol_id AS manual_protocol_id,
        m.protocol_slug AS manual_protocol_slug,
        m.match_type AS manual_match_type
    FROM
        outcomes_base o
        LEFT JOIN manual_mappings m
        ON o.platform = m.platform
),

-- MATCHING STRATEGY 1: Exact slug match
exact_match AS (
    SELECT
        o.*,
        p.protocol_id,
        p.protocol_slug,
        p.protocol_version,
        FALSE AS is_imputed,
        NULL AS imputed_protocol_id,
        NULL AS imputed_protocol_slug,
        'exact_slug_match' AS match_type
    FROM
        outcomes_base o
        LEFT JOIN defillama_protocols p
        ON p.protocol_slug = o.platform
    WHERE
        o.platform NOT IN (
            SELECT
                platform
            FROM
                manual_mapping_platforms
        )
),

-- MATCHING STRATEGY 2: Match on base name + version
base_version_match AS (
    SELECT
        o.*,
        p.protocol_id,
        p.protocol_slug,
        p.protocol_version,
        FALSE AS is_imputed,
        NULL AS imputed_protocol_id,
        NULL AS imputed_protocol_slug,
        'exact_version_match' AS match_type
    FROM
        outcomes_base o
        LEFT JOIN defillama_protocols p
        ON p.protocol_first_part = o.platform_base_name
        AND p.protocol_version = o.platform_version
        AND o.platform_version IS NOT NULL
    WHERE
        o.platform NOT IN (
            SELECT DISTINCT platform FROM exact_match WHERE protocol_id IS NOT NULL
        )
),

-- MATCHING STRATEGY 3: Match on category + base name
category_match AS (
    SELECT
        o.blockchain,
        o.platform,
        o.platform_version,
        o.platform_base_name,
        o.defillama_category,
        o.action,
        o.last_action_timestamp,
        MIN(p.protocol_id) AS protocol_id,
        MIN(p.protocol_slug) AS protocol_slug,
        MIN(p.protocol_version) AS protocol_version,
        TRUE AS is_imputed,
        ARRAY_AGG(DISTINCT p.protocol_id) WITHIN GROUP (ORDER BY p.protocol_id) AS imputed_protocol_id,
        ARRAY_AGG(DISTINCT p.protocol_slug) WITHIN GROUP (ORDER BY p.protocol_slug) AS imputed_protocol_slug,
        'category_match' AS match_type
    FROM
        outcomes_base o
        LEFT JOIN defillama_protocols p
        ON p.protocol_base_name = o.platform_base_name
        AND p.category = o.defillama_category
    WHERE
        o.platform NOT IN (
            SELECT DISTINCT platform FROM exact_match WHERE protocol_id IS NOT NULL
            UNION 
            SELECT DISTINCT platform FROM base_version_match WHERE protocol_id IS NOT NULL
        )
    GROUP BY 
        o.blockchain, o.platform, o.platform_version, o.platform_base_name, 
        o.defillama_category, o.action, o.last_action_timestamp
),

-- MATCHING STRATEGY 4: Match on first part of name
name_match AS (
    SELECT
        o.blockchain,
        o.platform,
        o.platform_version,
        o.platform_base_name,
        o.defillama_category,
        o.action,
        o.last_action_timestamp,
        MIN(p.protocol_id) AS protocol_id,
        MIN(p.protocol_slug) AS protocol_slug,
        MIN(p.protocol_version) AS protocol_version,
        TRUE AS is_imputed,
        ARRAY_AGG(DISTINCT p.protocol_id) WITHIN GROUP (ORDER BY p.protocol_id) AS imputed_protocol_id,
        ARRAY_AGG(DISTINCT p.protocol_slug) WITHIN GROUP (ORDER BY p.protocol_slug) AS imputed_protocol_slug,
        'name_match' AS match_type
    FROM
        outcomes_base o
        LEFT JOIN defillama_protocols p
        ON SPLIT_PART(o.platform_base_name, '-', 1) = p.protocol_first_part
    WHERE
        o.platform NOT IN (
            SELECT DISTINCT platform FROM exact_match WHERE protocol_id IS NOT NULL
            UNION 
            SELECT DISTINCT platform FROM base_version_match WHERE protocol_id IS NOT NULL
            UNION
            SELECT DISTINCT platform FROM category_match WHERE protocol_id IS NOT NULL
        )
    GROUP BY 
        o.blockchain, o.platform, o.platform_version, o.platform_base_name, 
        o.defillama_category, o.action, o.last_action_timestamp
),

-- MATCHING STRATEGY 5: Use manual/ChatGPT mappings as last resort
manual_match AS (
    SELECT
        o.blockchain,
        o.platform,
        o.defillama_category,
        o.action,
        o.last_action_timestamp,
        m.protocol_id,
        TRUE AS is_imputed,
        ARRAY_CONSTRUCT(m.protocol_id) AS imputed_protocol_id,
        ARRAY_CONSTRUCT(m.protocol_slug) AS imputed_protocol_slug,
        m.match_type
    FROM
        outcomes_base o
        JOIN manual_mappings m
        ON o.platform = m.platform
    WHERE
        o.platform NOT IN (
            SELECT DISTINCT platform FROM exact_match WHERE protocol_id IS NOT NULL
            UNION 
            SELECT DISTINCT platform FROM base_version_match WHERE protocol_id IS NOT NULL
            UNION
            SELECT DISTINCT platform FROM category_match WHERE protocol_id IS NOT NULL
            UNION
            SELECT DISTINCT platform FROM name_match WHERE protocol_id IS NOT NULL
        )
),

-- Handle unmatched platforms
unmatched AS (
    SELECT
        o.*,
        NULL AS protocol_id,
        NULL AS protocol_slug,
        NULL AS protocol_version,
        FALSE AS is_imputed,
        NULL AS imputed_protocol_id,
        NULL AS imputed_protocol_slug,
        'no_match' AS match_type
    FROM
        outcomes_base o
    WHERE
        o.platform NOT IN (
            SELECT DISTINCT platform FROM exact_match WHERE protocol_id IS NOT NULL
            UNION 
            SELECT DISTINCT platform FROM base_version_match WHERE protocol_id IS NOT NULL
            UNION
            SELECT DISTINCT platform FROM category_match WHERE protocol_id IS NOT NULL
            UNION
            SELECT DISTINCT platform FROM name_match WHERE protocol_id IS NOT NULL
            UNION
            SELECT DISTINCT platform FROM manual_match
        )
),

-- Combine all results into a single CTE
combined_results AS (
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
    FROM
        exact_match
    WHERE
        protocol_id IS NOT NULL
    
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
    FROM
        base_version_match
    WHERE
        protocol_id IS NOT NULL
    
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
    FROM
        category_match
    WHERE
        protocol_id IS NOT NULL
    
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
    FROM
        name_match
    WHERE
        protocol_id IS NOT NULL
    
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
    FROM
        manual_match
    
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
    FROM
        unmatched
)

-- Final formatted output with metadata columns
SELECT
    blockchain,
    platform,
    action,
    last_action_timestamp,
    protocol_id,
    is_imputed,
    imputed_protocol_id,
    imputed_protocol_slug,
    match_type,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    {{ dbt_utils.generate_surrogate_key(['blockchain', 'platform', 'action']) }} AS outcome_id,
    '{{ invocation_id }}' AS _invocation_id
FROM
    combined_results
