{{ config(
    materialized = 'incremental',
    unique_key = ['outcome_id'],
    merge_exclude_columns = ['inserted_timestamp'],
    cluster_by = ['modified_timestamp::DATE'],
    tags = ['daily']
) }}

WITH base AS (
    SELECT
        blockchain,
        platform,
        'lp' AS action,
        MAX(creation_time) AS last_action_timestamp
    FROM
        {{ ref('defi__dim_dex_liquidity_pools') }}
    {% if is_incremental() %}
    WHERE creation_time > (
        SELECT MAX(last_action_timestamp)
        FROM {{ this }}
        WHERE action = 'lp'
    )
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
    WHERE block_timestamp > (
        SELECT MAX(last_action_timestamp)
        FROM {{ this }}
        WHERE action = 'swap'
    )
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
    WHERE block_timestamp > (
        SELECT MAX(last_action_timestamp)
        FROM {{ this }}
        WHERE action = 'bridge'
    )
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

-- Manual mappings from seed file (split by match_type)
manual_mappings AS (
    SELECT
        platform,
        protocol_id,
        protocol_slug,
        match_type
    FROM
        {{ ref('silver__outcomes_protocol_mappings') }}
),

-- High priority manual mappings (match_type = 'manual_match')
high_priority_manual_mappings AS (
    SELECT
        platform,
        protocol_id,
        protocol_slug,
        match_type
    FROM
        manual_mappings
    WHERE
        match_type = 'manual_match'
),

-- Normal priority manual mappings (match_type = 'seed_file_chatgpt')
normal_priority_manual_mappings AS (
    SELECT
        platform,
        protocol_id,
        protocol_slug,
        match_type
    FROM
        manual_mappings
    WHERE
        match_type = 'seed_file_chatgpt'
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

-- MATCHING STRATEGY 0: High priority manual mappings (FIRST PRIORITY)
high_priority_manual_match AS (
    SELECT
        o.blockchain,
        o.platform,
        o.defillama_category,
        o.action,
        o.last_action_timestamp,
        TRUE AS is_imputed,
        OBJECT_CONSTRUCT(m.protocol_slug, m.protocol_id::NUMBER) AS defillama_metadata,
        m.match_type
    FROM
        outcomes_base o
        JOIN high_priority_manual_mappings m
        ON o.platform = m.platform
    WHERE
        m.protocol_slug is not null
),

-- MATCHING STRATEGY 1: Exact slug match (SECOND PRIORITY)
exact_match AS (
    SELECT
        o.blockchain,
        o.platform,
        o.defillama_category,
        o.action,
        o.last_action_timestamp,
        FALSE AS is_imputed,
        OBJECT_CONSTRUCT(p.protocol_slug, p.protocol_id::NUMBER) AS defillama_metadata,
        'exact_slug_match' AS match_type
    FROM
        outcomes_base o
        LEFT JOIN defillama_protocols p
        ON p.protocol_slug = o.platform
    WHERE
        p.protocol_slug is not null
        AND o.platform NOT IN (
            SELECT DISTINCT platform FROM high_priority_manual_match
        )
),

-- MATCHING STRATEGY 2: Fuzzy matching methods
all_fuzzy_matches AS (
    SELECT
        o.blockchain,
        o.platform,
        o.defillama_category,
        o.action,
        o.last_action_timestamp,
        TRUE AS is_imputed,
        p.protocol_slug,
        p.protocol_id,
        CASE
            WHEN p.protocol_first_part = o.platform_base_name AND p.protocol_version = o.platform_version AND o.platform_version IS NOT NULL THEN 'exact_version_match'
            WHEN jarowinkler_similarity(p.protocol_slug, o.platform) > 95 THEN 'fuzzy_slug_match'
            WHEN jarowinkler_similarity(p.protocol_base_name, o.platform_base_name) > 95 AND p.protocol_version = o.platform_version AND o.platform_version IS NOT NULL THEN 'fuzzy_version_match'
            WHEN SPLIT_PART(o.platform_base_name, '-', 1) = p.protocol_first_part THEN 'name_match'
            WHEN jarowinkler_similarity(SPLIT_PART(o.platform_base_name, '-', 1), p.protocol_first_part) > 95 THEN 'fuzzy_name_match'
            ELSE NULL
        END AS match_subtype
    FROM
        outcomes_base o
        JOIN defillama_protocols p
        ON (jarowinkler_similarity(p.protocol_slug, o.platform) > 95) OR
           (p.protocol_first_part = o.platform_base_name AND p.protocol_version = o.platform_version AND o.platform_version IS NOT NULL) OR
           (jarowinkler_similarity(p.protocol_base_name, o.platform_base_name) > 95 AND p.protocol_version = o.platform_version AND o.platform_version IS NOT NULL) OR
           (SPLIT_PART(o.platform_base_name, '-', 1) = p.protocol_first_part) OR
           (jarowinkler_similarity(SPLIT_PART(o.platform_base_name, '-', 1), p.protocol_first_part) > 95)
    WHERE
        o.platform NOT IN (
            SELECT DISTINCT platform FROM high_priority_manual_match
            UNION
            SELECT DISTINCT platform FROM exact_match WHERE defillama_metadata IS NOT NULL
        )
        AND p.protocol_slug is not null
),

-- Add match type priority to fuzzy matches
fuzzy_matches_with_priority AS (
    SELECT
        *,
        CASE match_subtype
            WHEN 'exact_version_match' THEN 1   -- Highest priority
            WHEN 'fuzzy_slug_match' THEN 2
            WHEN 'fuzzy_version_match' THEN 3
            WHEN 'name_match' THEN 4
            WHEN 'fuzzy_name_match' THEN 5      -- Lowest priority
            ELSE 99
        END AS match_priority
    FROM
        all_fuzzy_matches
),

-- Aggregate fuzzy matches by platform and select the highest priority match for each platform
fuzzy_matches_consolidated AS (
    SELECT
        blockchain,
        platform,
        defillama_category,
        action,
        last_action_timestamp,
        is_imputed,
        OBJECT_AGG(protocol_slug, protocol_id::NUMBER) AS defillama_metadata,
        match_subtype AS match_type,
        MIN(match_priority) AS min_priority
    FROM
        fuzzy_matches_with_priority
    GROUP BY
        blockchain,
        platform,
        defillama_category,
        action,
        last_action_timestamp,
        is_imputed,
        match_subtype
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY blockchain, platform, defillama_category, action
        ORDER BY min_priority ASC
    ) = 1
),

-- MATCHING STRATEGY 3: ChatGPT seed doc mappings
normal_priority_manual_match AS (
    SELECT
        o.blockchain,
        o.platform,
        o.defillama_category,
        o.action,
        o.last_action_timestamp,
        TRUE AS is_imputed,
        OBJECT_CONSTRUCT(m.protocol_slug, m.protocol_id::NUMBER) AS defillama_metadata,
        m.match_type
    FROM
        outcomes_base o
        JOIN normal_priority_manual_mappings m
        ON o.platform = m.platform
    WHERE
        o.platform NOT IN (
            SELECT DISTINCT platform FROM high_priority_manual_match
            UNION 
            SELECT DISTINCT platform FROM exact_match WHERE defillama_metadata IS NOT NULL
            UNION
            SELECT DISTINCT platform FROM fuzzy_matches_consolidated WHERE defillama_metadata IS NOT NULL
        )
        AND m.protocol_slug is not null
),

-- Handle unmatched platforms
unmatched AS (
    SELECT
        o.blockchain,
        o.platform,
        o.defillama_category,
        o.action,
        o.last_action_timestamp,
        FALSE AS is_imputed,
        NULL AS defillama_metadata,
        'no_match' AS match_type
    FROM
        outcomes_base o
    WHERE
        o.platform NOT IN (
            SELECT DISTINCT platform FROM high_priority_manual_match
            UNION
            SELECT DISTINCT platform FROM exact_match WHERE defillama_metadata IS NOT NULL
            UNION
            SELECT DISTINCT platform FROM fuzzy_matches_consolidated WHERE defillama_metadata IS NOT NULL
            UNION
            SELECT DISTINCT platform FROM normal_priority_manual_match
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
        is_imputed,
        defillama_metadata,
        match_type
    FROM
        high_priority_manual_match
    
    UNION ALL
    
    SELECT
        blockchain,
        platform,
        defillama_category,
        action,
        last_action_timestamp,
        is_imputed,
        defillama_metadata,
        match_type
    FROM
        exact_match
    WHERE
        defillama_metadata IS NOT NULL
    
    UNION ALL
    
    SELECT
        blockchain,
        platform,
        defillama_category,
        action,
        last_action_timestamp,
        is_imputed,
        defillama_metadata,
        match_type
    FROM
        fuzzy_matches_consolidated
    WHERE
        defillama_metadata IS NOT NULL
    
    UNION ALL
    
    SELECT
        blockchain,
        platform,
        defillama_category,
        action,
        last_action_timestamp,
        is_imputed,
        defillama_metadata,
        match_type
    FROM
        normal_priority_manual_match
    
    UNION ALL
    
    SELECT
        blockchain,
        platform,
        defillama_category,
        action,
        last_action_timestamp,
        is_imputed,
        defillama_metadata,
        match_type
    FROM
        unmatched
)

-- Final formatted output with metadata columns
SELECT
    o.blockchain,
    o.platform,
    o.action,
    o.last_action_timestamp,
    s.top_symbols as top_symbols_30D,
    o.is_imputed,
    o.defillama_metadata,
    o.match_type,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    {{ dbt_utils.generate_surrogate_key(['o.blockchain', 'o.platform', 'o.action']) }} AS outcome_id,
    '{{ invocation_id }}' AS _invocation_id
FROM
    combined_results o
LEFT JOIN {{ ref('silver_metrics__dim_outcome_symbols') }} s
    ON o.blockchain = s.blockchain 
    AND o.platform = s.platform
    AND o.action = s.action