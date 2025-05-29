{{ config(
    materialized = 'incremental',
    unique_key = ['blockchain', 'platform', 'action'],
    merge_exclude_columns = ['inserted_timestamp'],
    tags = ['daily']
) }}

{% if execute %}
{% if is_incremental() %}
{% set max_mod_query %}
SELECT MAX(modified_timestamp) :: DATE AS modified_timestamp
FROM {{ this }}
{% endset %}
{% set max_mod = run_query(max_mod_query)[0][0] %}
{% endif %}
{% endif %}

WITH symbols_by_lp AS (
    SELECT 
        l.blockchain,
        l.platform,
        'lp' as action,
        l.pool_name,
        l.pool_address,
        SUM(COALESCE(s.amount_in_usd, 0)) as volume_usd,
        ROW_NUMBER() OVER (PARTITION BY l.blockchain, l.platform ORDER BY SUM(COALESCE(s.amount_in_usd, 0)) DESC) as rn
    FROM {{ ref('defi__dim_dex_liquidity_pools') }} l
    LEFT JOIN {{ ref('defi__ez_dex_swaps') }} s
        ON l.blockchain = s.blockchain 
        AND l.pool_address = s.contract_address
    {% if is_incremental() %}
    WHERE l.modified_timestamp :: DATE >= '{{ max_mod }}'
    {% endif %}
    GROUP BY l.blockchain, l.platform, l.pool_name, l.pool_address
    QUALIFY rn <= 5
),

lp_symbols_agg AS (
    SELECT 
        blockchain,
        platform,
        action,
        OBJECT_AGG(
            'rank_' || rn::string, 
            OBJECT_CONSTRUCT(
                'pool_name', pool_name,
                'pool_address', pool_address,
                'volume_usd', volume_usd
            )
        ) as top_symbols
    FROM symbols_by_lp
    GROUP BY blockchain, platform, action
),

symbols_by_swap AS (
    SELECT 
        blockchain,
        platform,
        'swap' as action,
        symbol_in,
        symbol_out,
        SUM(COALESCE(amount_in_usd, 0)) as volume_usd,
        ROW_NUMBER() OVER (PARTITION BY blockchain, platform ORDER BY SUM(COALESCE(amount_in_usd, 0)) DESC) as rn
    FROM {{ ref('defi__ez_dex_swaps') }}
    {% if is_incremental() %}
    WHERE modified_timestamp :: DATE >= '{{ max_mod }}'
    {% endif %}
    GROUP BY blockchain, platform, symbol_in, symbol_out
    QUALIFY rn <= 5
),

swap_symbols_agg AS (
    SELECT 
        blockchain,
        platform,
        action,
        OBJECT_AGG(
            'rank_' || rn::string, 
            OBJECT_CONSTRUCT(
                'symbols', ARRAY_CONSTRUCT(symbol_in, symbol_out),
                'volume_usd', volume_usd
            )
        ) as top_symbols
    FROM symbols_by_swap
    GROUP BY blockchain, platform, action
),

symbols_by_bridge AS (
    SELECT 
        b.blockchain,
        b.platform,
        'bridge' as action,
        b.direction,
        t.symbol,
        COUNT(*) as symbol_count,
        ROW_NUMBER() OVER (PARTITION BY b.blockchain, b.platform, b.direction ORDER BY COUNT(*) DESC) as rn
    FROM {{ ref('defi__fact_bridge_activity') }} b
    LEFT JOIN {{ ref('silver__tokens') }} t
        ON b.token_address = t.address
        AND b.blockchain = t.blockchain
    {% if is_incremental() %}
    WHERE b.modified_timestamp :: DATE >= '{{ max_mod }}'
    {% endif %}
    GROUP BY b.blockchain, b.platform, b.direction, t.symbol
    QUALIFY rn <= 5
),

bridge_symbols_agg AS (
    WITH inbound_agg AS (
        SELECT 
            blockchain,
            platform,
            action,
            OBJECT_AGG(
                'rank_' || rn::string, 
                OBJECT_CONSTRUCT(
                    'symbol', symbol,
                    'count', symbol_count
                )
            ) as inbound_symbols
        FROM symbols_by_bridge
        WHERE direction = 'inbound'
        GROUP BY blockchain, platform, action
    ),
    outbound_agg AS (
        SELECT 
            blockchain,
            platform,
            action,
            OBJECT_AGG(
                'rank_' || rn::string, 
                OBJECT_CONSTRUCT(
                    'symbol', symbol,
                    'count', symbol_count
                )
            ) as outbound_symbols
        FROM symbols_by_bridge
        WHERE direction = 'outbound'
        GROUP BY blockchain, platform, action
    )
    SELECT 
        COALESCE(i.blockchain, o.blockchain) as blockchain,
        COALESCE(i.platform, o.platform) as platform,
        COALESCE(i.action, o.action) as action,
        OBJECT_CONSTRUCT(
            'inbound', i.inbound_symbols,
            'outbound', o.outbound_symbols
        ) as top_symbols
    FROM inbound_agg i
    FULL OUTER JOIN outbound_agg o
        ON i.blockchain = o.blockchain 
        AND i.platform = o.platform
        AND i.action = o.action
),

combined_symbols AS (
    SELECT * FROM lp_symbols_agg
    UNION ALL 
    SELECT * FROM swap_symbols_agg
    UNION ALL
    SELECT * FROM bridge_symbols_agg
)

SELECT
    blockchain,
    platform,
    action,
    top_symbols,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    {{ dbt_utils.generate_surrogate_key(['blockchain', 'platform', 'action']) }} AS outcome_symbols_id,
    '{{ invocation_id }}' AS _invocation_id
FROM combined_symbols 