{{ config(
    materialized = 'incremental',
    unique_key = ['bridge_comparison_id'],
    incremental_strategy = 'delete+insert',
    cluster_by = ['inserted_timestamp::DATE'],
    tags = ['ai_metrics']
) }}

WITH defillama AS (

    SELECT
        LOWER(
            REGEXP_SUBSTR(
                REGEXP_REPLACE(
                    IFF(
                        bridge = 'debridgedln',
                        'dlndebridge',
                        bridge
                    ),
                    '_',
                    ''
                ),
                '^[^: -]+'
            )
        ) AS platform_name,
        chain AS blockchain,
        SUM(deposit_usd) AS defillama_volume
    FROM
        {{ source(
            'external_defillama',
            'fact_bridge_volume_by_chain'
        ) }}
    WHERE
        DATE BETWEEN CURRENT_DATE() - 8
        AND CURRENT_DATE() - 1
    GROUP BY
        ALL
),
crosschain AS (
    SELECT
        LOWER(
            REGEXP_SUBSTR(REGEXP_REPLACE(platform, '_', ''), '^[^: -]+')
        ) AS platform_name,
        blockchain,
        SUM(amount_usd) AS crosschain_volume
    FROM
        {{ ref('defi__ez_bridge_activity') }}
    WHERE
        block_timestamp :: DATE BETWEEN CURRENT_DATE() - 8
        AND CURRENT_DATE() - 1
        AND direction = 'outbound'
    GROUP BY
        ALL
),
crosschain_chains AS (
    SELECT
        DISTINCT blockchain
    FROM
        crosschain
),
defillama_total_volume AS (
    SELECT
        blockchain,
        SUM(defillama_volume) AS total_defillama_volume
    FROM
        defillama
    GROUP BY
        ALL
),
comparison AS (
    SELECT
        COALESCE(
            d.blockchain,
            C.blockchain
        ) AS blockchain,
        COALESCE(
            d.platform_name,
            C.platform_name
        ) AS platform_name,
        defillama_volume,
        ROUND(
            defillama_volume / total_defillama_volume * 100,
            2
        ) AS defillama_volume_percent,
        crosschain_volume
    FROM
        defillama d full
        OUTER JOIN crosschain C USING (
            blockchain,
            platform_name
        )
        JOIN defillama_total_volume USING (blockchain)
        JOIN crosschain_chains USING (blockchain)
    ORDER BY
        defillama_volume DESC nulls last
),
fuzzy_match AS (
    SELECT
        blockchain,
        d.platform_name AS platform_name
    FROM
        crosschain C
        JOIN defillama d USING (blockchain)
    WHERE
        jarowinkler_similarity (
            C.platform_name,
            d.platform_name
        ) >= 88
)
SELECT
    blockchain,
    platform_name,
    defillama_volume,
    defillama_volume_percent,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    {{ dbt_utils.generate_surrogate_key(['inserted_timestamp', 'blockchain', 'platform_name']) }} AS bridge_comparison_id,
    '{{ invocation_id }}' AS _invocation_id
FROM
    comparison C
    LEFT JOIN fuzzy_match f USING (
        blockchain,
        platform_name
    )
WHERE
    crosschain_volume IS NULL
    AND defillama_volume_percent IS NOT NULL
    AND f.platform_name IS NULL qualify ROW_NUMBER() over (
        PARTITION BY blockchain
        ORDER BY
            defillama_volume DESC nulls last
    ) <= 5
