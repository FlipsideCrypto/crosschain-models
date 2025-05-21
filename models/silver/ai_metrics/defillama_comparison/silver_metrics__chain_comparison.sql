{{ config(
    materialized = 'incremental',
    unique_key = ['chain_comparison_id'],
    incremental_strategy = 'delete+insert',
    cluster_by = ['inserted_timestamp::DATE'],
    tags = ['ai_metrics']
) }}

WITH defillama_dex AS (

    SELECT
        IFF(
            chain = 'avax',
            'avalanche',
            chain
        ) AS blockchain,
        SUM(volume) AS defillama_volume
    FROM
        {{ source(
            'external_defillama',
            'fact_dex_volume'
        ) }}
    WHERE
        DATE BETWEEN CURRENT_DATE() - 8
        AND CURRENT_DATE() - 1
    GROUP BY
        ALL
),
crosschain_dex AS (
    SELECT
        blockchain,
        SUM(COALESCE(amount_in_usd, amount_out_usd)) AS crosschain_volume
    FROM
        {{ ref('defi__ez_dex_swaps') }}
    WHERE
        block_timestamp :: DATE BETWEEN CURRENT_DATE() - 8
        AND CURRENT_DATE() - 1
    GROUP BY
        ALL
),
defillama_bridge AS (
    SELECT
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
crosschain_bridge AS (
    SELECT
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
dex_comparison AS (
    SELECT
        blockchain,
        'dex' AS category,
        defillama_volume,
        crosschain_volume,
        ROUND(
            (
                defillama_volume - crosschain_volume
            ) / defillama_volume * 100,
            2
        ) AS diff_defillama_percent
    FROM
        defillama_dex d
        INNER JOIN crosschain_dex C USING (
            blockchain
        )
),
bridge_comparison AS (
    SELECT
        blockchain,
        'bridge' AS category,
        defillama_volume,
        crosschain_volume,
        ROUND(
            (
                defillama_volume - crosschain_volume
            ) / defillama_volume * 100,
            2
        ) AS diff_defillama_percent
    FROM
        defillama_bridge d
        INNER JOIN crosschain_bridge C USING (blockchain)
),
combined AS (
    SELECT
        blockchain,
        category,
        defillama_volume,
        crosschain_volume,
        diff_defillama_percent
    FROM
        dex_comparison
    UNION ALL
    SELECT
        blockchain,
        category,
        defillama_volume,
        crosschain_volume,
        diff_defillama_percent
    FROM
        bridge_comparison
)
SELECT
    blockchain,
    category,
    defillama_volume,
    crosschain_volume,
    diff_defillama_percent,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    {{ dbt_utils.generate_surrogate_key(['inserted_timestamp', 'blockchain', 'category']) }} AS chain_comparison_id,
    '{{ invocation_id }}' AS _invocation_id
FROM
    combined
