{{ config(
    materialized = 'incremental',
    unique_key = ['chain_overview_id'],
    incremental_strategy = 'delete+insert',
    cluster_by = ['inserted_timestamp::DATE'],
    tags = ['ai_metrics']
) }}

WITH defillama_dex AS (

    SELECT
        chain AS blockchain,
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
)
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
    ) AS diff_defillama_percent,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    {{ dbt_utils.generate_surrogate_key(['inserted_timestamp', 'blockchain', 'category']) }} AS chain_overview_id,
    '{{ invocation_id }}' AS _invocation_id
FROM
    defillama_dex d full
    INNER JOIN crosschain_dex C USING (
        blockchain
    )
