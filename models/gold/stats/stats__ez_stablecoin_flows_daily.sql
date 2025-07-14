{{ config(
    materialized = 'view',
    tags = ['metrics_daily']
) }}

SELECT
    A.blockchain,
    A.block_date,
    SUM(stablecoin_transfer_volume_usd) AS stablecoin_transfer_volume_usd,
    {{ dbt_utils.generate_surrogate_key(['a.blockchain','a.block_date']) }} AS ez_stablecoin_flows_daily_id,
    MAX(inserted_timestamp) AS inserted_timestamp,
    MAX(modified_timestamp) AS modified_timestamp
FROM
    {{ ref('stats__ez_stablecoin_token_flows_daily') }} A
GROUP BY
    A.blockchain,
    A.block_date
