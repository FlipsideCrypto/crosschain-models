{{ config(
    materialized = 'incremental',
    unique_key = ['token_asset_metadata_priority_id'],
    incremental_strategy = 'delete+insert',
    post_hook = "ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION on equality(token_address, blockchain)",
    tags = ['prices']
) }}

WITH all_providers AS (

    SELECT
        id,
        token_address,
        NAME,
        symbol,
        blockchain,
        blockchain_name,
        blockchain_id,
        provider,
        CASE
            WHEN provider = 'coingecko' AND source <> 'cg enhanced' THEN 1
            WHEN provider = 'coinmarketcap'  AND source <> 'cmc enhanced' THEN 2
            WHEN provider = 'coingecko' THEN 3
            WHEN provider = 'coinmarketcap' THEN 4
            WHEN provider = 'osmosis-onchain' THEN 5
            WHEN provider = 'solana-onchain' THEN 6
            WHEN provider = 'solscan' THEN 7
            ELSE 99
        END AS priority,
        source,
        is_deprecated,
        _inserted_timestamp
    FROM
        {{ ref('silver__token_asset_metadata_all_providers') }}

{% if is_incremental() %}
WHERE
    _inserted_timestamp >= (
        SELECT
            MAX(_inserted_timestamp) - INTERVAL '4 hours'
        FROM
            {{ this }}
    )
{% endif %}
)
SELECT
    id,
    token_address,
    NAME,
    symbol,
    blockchain,
    blockchain_name,
    blockchain_id,
    provider,
    priority,
    source,
    is_deprecated,
    _inserted_timestamp,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    {{ dbt_utils.generate_surrogate_key(['LOWER(token_address)','blockchain']) }} AS token_asset_metadata_priority_id,
    '{{ invocation_id }}' AS _invocation_id
FROM
    all_providers qualify(ROW_NUMBER() over (PARTITION BY LOWER(token_address), blockchain
ORDER BY
    _inserted_timestamp DESC, priority ASC, id ASC, blockchain_id ASC nulls last)) = 1
-- select the last inserted record (latest supported provider), then by priority etc.