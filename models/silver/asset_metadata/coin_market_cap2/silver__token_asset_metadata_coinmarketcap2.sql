{{ config(
    materialized = 'incremental',
    unique_key = ['token_address', 'platform'],
    incremental_strategy = 'delete+insert',
    cluster_by = ['_inserted_timestamp::DATE']
) }}

SELECT
    id,
    p.this :token_address :: STRING AS token_address,
    NAME,
    symbol,
    p.this :name :: STRING AS platform,
    p.this :id AS platform_id,
    p.this :slug :: STRING AS platform_slug,
    p.this :symbol :: STRING AS platform_symbol,
    _inserted_timestamp,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    {{ dbt_utils.generate_surrogate_key(['token_address','platform']) }} AS asset_metadata_coin_market_cap_id,
    '{{ invocation_id }}' AS _invocation_id
FROM
    {{ ref('bronze__streamline_asset_metadata_coinmarketcap') }} A,
    LATERAL FLATTEN(
        input => VALUE :platform
    ) p

{% if is_incremental() %}
WHERE
    _inserted_timestamp > (
        SELECT
            MAX(_inserted_timestamp)
        FROM
            {{ this }}
    )
{% endif %}

qualify(ROW_NUMBER() over (PARTITION BY token_address, platform
ORDER BY
    _inserted_timestamp DESC)) = 1 
    -- specifically built for tokens with token_address (not native/gas tokens)