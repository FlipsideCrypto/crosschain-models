{{ config(
    materialized = 'incremental',
    unique_key = ['token_address', 'platform'],
    incremental_strategy = 'delete+insert',
    cluster_by = ['_inserted_timestamp::DATE']
) }}

WITH base AS (

    SELECT
        f.value AS VALUE,
        'coingecko' AS provider,
        _inserted_date,
        f.value :id :: STRING AS id,
        f.value :symbol :: STRING AS symbol,
        f.value :name :: STRING AS NAME,
        _inserted_timestamp
    FROM
        {{ ref('bronze__streamline_asset_metadata_coingecko') }} A,
        LATERAL FLATTEN(
            input => DATA
        ) f
)
SELECT
    id,
    p.value :: STRING AS token_address,
    NAME,
    symbol,
    p.key :: STRING AS platform,
    _inserted_timestamp,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    {{ dbt_utils.generate_surrogate_key(['token_address','platform']) }} AS asset_metadata_coin_gecko_id,
    '{{ invocation_id }}' AS _invocation_id
FROM
    base b,
    LATERAL FLATTEN(input => VALUE :platforms) p qualify(ROW_NUMBER() over (PARTITION BY token_address, platform
ORDER BY
    _inserted_timestamp DESC)) = 1
-- need logic / macro to handle assets no longer in source (e.g. deprecated by coingecko)