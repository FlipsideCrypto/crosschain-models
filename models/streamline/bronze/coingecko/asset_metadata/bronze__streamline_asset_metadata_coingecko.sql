{{ config(
    materialized = 'view',
) }}

SELECT
    f.value AS VALUE,
    'coingecko' AS provider,
    _inserted_date,
    f.value :id :: STRING AS id,
    f.value :symbol :: STRING AS symbol,
    f.value :name :: STRING AS NAME,
    TO_TIMESTAMP(partition_key) AS _inserted_timestamp
FROM
    {{ source(
        'bronze_streamline',
        'asset_metadata_coin_gecko_api_v2'
    ) }} A,
    LATERAL FLATTEN(
        input => DATA
    ) f
-- columns parsed out to match legacy bronze model `bronze__asset_metadata_coin_gecko`