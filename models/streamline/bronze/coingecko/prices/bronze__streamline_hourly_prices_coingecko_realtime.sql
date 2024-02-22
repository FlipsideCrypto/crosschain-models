{{ config(
    materialized = 'view',
) }}

SELECT
    *,
    TO_TIMESTAMP_NTZ(
        SUBSTR(SPLIT_PART(metadata$filename, '/', 5), 1, 10) :: NUMBER,
        0
    ) AS _inserted_timestamp
FROM
    {{ source(
        'bronze_streamline',
        'asset_ohlc_coin_gecko_api_v2'
    ) }}
    -- endpoint: ohlc
    -- streamline 2.0 external table
    -- to serve as destination for `realtime` prices
