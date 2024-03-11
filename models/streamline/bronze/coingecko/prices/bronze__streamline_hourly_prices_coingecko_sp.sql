{{ config(
    materialized = 'view',
) }}

WITH meta AS (

    SELECT
        last_modified AS _inserted_timestamp,
        file_name,
        TO_DATE(
            TO_TIMESTAMP(
                SPLIT_PART(
                    SPLIT_PART(
                        file_name,
                        '/',
                        5
                    ),
                    '_',
                    1
                )
            )
        ) :: STRING AS _partition_key
    FROM
        TABLE(
            information_schema.external_table_files(
                table_name => '{{ source( "bronze_streamline", "asset_ohlc_coin_gecko_api") }}'
            )
        )
)
SELECT
    _inserted_date AS _runtime_date,
    run_time,
    id,
    VALUE :currency :: STRING AS currency,
    DATA,
    _inserted_timestamp
FROM
    {{ source(
        'bronze_streamline',
        'asset_ohlc_coin_gecko_api'
    ) }}
    s
    JOIN meta b
    ON s.metadata$filename = b.file_name
    AND s._inserted_date = b._partition_key 
    -- endpoint: ohlc
    -- streamline 1.0 external table
