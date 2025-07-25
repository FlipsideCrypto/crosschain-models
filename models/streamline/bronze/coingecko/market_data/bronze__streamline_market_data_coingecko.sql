{{ config(
    materialized = 'view',
) }}

WITH meta AS (

    SELECT
        last_modified AS _inserted_timestamp,
        file_name,
        SPLIT_PART(
            file_name,
            '/',
            4
        ) AS _partition_key
    FROM
        TABLE(
            information_schema.external_table_file_registration_history(
                table_name => '{{ source( "bronze_streamline", "market_data_coin_gecko_api") }}'
            )
        )
),
asset_metadata AS (
    SELECT
        partition_key,
        _inserted_date,
        TO_TIMESTAMP(partition_key) AS run_time,
        s.value :"PAGE_NUM" AS page_num,
        DATA,
        _inserted_timestamp
    FROM
        {{ source(
            'bronze_streamline',
            'market_data_coin_gecko_api'
        ) }}
        s
        JOIN meta b
        ON s.metadata$filename = b.file_name
)
SELECT
    page_num AS index,
    f.value AS VALUE,
    'coingecko' AS provider,
    partition_key,
    run_time AS partition_ts,
    _inserted_date,
    _inserted_timestamp
FROM
    asset_metadata A,
    LATERAL FLATTEN(
        input => DATA
    ) f 
    -- streamline 2.0 external table
