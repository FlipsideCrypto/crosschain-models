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
                table_name => '{{ source( "bronze_streamline", "asset_metadata_coin_gecko_api_v2") }}'
            )
        )
),
asset_metadata AS (
    SELECT
        partition_key,
        _inserted_date,
        TO_TIMESTAMP(partition_key) AS run_time,
        DATA,
        _inserted_timestamp
    FROM
        {{ source(
            'bronze_streamline',
            'asset_metadata_coin_gecko_api_v2'
        ) }}
        s
        JOIN meta b
        ON s.metadata$filename = b.file_name
)
SELECT
    f.value AS VALUE,
    'coingecko' AS provider,
    f.value :id :: STRING AS id,
    f.value :symbol :: STRING AS symbol,
    f.value :name :: STRING AS NAME,
    run_time,
    _inserted_date,
    _inserted_timestamp
FROM
    asset_metadata A,
    LATERAL FLATTEN(
        input => DATA
    ) f 
    -- columns parsed out to match legacy bronze model `bronze__asset_metadata_coin_gecko`
