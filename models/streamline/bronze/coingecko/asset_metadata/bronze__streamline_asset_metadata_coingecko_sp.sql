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
            information_schema.external_table_files(
                table_name => '{{ source( "bronze_streamline", "asset_metadata_coin_gecko_api") }}'
            )
        )
),
asset_metadata AS (
    SELECT
        _inserted_date AS partition_key,
        _inserted_date,
        TO_TIMESTAMP(partition_key) AS run_time,
        VALUE,
        _inserted_timestamp
    FROM
        {{ source(
            'bronze_streamline',
            'asset_metadata_coin_gecko_api'
        ) }}
        s
        JOIN meta b
        ON s.metadata$filename = b.file_name
)
SELECT
    VALUE,
    'coingecko' AS provider,
    VALUE :id :: STRING AS id,
    VALUE :symbol :: STRING AS symbol,
    VALUE :name :: STRING AS NAME,
    VALUE :platforms AS platforms,
    run_time,
    _inserted_date,
    _inserted_timestamp
FROM
    asset_metadata
