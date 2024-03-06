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
                table_name => '{{ source( "bronze_streamline", "asset_metadata_coin_market_cap_api_v2") }}'
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
            'asset_metadata_coin_market_cap_api_v2'
        ) }}
        s
        JOIN meta b
        ON s.metadata$filename = b.file_name
)
SELECT
    f.value AS VALUE,
    'coinmarketcap' AS provider,
    f.value :id :: STRING AS id,
    f.value :symbol :: STRING AS symbol,
    f.value :name :: STRING AS NAME,
    f.value :first_historical_data :: TIMESTAMP AS first_historical_data,
    f.value :last_historical_data :: TIMESTAMP AS last_historical_data,
    f.value :is_active :: INT AS is_active,
    f.value :platform AS platform,
    f.value :rank :: INT AS RANK,
    f.value :slug :: STRING AS slug,
    run_time,
    _inserted_date,
    _inserted_timestamp
FROM
    asset_metadata A,
    LATERAL FLATTEN(
        input => data :data
    ) f -- columns parsed out to match legacy bronze model `bronze__asset_metadata_coin_market_cap`
