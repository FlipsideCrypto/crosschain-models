{{ config(
    materialized = 'incremental',
    unique_key = 'token_address',
    merge_exclude_columns = ['inserted_timestamp'],
    full_refresh = false,
    tags = ['daily']
) }}

WITH tokes AS (

    SELECT
        DISTINCT token_address
    FROM
        {{ ref('silver__token_asset_metadata_coingecko') }}
    WHERE
        platform IN (
            'ton',
            'toncoin',
            'the open network'
        )
        AND token_address LIKE 'E%'
    UNION ALL
    SELECT
        DISTINCT token_address
    FROM
        {{ ref('silver__token_asset_metadata_coinmarketcap') }}
    WHERE
        platform IN (
            'ton',
            'toncoin',
            'the open network'
        )
        AND token_address LIKE 'E%'

{% if is_incremental() %}
EXCEPT
SELECT
    token_address
FROM
    {{ this }}
{% endif %}
)
SELECT
    to_char(TO_TIMESTAMP_NTZ(SYSDATE()), 'YYYY_MM_DD') AS partition_key,
    token_address,
    ton.live.udf_api(
        'GET',
        '{Service}/{Authentication}/detectAddress?address=' || token_address,
        OBJECT_CONSTRUCT(
            'Content-Type',
            'application/json'
        ),
        OBJECT_CONSTRUCT(),
        'Vault/prod/ton/quicknode/mainnet'
    ) :data :result :raw_form :: STRING AS token_address_raw,
    {{ dbt_utils.generate_surrogate_key(['token_address']) }} AS tokens_ton_lookup_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    tokes qualify(ROW_NUMBER() over (PARTITION BY token_address
ORDER BY
    token_address_raw DESC) = 1)
