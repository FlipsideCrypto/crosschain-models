{{ config(
    materialized = 'incremental',
    unique_key = ['onchain_solana_metadata_id'],
    incremental_strategy = 'delete+insert',
    cluster_by = ['_inserted_timestamp::DATE'],
    tags = ['prices']
) }}

WITH base_metadata AS (

    SELECT
        *
    FROM
        {{ source(
            'solana_silver',
            'decoded_metadata'
        ) }}

{% if is_incremental() %}
WHERE _inserted_timestamp >= (
    SELECT
        MAX(_inserted_timestamp)
    FROM
        {{ this }}
)
{% endif %}
),
base_types AS (
    SELECT
        *
    FROM
        {{ source(
            'solana_silver',
            'mint_types'
        ) }}
    WHERE
        mint_type = 'token'

{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        MAX(_inserted_timestamp)
    FROM
        {{ this }}
)
{% endif %}
),
token_metadata AS (
    SELECT
        LOWER(
            A.mint
        ) AS id,
        A.mint AS token_address,
        CASE
            WHEN LENGTH(
                A.token_name
            ) <= 0 THEN NULL
            ELSE A.token_name
        END AS token_name_clean,
        CASE
            WHEN LENGTH(
                A.symbol
            ) <= 0 THEN NULL
            ELSE A.symbol
        END AS symbol_clean,
        A._inserted_timestamp
    FROM
        base_metadata A
        INNER JOIN base_types b
        ON A.mint = b.mint
    WHERE
        (
            symbol_clean IS NOT NULL
            AND token_name_clean IS NOT NULL
        )
)
SELECT
    id,
    token_address,
    token_name_clean as name,
    symbol_clean as symbol,
    _inserted_timestamp,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    {{ dbt_utils.generate_surrogate_key(['token_address']) }} AS onchain_solana_metadata_id,
    '{{ invocation_id }}' AS _invocation_id
FROM
    token_metadata
