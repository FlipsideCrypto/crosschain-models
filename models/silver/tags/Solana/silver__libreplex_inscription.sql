{{ config(
    materialized = 'incremental',
    unique_key = "CONCAT_WS('-', address, start_date, tag_name)",
    incremental_strategy = 'delete+insert',
    tags = ['daily']
) }}

WITH libreplex_txs AS (

    SELECT
        block_timestamp,
        f_inner.value :accounts [2] :: STRING AS non_fungible_mint,
        _INSERTED_TIMESTAMP
    FROM
        {{ source(
            'solana_silver',
            'events'
        ) }}
        fe
        INNER JOIN LATERAL FLATTEN (
            input => inner_instruction :instructions
        ) f_inner
    WHERE
        succeeded
        AND ARRAY_CONTAINS(
            'inscokhJarcjaEs59QbQ7hYjrKz25LEPRfCbP8EmdUp' :: variant,
            instruction :accounts
        )
        AND f_inner.value :programId = 'inscokhJarcjaEs59QbQ7hYjrKz25LEPRfCbP8EmdUp'
        AND SUBSTR(
            utils.udf_base58_to_hex(
                f_inner.value :data
            ),
            1,
            18
        ) IN (
            '0x893cf85eabccc360', -- CreateInscription,
            '0x148e6512785a0975', -- CreateInscriptionV2,
            '0xa82b4de6f0f7a1af' -- CreateInscriptionV3
        )

{% if is_incremental() %}
AND _inserted_timestamp :: DATE >= (
    SELECT
        MAX(
            _inserted_timestamp
        )
    FROM
        {{ this }}
)
{% else %}
    AND _inserted_timestamp :: DATE > '2023-11-12'
{% endif %}
)
SELECT
    'solana' AS blockchain,
    'marqu' AS creator,
    non_fungible_mint AS address,
    'libreplex' AS tag_name,
    'inscription' AS tag_type,
    block_timestamp AS start_date,
    NULL AS end_date,
    SYSDATE() AS tag_created_at,
    _inserted_timestamp AS _inserted_timestamp,
    {{ dbt_utils.generate_surrogate_key(['address','tag_name','start_date']) }} AS libreplex_inscription_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id

FROM
    libreplex_txs

