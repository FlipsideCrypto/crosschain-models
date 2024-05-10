{{ config(
    materialized = 'incremental',
    unique_key = "CONCAT_WS('-', address, start_date)",
    incremental_strategy = 'delete+insert',
    tags = ['daily']
) }}

WITH pre_final AS (
    SELECT
        DISTINCT 'optimism' AS blockchain,
        'flipside' AS creator,
        decoded_flat: tokenContract :: STRING AS address,
        'erc-6551 owner' AS tag_name,
        'token standard' AS tag_type,
        MIN(
            block_timestamp :: DATE
        ) AS start_date,
        NULL AS end_date,
        CURRENT_TIMESTAMP AS tag_created_at,
        MIN(_inserted_timestamp) AS _inserted_timestamp
    FROM
        {{ source(
            'optimism_silver',
            'decoded_logs'
        ) }}
    WHERE
        contract_address = LOWER('0x02101dfB77FDE026414827Fdc604ddAF224F0921')
        AND event_name = 'AccountCreated'
        AND tx_status = 'SUCCESS'

    {% if is_incremental() %}
    AND _INSERTED_TIMESTAMP > (
        SELECT
            MAX(_INSERTED_TIMESTAMP)
        FROM
            {{ this }}
    )
    AND contract_address NOT IN (
        SELECT
            DISTINCT address
        FROM
            {{ this }}
    )
    {% endif %}
    GROUP BY
        3
)
SELECT 
    *,
    sysdate() as inserted_timestamp,
    sysdate() as modified_timestamp,
    {{ dbt_utils.generate_surrogate_key(['address','tag_name','start_date']) }} AS tags_token_standard_erc6551_owner_optimism_id,
    '{{ invocation_id }}' as _invocation_id  
FROM 
    pre_final