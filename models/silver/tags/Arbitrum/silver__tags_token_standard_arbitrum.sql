{{ config(
    materialized = 'incremental',
    unique_key = "CONCAT_WS('-', address, start_date, tag_name)",
    incremental_strategy = 'delete+insert',
    tags = ['daily']
) }}

WITH pre_final as (
    SELECT
        DISTINCT 'arbitrum' AS blockchain,
        'flipside' AS creator,
        contract_address AS address,
        CASE
            WHEN topics [0] = '0xc3d58168c5ae7397731d063d5bbf3d657854427343f4c083240f7aacaa2d0f62' THEN 'erc-1155'
            WHEN (
                topics [0] = '0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef'
                AND topics [3] IS NOT NULL
            ) THEN 'erc-721'
            ELSE 'erc-20'
        END AS tag_name,
        'token standard' AS tag_type,
        MIN(
            block_timestamp :: DATE
        ) AS start_date,
        NULL AS end_date,
        CURRENT_TIMESTAMP AS tag_created_at,
        MIN(modified_timestamp) AS _inserted_timestamp
    FROM
        {{ source(
            'arbitrum_core',
            'ez_decoded_event_logs'
        ) }}
    WHERE
        topics [0] IN (
            '0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef',
            '0xc3d58168c5ae7397731d063d5bbf3d657854427343f4c083240f7aacaa2d0f62'
        )
        AND decoded_log :from = '0x0000000000000000000000000000000000000000'

    {% if is_incremental() %}
    AND modified_timestamp > (
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
        1,
        2,
        3,
        4,
        5
)
SELECT 
    *,
    sysdate() as inserted_timestamp,
    sysdate() as modified_timestamp,
    {{ dbt_utils.generate_surrogate_key(['address','tag_name','start_date']) }} AS tags_token_standard_arbitrum_id,
    '{{ invocation_id }}' as _invocation_id
FROM 
    pre_final
