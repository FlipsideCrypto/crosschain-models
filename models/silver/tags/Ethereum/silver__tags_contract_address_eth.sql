{{ config(
    materialized = 'incremental',
    unique_key = "address",
    incremental_strategy = 'delete+insert',
    tags = ['daily']
) }}

WITH pre_final AS (
    SELECT
        DISTINCT 'ethereum' AS blockchain,
        'flipside' AS creator,
        to_address :: STRING AS address,
        'contract address' AS tag_name,
        'contract' AS tag_type,
        block_number,
        DATE_TRUNC(
            'day',
            block_timestamp
        ) AS start_date,
        NULL AS end_date,
        _inserted_timestamp,
        CURRENT_TIMESTAMP AS tag_created_at
    FROM
        {{ source(
            'ethereum_silver',
            'traces'
        ) }}
    WHERE
        TYPE in ('CREATE', 'CREATE2')
        AND tx_status = 'SUCCESS'
        AND to_address IS NOT NULL

    {% if is_incremental() %}
    AND _inserted_timestamp > (
        SELECT
            MAX(_inserted_timestamp)
        FROM
            {{ this }}
    )
    {% endif %}

    qualify(ROW_NUMBER() over(PARTITION BY address
    ORDER BY
        block_number DESC)) = 1
)
SELECT 
    *,
    sysdate() as inserted_timestamp,
    sysdate() as modified_timestamp,
    {{ dbt_utils.generate_surrogate_key(['address','tag_name','start_date']) }} AS tags_contract_address_eth_id,
    '{{ invocation_id }}' as _invocation_id
FROM 
    pre_final
