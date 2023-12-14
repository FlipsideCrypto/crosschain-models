{{ config(
    materialized = 'incremental',
    unique_key = "CONCAT_WS('-', address, start_date, tag_name)",
    incremental_strategy = 'delete+insert',
) }}

WITH pre_final AS (
    SELECT 
        this :msg :set_record :bech32_prefix :: STRING AS blockchain, 
        this :msg :set_record :adr36_info :signer_bech32_address :: STRING as address, 
        'flipside' AS creator,
        concat(this :msg :set_record :name :: STRING, '.', this :msg :set_record :bech32_prefix :: STRING) AS tag_name, 
        'ICNS' as tag_type, 
        block_timestamp :: date as start_date, 
        null as end_date, 
        _inserted_timestamp as tag_created_at,
        _inserted_timestamp
    FROM {{ source('osmosis_silver', 'transactions') }}, 
    LATERAL FLATTEN (
        input => tx_body :messages, 
        recursive => TRUE
    ) b
    WHERE 
        key = '@type'
        AND value :: STRING = '/cosmwasm.wasm.v1.MsgExecuteContract'
        AND this :contract :: STRING = 'osmo1xk0s8xgktn9x5vwcgtjdxqzadg88fgn33p8u9cnpdxwemvxscvast52cdd'
        AND this :msg :set_record :adr36_info :signer_bech32_address :: STRING IS NOT NULL
        
        {% if is_incremental() %}
    AND _inserted_timestamp >= (
        SELECT
            MAX(
                _inserted_timestamp
            )
        FROM
            {{ this }}
    )
    {% endif %}

    qualify(ROW_NUMBER() over(PARTITION BY address, start_date, tag_name
    ORDER BY
        _inserted_timestamp DESC)) = 1 
)
SELECT 
    *,
    sysdate() as inserted_timestamp,
    sysdate() as modified_timestamp,
    {{ dbt_utils.generate_surrogate_key(['address','tag_name','start_date']) }} AS tags_icns_id,
    '{{ invocation_id }}' as _invocation_id  
FROM 
    pre_final
