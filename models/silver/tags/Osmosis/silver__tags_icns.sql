{{ config(
    materialized = 'incremental',
    unique_key = "CONCAT_WS('-', address, start_date)",
    incremental_strategy = 'delete+insert',
) }}

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
        AND tx_id = '6EA075EE8C10773BE5AA307950ABC5BCFE8D31B68F0A5E33160C4E79247A07FB'
        AND this :contract :: STRING = 'osmo1xk0s8xgktn9x5vwcgtjdxqzadg88fgn33p8u9cnpdxwemvxscvast52cdd'
    
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