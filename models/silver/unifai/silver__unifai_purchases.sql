{{ config(
    materialized = 'incremental',
    unique_key = ['unifai_purchases_id'],
    incremental_strategy = 'delete+insert',
    cluster_by = ['created_at_timestamp::DATE','_inserted_timestamp::DATE'],
    tags = ['unifai_daily']
) }}
-- depends_on: {{ ref('bronze__streamline_unifai_purchase_txs') }}
-- depends_on: {{ ref('bronze__streamline_FR_unifai_purchase_txs') }}

{% if execute %}
    {% if is_incremental() %}
    {% set max_inserted_query %}
    SELECT
        MAX(_inserted_timestamp) AS _inserted_timestamp
    FROM
        {{ this }}
    {% endset %}
    {% set max_inserted_timestamp = run_query(max_inserted_query)[0][0] %}
    {% endif %}
{% endif %}

SELECT
    data:createdAt::timestamp_ntz AS created_at_timestamp,
    data:txHash::string as tx_hash,
    data:chain::string as chain,
    data:status::string as status,
    data:fromAddress::string as from_address,
    data:toAddress::string as to_address,
    data:tokenAmount::FLOAT as token_amount,
    data:tokenSymbol::string as token_symbol,
    data:tokenValue::FLOAT as token_value,
    data:user::string as user,
    partition_key,
    _inserted_timestamp,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    {{ dbt_utils.generate_surrogate_key(['tx_hash', 'chain']) }} AS unifai_purchases_id,
    '{{ invocation_id }}' AS _invocation_id
FROM 

{% if is_incremental() %}
    {{ ref('bronze__streamline_unifai_purchase_txs') }}
WHERE 
    _inserted_timestamp >= '{{ max_inserted_timestamp }}'
{% else %}
    {{ ref('bronze__streamline_FR_unifai_purchase_txs') }} a
{% endif %}
QUALIFY
    row_number() OVER (PARTITION BY created_at_timestamp, tx_hash, chain ORDER BY _inserted_timestamp DESC) = 1
