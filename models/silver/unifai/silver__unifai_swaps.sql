{{ config(
    materialized = 'incremental',
    unique_key = ['unifai_swaps_id'],
    incremental_strategy = 'delete+insert',
    cluster_by = ['created_at_timestamp::DATE','_inserted_timestamp::DATE'],
    tags = ['unifai_daily']
) }}
-- depends_on: {{ ref('bronze__streamline_unifai_swap_txs') }}
-- depends_on: {{ ref('bronze__streamline_FR_unifai_swap_txs') }}

--todo: use dynamic merge predicate?

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
    value:data:createdAt::timestamp_ntz AS created_at_timestamp,
    value:data:txHash::string AS tx_hash,
    value:data:chain::string AS chain,
    value:data:metadata:amount::int AS amount,
    value:data:metadata:inputToken::string AS input_token,
    value:data:metadata:outputToken::string AS output_token,
    value:data:metadata:slippage::int AS slippage,
    value:data:status::string AS status,
    value:data:txType::string AS tx_type,
    value:data:user::string AS user,
    value:data:wallet:address::string AS wallet_address,
    value:data:wallet:chain::string AS wallet_chain,
    partition_key,
    _inserted_timestamp,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    {{ dbt_utils.generate_surrogate_key(['tx_hash']) }} AS unifai_swaps_id,
    '{{ invocation_id }}' AS _invocation_id
FROM 

{% if is_incremental() %}
    {{ ref('bronze__streamline_unifai_swap_txs') }}
WHERE 
    _inserted_timestamp >= '{{ max_inserted_timestamp }}'
{% else %}
    {{ ref('bronze__streamline_FR_unifai_swap_txs') }} a
{% endif %}
QUALIFY
    row_number() OVER (PARTITION BY created_at_timestamp, tx_hash ORDER BY _inserted_timestamp DESC) = 1
