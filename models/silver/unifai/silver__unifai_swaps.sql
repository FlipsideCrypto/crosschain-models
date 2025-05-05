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
    t.value:createdAt::timestamp_ntz AS created_at_timestamp,
    t.value:txHash::string AS tx_hash,
    t.value:chain::string AS chain,
    t.value:metadata:amount::int AS amount,
    t.value:metadata:inputToken::string AS input_token,
    t.value:metadata:outputToken::string AS output_token,
    t.value:metadata:slippage::int AS slippage,
    t.value:status::string AS status,
    t.value:txType::string AS tx_type,
    t.value:user::string AS user,
    t.value:wallet:address::string AS wallet_address,
    t.value:wallet:chain::string AS wallet_chain,
    partition_key,
    _inserted_timestamp,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    {{ dbt_utils.generate_surrogate_key(['tx_hash']) }} AS unifai_swaps_id,
    '{{ invocation_id }}' AS _invocation_id
FROM 

{% if is_incremental() %}
    {{ ref('bronze__streamline_unifai_swap_txs') }} a,
{% else %}
    {{ ref('bronze__streamline_FR_unifai_swap_txs') }} a,
{% endif %}
    TABLE(FLATTEN(data:data)) t
{% if is_incremental() %}
WHERE 
    _inserted_timestamp >= '{{ max_inserted_timestamp }}'
{% endif %}
QUALIFY
    row_number() OVER (PARTITION BY created_at_timestamp, tx_hash ORDER BY _inserted_timestamp DESC) = 1
