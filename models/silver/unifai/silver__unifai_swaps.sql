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
    data:createdAt::timestamp_ntz AS created_at_timestamp,
    data:txHash::string AS tx_hash,
    data:chain::string AS chain,
    data:metadata:amount::float AS amount,
    data:metadata:inputToken::string AS input_token,
    data:metadata:outputToken::string AS output_token,
    data:metadata:slippage::float AS slippage,
    data:status::string AS status,
    data:txType::string AS tx_type,
    data:user::string AS user,
    data:wallet:address::string AS wallet_address,
    data:wallet:chain::string AS wallet_chain,
    partition_key,
    _inserted_timestamp,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    {{ dbt_utils.generate_surrogate_key(['tx_hash', 'chain']) }} AS unifai_swaps_id,
    '{{ invocation_id }}' AS _invocation_id
FROM 

{% if is_incremental() %}
    {{ ref('bronze__streamline_unifai_swap_txs') }}
WHERE 
    _inserted_timestamp >= '{{ max_inserted_timestamp }}'
    AND created_at_timestamp IS NOT NULL
{% else %}
    {{ ref('bronze__streamline_FR_unifai_swap_txs') }}
WHERE 
    created_at_timestamp IS NOT NULL
{% endif %}
QUALIFY
    row_number() OVER (PARTITION BY created_at_timestamp, tx_hash, chain ORDER BY _inserted_timestamp DESC) = 1
