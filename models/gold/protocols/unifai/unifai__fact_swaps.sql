{{ config(
    materialized = 'incremental',
    unique_key = ['fact_swaps_id'],
    incremental_strategy = 'delete+insert',
    post_hook = "ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION ON EQUALITY(tx_hash, input_token, output_token, wallet_address)",
    cluster_by = ['created_at_timestamp::DATE','chain'],
    meta ={ 'database_tags':{ 'table':{ 'PROTOCOL': 'UNIFAI',
    'PURPOSE': 'SWAPS',
    } } },
    tags = ['unifai_daily']
) }}

{% if execute %}
    {% if is_incremental() %}
    {% set max_modified_query %}
    SELECT
        MAX(modified_timestamp) AS modified_timestamp
    FROM
        {{ this }}
    {% endset %}
    {% set max_modified_timestamp = run_query(max_modified_query)[0][0] %}
    {% endif %}
{% endif %}

SELECT
    created_at_timestamp,
    tx_hash,
    chain,
    amount,
    input_token,
    output_token,
    slippage,
    status,
    tx_type,
    wallet_address,
    wallet_chain,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    unifai_swaps_id as fact_swaps_id
FROM 
    {{ ref('silver__unifai_swaps') }}
{% if is_incremental() %}
WHERE 
    modified_timestamp >= '{{ max_modified_timestamp }}'
{% endif %}
