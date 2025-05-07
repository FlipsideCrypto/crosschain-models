{{ config(
    materialized = 'incremental',
    unique_key = ['fact_purchases_id'],
    incremental_strategy = 'delete+insert',
    post_hook = "ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION ON EQUALITY(tx_hash, from_address, to_address)",
    cluster_by = ['created_at_timestamp::DATE','chain'],
    meta ={ 'database_tags':{ 'table':{ 'PROTOCOL': 'UNIFAI',
    'PURPOSE': 'PURCHASES',
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
    status,
    from_address,
    to_address,
    token_amount,
    token_symbol,
    token_value,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    unifai_purchases_id as fact_purchases_id
FROM 
    {{ ref('silver__unifai_purchases') }}
{% if is_incremental() %}
WHERE 
    modified_timestamp >= '{{ max_modified_timestamp }}'
{% endif %}
