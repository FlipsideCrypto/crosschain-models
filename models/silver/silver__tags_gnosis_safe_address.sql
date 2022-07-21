{{ config(
    materialized = 'incremental',
    unique_key = "address",
    incremental_strategy = 'delete+insert',
) }}

SELECT
    DISTINCT 'ethereum' AS blockchain,
    'flipside' AS creator,
    TX_JSON:receipt:logs[0]:decoded:inputs:instantiation::string AS address,
    'gnosis safe address' AS tag_name,
    'contract' AS tag_type,
    DATE_TRUNC(
        'day',
        block_timestamp
    ) AS start_date,
    NULL AS end_date,
    current_timestamp as _inserted_timestamp
FROM
    {{source('ethereum_core', 'fact_transactions')}}
WHERE TX_JSON:receipt:logs[0]:decoded:inputs:instantiation != 'null'
    
    {% if is_incremental() %} 
    and 
        block_timestamp > (
        SELECT
            MAX(start_date)
        FROM {{this}}
    ) 
    {% endif %} 