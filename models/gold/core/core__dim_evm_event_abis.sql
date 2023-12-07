{{ config(
    materialized = 'view',
    persist_docs ={ "relation": true,
    "columns": true }
) }}

SELECT
    blockchain,
    parent_contract_address,
    event_name,
    abi,
    simple_event_name,
    event_signature,
    start_block,
    end_block
FROM
    {{ ref('silver__event_abis') }}
