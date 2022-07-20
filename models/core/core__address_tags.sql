{{ config(
    materialized = 'view',
) }}


select 
    blockchain,
    creator,
    address,
    tag_name,
    tag_type,
    start_date,
    end_date,
    _inserted_timestamp
from 
    {{ref('silver__tags_contract_address')}}
