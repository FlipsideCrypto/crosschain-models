{{ config (
    materialized = "table",
    tags = ['ai_metrics'],
    cluster_by = ["blockchain"],
    post_hook = "ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION ON EQUALITY(contract_address)"
) }}

with base as (
    
select distinct 
    blockchain, 
    platform,
    bridge_address as contract_address,
    'bridge' as contract_type
from 
    {{ ref('defi__ez_bridge_activity') }}
union all 
select distinct 
    blockchain, 
    platform,
    contract_address,
    'dex' as contract_type
from 
    {{ ref('defi__ez_dex_swaps') }}
union all
select distinct 
    blockchain,
    protocol,
    contract_address,
    'lending' as contract_type
from 
    {{ ref('defi__ez_lending_deposits') }}
union all 
select distinct 
    blockchain,
    protocol,
    contract_address,
    'lending' as contract_type
from 
    {{ ref('defi__ez_lending_borrows') }}
)
select 
    blockchain,
    platform,
    contract_address,
    contract_type
from base

qualify row_number() over (partition by blockchain, contract_address order by platform asc, contract_type asc) = 1