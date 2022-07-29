{{ config(
    materialized = 'incremental',
    unique_key = "CONCAT_WS('-', address, start_date)",
    incremental_strategy = 'delete+insert',
) }}


{% if is_incremental() %}
  with base_eth as (
      select distinct 
        from_address as address,
        min(date_trunc('day', block_timestamp)) as start_date
        from     
        {{ source(
        'ethereum_silver',
        'transactions'
        ) }}
        WHERE block_timestamp >= current_date -7
        group by from_address having count(distinct tx_hash) > 0
  ), current_tagged as (
      select *
      from {{ this }}
      where end_date is null and tag_name = 'active on ethereum last 7'
  ), additions as (
      select distinct 
        'ethereum' as blockchain,
        'flipside' as creator,
        address as address,
        'active on ethereum last 7' as tag_name,
        'profile' as tag_type,
        start_date as start_date, 
        null as end_date,
        CURRENT_TIMESTAMP AS tag_created_at
        from base_eth
        where address not in (select distinct address from current_tagged)
  ),
     cap_end_date as (
      select distinct 
        blockchain,
        creator,
        address,
        tag_name,
        tag_type,
        start_date, 
        date_trunc('DAY', current_date) as end_date,
        CURRENT_TIMESTAMP AS tag_created_at
        from current_tagged
        where address not in (select distinct address from base_eth)
  )
  select * from additions
  union 
  select * from cap_end_date

{% else %}

    with address_base AS (
    SELECT distinct from_address as EOA, date_trunc('DAY', block_timestamp) as day_, 1 as active 
    FROM     
    {{ source(
        'ethereum_core',
        'fact_transactions'
    ) }}
    ORDER BY day_ DESC
    ),

    all_dates AS ( 
    SELECT DISTINCT(DATE_TRUNC('DAY', block_timestamp)) as day_all
    FROM 
    {{ source(
        'ethereum_core',
        'fact_transactions'
    ) }}
    ORDER BY day_all DESC
    ),

    all_hits as (
    select  
        eoa,
        day_all,
        datediff('day', lag(day_all) over (partition by eoa order by eoa, day_all), day_all) as difference
    from all_dates a, 
    lateral (select * from address_base as c where a.day_all <= DATEADD('Day', 7,c.day_) AND a.day_all >= c.day_)
    group by eoa, day_all
    order by day_all
    ),
    final_output as (

    select 
        distinct 
        'ethereum' as blockchain,
        'flipside' as creator,
        eoa as address,
        'active on ethereum last 7' as tag_name,
        'profile' as tag_type,
        day_all as start_date, 
        dateadd('day', -lead(difference) over (partition by eoa order by eoa, day_all), lead(day_all) over (partition by eoa order by eoa, day_all)) as end_date
    from all_hits 
    where difference != 1 or difference is null
    order by eoa, day_all
    )
    select 
    distinct
    blockchain, creator, address, tag_name, tag_type, start_date, 
    case when end_date is null then 
        case when datediff('day', start_date, current_date) > 7 then dateadd('day', 7, start_date)
        else null
        end
    else end_date
    end as end_date,
    CURRENT_TIMESTAMP AS tag_created_at
    from final_output
    order by address, start_date

{% endif %}