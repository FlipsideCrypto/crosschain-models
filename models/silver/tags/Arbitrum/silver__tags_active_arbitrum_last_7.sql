{{ config(
    materialized = 'incremental',
    unique_key = "CONCAT_WS('-', address, start_date)",
    incremental_strategy = 'delete+insert',
    tags = ['daily']
) }}


{% if is_incremental() %}
  with base as (
      select distinct 
        from_address as address,
        min(date_trunc('day', block_timestamp)) as start_date
        from     
        {{ source(
        'arbitrum_silver',
        'transactions'
        ) }}
        WHERE block_timestamp >= current_date -7
        group by from_address 
  ), current_tagged as (
      select *
      from {{ this }}
      where end_date is null 
  ), additions as (
      select distinct 
        'arbitrum' as blockchain,
        'flipside' as creator,
        address as address,
        'active on arbitrum last 7' as tag_name,
        'activity' as tag_type,
        start_date::date as start_date, 
        null as end_date,
        CURRENT_TIMESTAMP AS tag_created_at,
        sysdate() as inserted_timestamp,
        sysdate() as modified_timestamp,
        {{ dbt_utils.generate_surrogate_key(['address','start_date']) }} AS tags_active_arbitrum_last_7_id,
        '{{ invocation_id }}' as _invocation_id
        from base
        where address not in (select distinct address from current_tagged)
  ),
     cap_end_date as (
      select distinct 
        blockchain,
        creator,
        address,
        tag_name,
        tag_type,
        start_date::date as start_date, 
        date_trunc('DAY', current_date)::date as end_date,
        CURRENT_TIMESTAMP AS tag_created_at,
        sysdate() as inserted_timestamp,
        sysdate() as modified_timestamp,
        {{ dbt_utils.generate_surrogate_key(['address','start_date']) }} AS tags_active_arbitrum_last_7_id,
        '{{ invocation_id }}' as _invocation_id
        from current_tagged
        where address not in (select distinct address from base)
  )
  select * from additions
  union 
  select * from cap_end_date

{% else %}

    with address_base as (
    select distinct from_address, block_timestamp::date as bt
    from    
    {{ source(
        'arbitrum_silver',
        'transactions'
    ) }}
    ),
    next_date as (
        select *, 
            lead(bt) over (partition by from_address order by bt) as nt,
            datediff('day',bt, nt) as days_between_activity
        from address_base
    )
    , conditional_group as (
    select 
        *,
        conditional_true_event(days_between_activity > 7) over (partition by from_address order by bt) as e
        from next_date
    )
    , conditional_group_lagged as (
        select *,
        coalesce(lag(e) over (partition by from_address order by bt),0) as grouping_val
        from conditional_group
    )
    , final_base as (
        select 
            from_address,
            grouping_val,
            min(bt) as start_date,
            dateadd('day',7,max(bt)) as end_date
        from conditional_group_lagged
        group by 1, 2
    )
    select 
        'arbitrum' as blockchain,
        'flipside' as creator,
        from_address as address,
        'active on arbitrum last 7' as tag_name,
        'activity' as tag_type,
        start_date, 
        iff(end_date>current_date, null, end_date) as end_date,
        CURRENT_TIMESTAMP AS tag_created_at,
        sysdate() as inserted_timestamp,
        sysdate() as modified_timestamp,
        {{ dbt_utils.generate_surrogate_key(['address','start_date']) }} AS tags_active_arbitrum_last_7_id,
        '{{ invocation_id }}' as _invocation_id
        
    from final_base


{% endif %}