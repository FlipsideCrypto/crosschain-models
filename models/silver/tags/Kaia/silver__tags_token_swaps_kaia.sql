{{ config(
    materialized = 'incremental',
    unique_key = "CONCAT_WS('-', address, start_date::DATE)",
    incremental_strategy = 'delete+insert',
    tags = ['monthly']
) }}

{% if is_incremental() %}
  with swap_activity as (
      select distinct 
        origin_from_address as address,
        min(date_trunc('day', block_timestamp)) as start_date
      from     
      {{ source(
        'kaia_defi',
        'ez_dex_swaps'
      ) }}
      where block_timestamp >= current_date - 30
      -- Ensuring we have valid swaps with amounts
      and amount_in > 0 
      and amount_out > 0
      group by origin_from_address
  ), 
  current_tagged as (
      select *
      from {{ this }}
      where end_date is null
  ), 
  additions as (
        select distinct 
            'kaia' as blockchain,
            'flipside' as creator,
            address,
            'token swapper' as tag_name,
            'defi_activity' as tag_type,
            start_date::date as start_date, 
            null as end_date,
            CURRENT_TIMESTAMP AS tag_created_at,
            sysdate() as inserted_timestamp,
            sysdate() as modified_timestamp,
            {{ dbt_utils.generate_surrogate_key(['address','start_date']) }} AS tags_dex_swap_activity_id,
            '{{ invocation_id }}' as _invocation_id
        from swap_activity
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
            {{ dbt_utils.generate_surrogate_key(['address','start_date']) }} AS tags_dex_swap_activity_id,
            '{{ invocation_id }}' as _invocation_id
        from current_tagged
        where address not in (select distinct address from swap_activity)
  )
  select * from additions
  union 
  select * from cap_end_date

{% else %}

    with swap_base as (
    select distinct 
        origin_from_address as address, 
        block_timestamp::date as bt
    from    
    {{ source(
        'kaia_defi',
        'ez_dex_swaps'
    ) }}
    where amount_in > 0 
    and amount_out > 0
    and bt is not null
    ),
    next_date as (
        select *, 
            lead(bt) over (partition by address order by bt) as nt,
            datediff('day', bt, nt) as days_between_activity
        from swap_base
    ),
    conditional_group as (
        select 
            *,
            conditional_true_event(days_between_activity > 30) over (partition by address order by bt) as e
        from next_date
    ),
    conditional_group_lagged as (
        select *,
        coalesce(lag(e) over (partition by address order by bt), 0) as grouping_val
        from conditional_group
    ),
    final_base as (
        select 
            address,
            grouping_val,
            min(bt) as start_date,
            dateadd('day', 30, max(bt)) as end_date
        from conditional_group_lagged
        group by 1, 2
    )
    select 
        'kaia' as blockchain,
        'flipside' as creator,
        address,
        'token swapper' as tag_name,
        'defi_activity' as tag_type,
        start_date, 
        iff(end_date > current_date, null, end_date) as end_date,
        CURRENT_TIMESTAMP AS tag_created_at,
        sysdate() as inserted_timestamp,
        sysdate() as modified_timestamp,
        {{ dbt_utils.generate_surrogate_key(['address','start_date']) }} AS tags_dex_swap_activity_id,
        '{{ invocation_id }}' as _invocation_id
    from final_base

{% endif %}