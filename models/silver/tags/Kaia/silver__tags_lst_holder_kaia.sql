{{ config(
    materialized = 'incremental',
    unique_key = "CONCAT_WS('-', address, start_date::DATE)",
    incremental_strategy = 'delete+insert',
    tags = ['monthly']
) }}

{% if is_incremental() %}
  with lst_tokens as (
    -- Define known LST tokens on Kaia
    select distinct 
      address as token_address,
      symbol,
      decimals
    from {{ source('kaia_core', 'dim_contracts') }}
    where lower(symbol) in (lower('sKLAY'), lower('stKLAY'))  -- Add other LST tokens as needed
  ),
  current_balances as (
    -- Calculate current token balances
    select 
      to_address as address,
      t.contract_address as token_address,
      lst.symbol,
      lst.decimals,
      sum(case 
        when to_address = address then amount
        when from_address = address then -amount
        end) as balance
    from {{ source('kaia_core', 'ez_token_transfers') }} t
    inner join lst_tokens lst 
      on t.contract_address = lst.token_address
    group by 1,2,3,4
    having balance > 0
  ),
  token_prices as (
    -- Get latest prices for LST tokens
    select 
      token_address,
      price as token_price
    from {{ source('kaia_price', 'ez_prices_hourly') }}
    where is_native = true
    qualify row_number() over (partition by token_address order by hour desc) = 1
  ),
  valued_balances as (
    -- Calculate USD value of holdings
    select 
      cb.address,
      min(date_trunc('day', current_date - 30)) as start_date,
      sum(cb.balance * tp.token_price) as total_lst_value
    from current_balances cb
    left join token_prices tp 
      on 1 = 1
    group by 1
    having total_lst_value >= 150
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
        'holds $150+ in LST tokens' as tag_name,
        'lst_holder' as tag_type,
        start_date::date as start_date, 
        null as end_date,
        CURRENT_TIMESTAMP AS tag_created_at,
        sysdate() as inserted_timestamp,
        sysdate() as modified_timestamp,
        {{ dbt_utils.generate_surrogate_key(['address','start_date']) }} AS tags_lst_holder_id,
        '{{ invocation_id }}' as _invocation_id
    from valued_balances
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
        {{ dbt_utils.generate_surrogate_key(['address','start_date']) }} AS tags_lst_holder_id,
        '{{ invocation_id }}' as _invocation_id
    from current_tagged
    where address not in (select distinct address from valued_balances)
  )
  select * from additions
  union 
  select * from cap_end_date

{% else %}

    with lst_tokens as (
        -- Define known LST tokens on Kaia
        select distinct 
          address as token_address,
          symbol,
          decimals
        from {{ source('kaia_core', 'dim_contracts') }}
        where lower(symbol) in (lower('sKLAY'), lower('stKLAY'))  -- Add other LST tokens as needed
    ),
    daily_balances as (
        -- Calculate daily token balances with running total across ALL days
        select 
          date_trunc('day', block_timestamp) as bt,
          to_address as address,
          t.contract_address as token_address,
          lst.symbol,
          lst.decimals,
          sum(case 
            when to_address = address then amount
            when from_address = address then -amount
            end) as daily_change
        from {{ source('kaia_core', 'ez_token_transfers') }} t
        inner join lst_tokens lst 
          on t.contract_address = lst.token_address
          where bt is not null
        group by 1, 2, 3, 4, 5
    ),
    daily_balances_agg as (
      select 
    bt,
    address,
    token_address,
    symbol,
    decimals,
    daily_change,
    sum(daily_change) over (
        partition by address, token_address 
        order by bt
        rows unbounded preceding
    ) as running_balance
from daily_balances
    ),
    daily_prices as (
        -- Get daily prices for LST tokens
        select 
          date_trunc('day', hour) as day,
          token_address,
          avg(price) as token_price
        from {{ source('kaia_price', 'ez_prices_hourly') }}
        where is_native = true
        group by 1, 2
    ),
    daily_valued_balances as (
        -- Calculate daily USD value of cumulative holdings
        select 
          db.bt,
          db.address,
          sum(db.running_balance * dp.token_price) as total_lst_value
        from daily_balances_agg db
        left join daily_prices dp 
          on db.bt = dp.day
        group by 1, 2
        having total_lst_value >= 150
    ),
    next_date as (
        select *, 
            lead(bt) over (partition by address order by bt) as nt,
            datediff('day', bt, nt) as days_between_activity
        from daily_valued_balances
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
        'holds $150+ in LST tokens' as tag_name,
        'lst_holder' as tag_type,
        start_date, 
        iff(end_date > current_date, null, end_date) as end_date,
        CURRENT_TIMESTAMP AS tag_created_at,
        sysdate() as inserted_timestamp,
        sysdate() as modified_timestamp,
        {{ dbt_utils.generate_surrogate_key(['address','start_date']) }} AS tags_lst_holder_id,
        '{{ invocation_id }}' as _invocation_id
    from final_base

{% endif %}