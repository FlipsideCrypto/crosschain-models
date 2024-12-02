{{ config(
    materialized = 'incremental',
    unique_key = "CONCAT_WS('-', address, start_date::DATE)",
    incremental_strategy = 'delete+insert',
    tags = ['monthly']
) }}

{% if is_incremental() %}
WITH filtered_pools AS (
    SELECT DISTINCT pool_address 
    FROM {{ source('kaia_defi', 'dim_dex_liquidity_pools') }}
),
filtered_swaps AS (
    SELECT DISTINCT tx_hash 
    FROM {{ source('kaia_defi', 'ez_dex_swaps') }}
),
lp_positions as (
SELECT 
    CASE 
        WHEN tt.to_address = fp.pool_address THEN tt.from_address 
        ELSE tt.to_address 
    END as address,
    DATE(tt.block_timestamp) as activity_date,
    tt.contract_address as token_address,
    CASE 
        WHEN tt.to_address = fp.pool_address THEN tt.amount  -- LP deposit
        ELSE -tt.amount  -- LP withdrawal
    END as amount
FROM {{ source('kaia_core', 'ez_token_transfers') }} tt
INNER JOIN filtered_pools fp 
    ON tt.to_address = fp.pool_address 
    OR tt.from_address = fp.pool_address
LEFT JOIN filtered_swaps fs 
    ON tt.tx_hash = fs.tx_hash
WHERE fs.tx_hash IS NULL
and activity_date is not NULL
),
  net_positions as (
    -- Calculate net position per address
    SELECT 
      address,
      min(date_trunc('day', current_date - 30)) as start_date,
      SUM(amount) as net_amount
    FROM lp_positions
    GROUP BY address
    HAVING SUM(amount) > 0 -- Only include addresses with positive LP positions
  ),
  current_tagged as (
    SELECT *
    FROM {{ this }}
    WHERE end_date is null
  ),
  additions as (
    -- New addresses to tag
    SELECT DISTINCT 
      'kaia' as blockchain,
      'flipside' as creator,
      address,
      'active LP provider' as tag_name,
      'liquidity' as tag_type,
      start_date,
      null as end_date,
      CURRENT_TIMESTAMP AS tag_created_at,
      sysdate() as inserted_timestamp,
      sysdate() as modified_timestamp,
      {{ dbt_utils.generate_surrogate_key(['address','start_date']) }} AS tags_active_lp_id,
      '{{ invocation_id }}' as _invocation_id
    FROM net_positions
    WHERE address NOT IN (SELECT DISTINCT address FROM current_tagged)
  ),
  cap_end_date as (
    -- End tags for addresses no longer meeting criteria
    SELECT DISTINCT 
      blockchain,
      creator,
      address,
      tag_name,
      tag_type,
      start_date::date,
      date_trunc('DAY', current_date)::date as end_date,
      tag_created_at,
      inserted_timestamp,
      modified_timestamp,
      {{ dbt_utils.generate_surrogate_key(['address','start_date']) }} AS tags_active_lp_id,
      '{{ invocation_id }}' as _invocation_id
    FROM current_tagged
    WHERE address NOT IN (SELECT DISTINCT address FROM net_positions)
  )
  SELECT * FROM additions
  UNION 
  SELECT * FROM cap_end_date

{% else %}
    WITH filtered_pools AS (
        SELECT DISTINCT pool_address 
        FROM {{ source('kaia_defi', 'dim_dex_liquidity_pools') }}
    ),
    filtered_swaps AS (
        SELECT DISTINCT tx_hash 
        FROM {{ source('kaia_defi', 'ez_dex_swaps') }}
        where block_timestamp is not null
    ),
    lp_history as (
    SELECT 
        CASE 
            WHEN tt.to_address = fp.pool_address THEN tt.from_address 
            ELSE tt.to_address 
        END as address,
        DATE(tt.block_timestamp) as activity_date,
        tt.contract_address as token_address,
        CASE 
            WHEN tt.to_address = fp.pool_address THEN tt.amount  -- LP deposit
            ELSE -tt.amount  -- LP withdrawal
        END as amount
    FROM {{ source('kaia_core', 'ez_token_transfers') }} tt
    INNER JOIN filtered_pools fp 
        ON tt.to_address = fp.pool_address 
        OR tt.from_address = fp.pool_address
    LEFT JOIN filtered_swaps fs 
        ON tt.tx_hash = fs.tx_hash
    WHERE fs.tx_hash IS NULL
    and activity_date is not NULL
    ),
  running_balance AS (
    -- Calculate running balance over time
    SELECT 
      address,
      activity_date as bt,
      SUM(amount) OVER (
        PARTITION BY address 
        ORDER BY activity_date
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
      ) as cumulative_amount
    FROM lp_history
    QUALIFY cumulative_amount > 0
  ),
    running_balance_group AS (
    -- Calculate running balance over time
    SELECT 
      address,
      bt,
      SUM(cumulative_amount) as cumulative_amount
    FROM running_balance
    group by 1,2
  ),
  next_date as (
        select *, 
            lead(bt) over (partition by address order by bt) as nt,
            datediff('day',bt, nt) as days_between_activity
        from running_balance_group
    )
    , conditional_group as (
    select 
        *,
        conditional_true_event(days_between_activity > 30) over (partition by address order by bt) as e
        from next_date
    )
    , conditional_group_lagged as (
        select *,
        coalesce(lag(e) over (partition by address order by bt),0) as grouping_val
        from conditional_group
    )
    , final_base as (
        select 
            address,
            grouping_val,
            min(bt) as start_date,
            dateadd('day',30,max(bt)) as end_date
        from conditional_group_lagged
        group by 1, 2
    )
  SELECT DISTINCT
    'kaia' as blockchain,
    'flipside' as creator,
    address,
    'active LP provider' as tag_name,
    'liquidity' as tag_type,
    start_date,
    CASE 
      WHEN end_date >= CURRENT_DATE THEN NULL 
      ELSE end_date 
    END as end_date,
    CURRENT_TIMESTAMP AS tag_created_at,
    sysdate() as inserted_timestamp,
    sysdate() as modified_timestamp,
    {{ dbt_utils.generate_surrogate_key(['address','start_date']) }} AS tags_active_lp_id,
    '{{ invocation_id }}' as _invocation_id
  FROM final_base

{% endif %}