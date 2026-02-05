/*
    fct_cohort_retention.sql
    
    ═══════════════════════════════════════════════════════════════════════════
                    THE COHORT RETENTION MATRIX (FINAL OUTPUT)
    ═══════════════════════════════════════════════════════════════════════════
    
    PURPOSE:
    ────────
    This is THE main deliverable of the entire project.
    It calculates the retention matrix that answers:
    
        "Of users who bridged in Month X, what % were still active in Month X+1, X+2, etc.?"
    
    WHY THIS MATTERS:
    ─────────────────
    This directly answers the core business question:
        "Are users who bridge to Linea sticking around, or leaving?"
    
    If retention drops fast → Problem with user experience
    If retention stays high → Product is healthy, users find value
    
    WHAT IS A RETENTION MATRIX?
    ───────────────────────────
    
        | Cohort   | Month 0 | Month 1 | Month 2 | Month 3 |
        |----------|---------|---------|---------|---------|
        | 2024-01  | 100%    | 45%     | 28%     | 22%     |  ← Jan cohort
        | 2024-02  | 100%    | 52%     | 31%     | ...     |  ← Feb cohort
    
    - Month 0 = The month they bridged (always 100%)
    - Month 1 = 1 month after bridging
    - Month 2 = 2 months after bridging
    ...
    
    COLUMNS EXPLAINED:
    ──────────────────
    - cohort_month        → The month users first bridged
    - user_segment        → Whale or Retail (we calculate separately!)
    - months_since_bridge → 0, 1, 2, 3... (relative time from cohort)
    - cohort_size         → Total users in this cohort+segment
    - active_users        → How many were active in this relative month
    - retention_rate      → active_users / cohort_size (the % we care about)
*/

-- Linea Canonical Bridge contract (receives bridged funds on L2)
{% set bridge_contract = '0xd19d4b5d358258f05d7b411e21a1460d11b0876f' %}

-- ============================================================================
-- STEP 1: Get all users with their cohort and segment
-- ============================================================================
with users as (

    select 
        user_address,
        cohort_month,
        user_segment
    from {{ ref('dim_users') }}

),

-- ============================================================================
-- STEP 2: Get monthly activity for each user
-- ============================================================================
activity as (

    select 
        user_address,
        activity_month
    from {{ ref('int_monthly_activity') }}

),

-- ============================================================================
-- STEP 3: Calculate cohort sizes (how many users per cohort + segment)
-- This is our denominator for retention rate
-- ============================================================================
cohort_sizes as (

    select
        cohort_month,
        user_segment,
        count(distinct user_address) as cohort_size
    from users
    group by cohort_month, user_segment

),

-- ============================================================================
-- STEP 4: For each user, calculate months_since_bridge for their activity
-- This converts absolute months (2024-01, 2024-02) to relative months (0, 1, 2)
-- ============================================================================
user_relative_activity as (

    select
        u.user_address,
        u.cohort_month,
        u.user_segment,
        a.activity_month,
        
        -- Calculate how many months after bridging this activity occurred
        -- Formula: (activity_year * 12 + activity_month) - (cohort_year * 12 + cohort_month)
        (
            (cast(substring(a.activity_month, 1, 4) as integer) * 12 + 
             cast(substring(a.activity_month, 6, 2) as integer))
            -
            (cast(substring(u.cohort_month, 1, 4) as integer) * 12 + 
             cast(substring(u.cohort_month, 6, 2) as integer))
        ) as months_since_bridge
        
    from users u
    inner join activity a
        on u.user_address = a.user_address
    
    -- Only count activity AFTER bridging (months_since_bridge >= 0)
    where a.activity_month >= u.cohort_month

),

-- ============================================================================
-- STEP 5: Identify users who were active in Month 0 (the baseline for retention)
-- These are the ONLY users we track for true retention in subsequent months
-- ============================================================================
month0_users as (

    select distinct
        user_address,
        cohort_month,
        user_segment
    from user_relative_activity
    where months_since_bridge = 0

),

-- ============================================================================
-- STEP 6: Count Month 0 active users per cohort + segment (retention denominator)
-- ============================================================================
month0_counts as (

    select
        cohort_month,
        user_segment,
        count(distinct user_address) as month0_active_users
    from month0_users
    group by cohort_month, user_segment

),

-- ============================================================================
-- STEP 6b: Count Month 0 users with NON-BRIDGE activity (for activation_rate)
-- ============================================================================
month0_non_bridge_users as (

    select
        u.cohort_month,
        u.user_segment,
        count(distinct t.from_address) as non_bridge_user_count
    from {{ ref('dim_users') }} u
    inner join {{ ref('stg_linea_transactions') }} t
        on u.user_address = t.from_address
        and to_char(t.block_timestamp, 'YYYY-MM') = u.cohort_month
    where lower(t.from_address) != '{{ bridge_contract }}'
    group by u.cohort_month, u.user_segment

),

-- ============================================================================
-- STEP 7: For TRUE RETENTION, count only Month 0 users who returned in Month N
-- This ensures we're tracking the SAME users over time
-- ============================================================================
true_retention_counts as (

    select
        ura.cohort_month,
        ura.user_segment,
        ura.months_since_bridge,
        count(distinct ura.user_address) as returning_users
    from user_relative_activity ura
    inner join month0_users m0
        on ura.user_address = m0.user_address
        and ura.cohort_month = m0.cohort_month
        and ura.user_segment = m0.user_segment
    where ura.months_since_bridge >= 0
      and ura.months_since_bridge <= 12
    group by ura.cohort_month, ura.user_segment, ura.months_since_bridge

),

-- ============================================================================
-- STEP 8: For CUMULATIVE RETENTION, count users active in ALL months 0 through N
-- This gives a strictly decreasing curve (classic SaaS-style retention)
-- ============================================================================
user_month_counts as (
    -- Count how many months each user was active in (from 0 to their max month)
    select
        ura.user_address,
        ura.cohort_month,
        ura.user_segment,
        max(ura.months_since_bridge) as max_month,
        count(distinct ura.months_since_bridge) as active_month_count
    from user_relative_activity ura
    inner join month0_users m0
        on ura.user_address = m0.user_address
        and ura.cohort_month = m0.cohort_month
        and ura.user_segment = m0.user_segment
    where ura.months_since_bridge >= 0
      and ura.months_since_bridge <= 12
    group by ura.user_address, ura.cohort_month, ura.user_segment
),

cumulative_retention as (
    -- A user is "cumulatively retained" at month N if they were active in ALL months 0 through N
    select
        umc.cohort_month,
        umc.user_segment,
        m.month_num as months_since_bridge,
        count(distinct case 
            when umc.active_month_count >= (m.month_num + 1) 
             and umc.max_month >= m.month_num
            then umc.user_address 
        end) as cumulative_users
    from user_month_counts umc
    cross join (
        select generate_series(0, 12) as month_num
    ) m
    group by umc.cohort_month, umc.user_segment, m.month_num
),

-- ============================================================================
-- STEP 9: Calculate all metrics
-- ============================================================================
final as (

    select
        r.cohort_month,
        r.user_segment,
        r.months_since_bridge,
        s.cohort_size,
        m.month0_active_users,
        r.returning_users as active_users,
        coalesce(c.cumulative_users, 0) as cumulative_users,
        
        -- Metric type for filtering in dashboards
        case 
            when r.months_since_bridge = 0 then 'activation_rate'
            else 'retention_rate'
        end as metric_type,
        
        -- 1. Activation Rate (Month 0 only): % of bridgers who had NON-BRIDGE Linea activity
        case 
            when r.months_since_bridge = 0 then 
                round((coalesce(nb.non_bridge_user_count, 0)::decimal / s.cohort_size) * 100, 2)
            else null 
        end as activation_rate,
        
        -- 2. Month-Specific Retention: % of COHORT active in THIS month
        -- Month 0 = Activation Rate (Excludes bridge transaction)
        case 
            when r.months_since_bridge = 0 then 
                round((coalesce(nb.non_bridge_user_count, 0)::decimal / s.cohort_size) * 100, 2)
            else 
                round((r.returning_users::decimal / s.cohort_size) * 100, 2)
        end as retention_rate,
        
        -- 3. Cumulative Retention: % of COHORT active in ALL months 0 through N
        -- Always decreasing
        case 
            when r.months_since_bridge = 0 then 
                round((coalesce(nb.non_bridge_user_count, 0)::decimal / s.cohort_size) * 100, 2)
            else 
                round((coalesce(c.cumulative_users, 0)::decimal / s.cohort_size) * 100, 2)
        end as cumulative_retention_rate,
        
        -- For visualization: create a readable month label
        case r.months_since_bridge
            when 0 then 'Month 0 (Bridge Month)'
            when 1 then 'Month 1'
            when 2 then 'Month 2'
            when 3 then 'Month 3'
            when 6 then 'Month 6'
            when 12 then 'Month 12'
            else 'Month ' || r.months_since_bridge
        end as month_label
        
    from true_retention_counts r
    join cohort_sizes s
        on r.cohort_month = s.cohort_month
        and r.user_segment = s.user_segment
    join month0_counts m
        on r.cohort_month = m.cohort_month
        and r.user_segment = m.user_segment
    left join month0_non_bridge_users nb
        on r.cohort_month = nb.cohort_month
        and r.user_segment = nb.user_segment
    left join cumulative_retention c
        on r.cohort_month = c.cohort_month
        and r.user_segment = c.user_segment
        and r.months_since_bridge = c.months_since_bridge
    
    order by 
        r.cohort_month,
        r.user_segment,
        r.months_since_bridge

)

select * from final

