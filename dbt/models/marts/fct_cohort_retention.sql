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
-- STEP 5: Count active users per cohort + segment + relative month
-- ============================================================================
retention_counts as (

    select
        cohort_month,
        user_segment,
        months_since_bridge,
        count(distinct user_address) as active_users
    from user_relative_activity
    where months_since_bridge >= 0
      and months_since_bridge <= 12  -- Limit to first 12 months for readability
    group by cohort_month, user_segment, months_since_bridge

),

-- ============================================================================
-- STEP 6: Calculate retention rate = active_users / cohort_size
-- ============================================================================
final as (

    select
        r.cohort_month,
        r.user_segment,
        r.months_since_bridge, --
        s.cohort_size,
        r.active_users,
        
        -- THE KEY METRIC: What % of the cohort is still active?
        round(
            (r.active_users::decimal / s.cohort_size) * 100, 
            2
        ) as retention_rate,
        
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
        
    from retention_counts r
    join cohort_sizes s
        on r.cohort_month = s.cohort_month
        and r.user_segment = s.user_segment
    
    order by 
        r.cohort_month,
        r.user_segment,
        r.months_since_bridge

)

select * from final
