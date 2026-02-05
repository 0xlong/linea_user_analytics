/*
    dim_users.sql
    
    ═══════════════════════════════════════════════════════════════════════════
                            USER DIMENSION TABLE
    ═══════════════════════════════════════════════════════════════════════════
    
    PURPOSE:
    ────────
    This is the "master user table" - ONE ROW PER USER with all their attributes.
    It combines cohort info + segment info + activity summary into a single view.
    
    WHY WE NEED THIS:
    ─────────────────
    • Dashboard filtering: "Show me only Whales from January 2024"
    • User-level analysis: "What does our average user look like?"
    • Joining to fact tables: Any fact table can join here to get user attributes
    
    BUSINESS QUESTIONS ANSWERED:
    ────────────────────────────
    1. How many users do we have per cohort?
    2. What's the whale vs retail split?
    3. Are users still active or churned?
    4. What's the average user profile?
    
    COLUMNS EXPLAINED:
    ──────────────────
    - user_address       → Wallet address (our user ID)
    - cohort_month       → When they first bridged (YYYY-MM)
    - user_segment       → Whale or Retail (based on bridge volume)
    - user_tier          → More granular: Mega Whale, Whale, Mid-tier, Retail, Micro
    - total_bridged_eth  → How much ETH they bridged total
    - total_bridge_count → How many bridge transactions
    - lifetime_tx_count  → Total transactions on Linea (ever)
    - active_months      → Number of months they were active on Linea
    - last_active_month  → Most recent month they did something on Linea
    - is_churned         → Have they been inactive for 2+ months?
*/

-- ============================================================================
-- STEP 1: Get user cohort and segment data (from intermediate layer)
-- ============================================================================
with user_segments as (

    select * from {{ ref('int_user_segments') }}

),

-- ============================================================================
-- STEP 2: Summarize each user's total Linea activity
-- ============================================================================
user_activity_summary as (

    select
        user_address,
        
        -- How many months were they active?
        count(distinct activity_month) as active_months,
        
        -- Total transactions ever
        sum(transaction_count) as lifetime_tx_count,
        
        -- Total ETH moved on Linea
        sum(total_eth_transacted) as lifetime_eth_transacted,
        
        -- When was their last activity?
        max(activity_month) as last_active_month,
        
        -- When was their first activity on Linea?
        min(activity_month) as first_active_month
        
    from {{ ref('int_monthly_activity') }}
    group by user_address

),

-- ============================================================================
-- STEP 3: Join everything together into one master user table
-- ============================================================================
final as (

    select
        -- ─────────────────────────────────────────────────────
        -- IDENTITY
        -- ─────────────────────────────────────────────────────
        s.user_address,
        
        -- ─────────────────────────────────────────────────────
        -- COHORT INFO (when did they arrive?)
        -- ─────────────────────────────────────────────────────
        s.cohort_month,
        s.first_bridge_date,
        
        -- ─────────────────────────────────────────────────────
        -- SEGMENT INFO (how valuable are they?)
        -- ─────────────────────────────────────────────────────
        s.user_segment,           -- Whale or Retail
        s.user_tier,              -- More granular tiers
        s.volume_rank_in_cohort,  -- Rank among their cohort peers
        
        -- ─────────────────────────────────────────────────────
        -- BRIDGE METRICS
        -- ─────────────────────────────────────────────────────
        s.total_bridged_eth,
        s.total_bridge_count,
        
        -- ─────────────────────────────────────────────────────
        -- LINEA ACTIVITY METRICS
        -- ─────────────────────────────────────────────────────
        coalesce(a.active_months, 0) as active_months,
        coalesce(a.lifetime_tx_count, 0) as lifetime_tx_count,
        coalesce(a.lifetime_eth_transacted, 0) as lifetime_eth_transacted,
        a.first_active_month,
        a.last_active_month,
        
        -- ─────────────────────────────────────────────────────
        -- CHURN INDICATOR
        -- A user is "churned" if they haven't been active in 2+ months
        -- This helps identify users we might be losing
        -- ─────────────────────────────────────────────────────
        case
            when a.last_active_month is null then true  -- Never active on Linea
            when a.last_active_month < to_char(current_date - interval '2 months', 'YYYY-MM') 
                then true
            else false
        end as is_churned,
        
        -- ─────────────────────────────────────────────────────
        -- ENGAGEMENT SCORE (simple formula for user health)
        -- Combines: bridge volume + tx activity + recency
        -- ─────────────────────────────────────────────────────
        case
            when a.active_months >= 3 and s.user_segment = 'Whale' then 'High Value Retained (3+ months, Whale)'
            when a.active_months >= 3 then 'Retained (3+ months)'
            when a.active_months >= 1 then 'Engaged (1-2 months)'
            when a.active_months = 0 or a.active_months is null then 'Bridge Only (0 months)'
            else 'Unknown'
        end as engagement_status

    from user_segments s
    left join user_activity_summary a
        on s.user_address = a.user_address

)

select * from final
