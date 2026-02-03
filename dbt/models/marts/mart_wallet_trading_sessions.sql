/*
    mart_wallet_trading_sessions.sql
    
    ═══════════════════════════════════════════════════════════════════════════
                        WALLET TRADING SESSION ANALYSIS
    ═══════════════════════════════════════════════════════════════════════════
    
    PURPOSE:
    ────────
    Analyzes when wallets trade based on UTC time, classifying transactions
    into trading sessions (Asian, European, American).
    
    TRADING SESSIONS (UTC):
    ───────────────────────
    • Asian Session:     00:00 - 08:00 (Binance, OKX)
    • European Session:  08:00 - 16:00 (Kraken, Bitstamp)
    • American Session:  13:00 - 22:00 (Coinbase, Gemini)
    
    BUSINESS QUESTIONS ANSWERED:
    ────────────────────────────
    1. Which trading session is each wallet most active in?
    2. What's the distribution of users across sessions?
    4. How does transaction volume differ by session?
    
    COLUMNS EXPLAINED:
    ──────────────────
    - user_address          → Wallet address
    - dominant_session      → Session with most transactions
    - asian_tx_count        → Transactions during Asian session
    - european_tx_count     → Transactions during European session
    - american_tx_count     → Transactions during American session
    - total_tx_count        → Total transactions
    - dominant_session_pct  → % of transactions in dominant session
*/

-- ============================================================================
-- STEP 1: Tag each transaction with its trading session
-- ============================================================================
with transactions_with_session as (

    select
        from_address as user_address,
        block_timestamp,
        extract(hour from block_timestamp) as tx_hour,
        value_eth,
        
        -- Classify transaction into trading session
        case
            when extract(hour from block_timestamp) >= 0 
                 and extract(hour from block_timestamp) < 8 
                then 'asian'
            when extract(hour from block_timestamp) >= 8 
                 and extract(hour from block_timestamp) < 16 
                then 'european'
            when extract(hour from block_timestamp) >= 13 
                 and extract(hour from block_timestamp) < 22 
                then 'american'
            else 'off_hours'  -- 22:00 - 00:00 UTC
        end as trading_session

    from {{ ref('stg_etherscan_logs') }}

),

-- ============================================================================
-- STEP 2: Aggregate transactions per user per session
-- ============================================================================
user_session_stats as (

    select
        user_address,
        
        -- Count transactions per session
        count(*) filter (where trading_session = 'asian') as asian_tx_count,
        count(*) filter (where trading_session = 'european') as european_tx_count,
        count(*) filter (where trading_session = 'american') as american_tx_count,
        count(*) filter (where trading_session = 'off_hours') as off_hours_tx_count,
        count(*) as total_tx_count,
        
        -- Sum ETH volume per session
        coalesce(sum(value_eth) filter (where trading_session = 'asian'), 0) as asian_eth_volume,
        coalesce(sum(value_eth) filter (where trading_session = 'european'), 0) as european_eth_volume,
        coalesce(sum(value_eth) filter (where trading_session = 'american'), 0) as american_eth_volume,
        coalesce(sum(value_eth) filter (where trading_session = 'off_hours'), 0) as off_hours_eth_volume,
        coalesce(sum(value_eth), 0) as total_eth_volume

    from transactions_with_session
    group by user_address

),

-- ============================================================================
-- STEP 3: Determine dominant session for each user
-- ============================================================================
final as (

    select
        user_address,
        
        -- ─────────────────────────────────────────────────────
        -- TRANSACTION COUNTS BY SESSION
        -- ─────────────────────────────────────────────────────
        asian_tx_count,
        european_tx_count,
        american_tx_count,
        off_hours_tx_count,
        total_tx_count,
        
        -- ─────────────────────────────────────────────────────
        -- ETH VOLUME BY SESSION
        -- ─────────────────────────────────────────────────────
        asian_eth_volume,
        european_eth_volume,
        american_eth_volume,
        off_hours_eth_volume,
        total_eth_volume,
        
        -- ─────────────────────────────────────────────────────
        -- DOMINANT SESSION (most transactions)
        -- ─────────────────────────────────────────────────────
        case
            when greatest(asian_tx_count, european_tx_count, american_tx_count, 
                         off_hours_tx_count) = american_tx_count 
                then 'american'
            when greatest(asian_tx_count, european_tx_count, american_tx_count, 
                         off_hours_tx_count) = european_tx_count 
                then 'european'
            when greatest(asian_tx_count, european_tx_count, american_tx_count, 
                         off_hours_tx_count) = asian_tx_count 
                then 'asian'
            else 'off_hours'
        end as dominant_session,
        
        -- ─────────────────────────────────────────────────────
        -- SESSION PERCENTAGES
        -- ─────────────────────────────────────────────────────
        round(100.0 * asian_tx_count / nullif(total_tx_count, 0), 2) as asian_tx_pct,
        round(100.0 * european_tx_count / nullif(total_tx_count, 0), 2) as european_tx_pct,
        round(100.0 * american_tx_count / nullif(total_tx_count, 0), 2) as american_tx_pct,
        round(100.0 * off_hours_tx_count / nullif(total_tx_count, 0), 2) as off_hours_tx_pct

    from user_session_stats

)

select * from final
