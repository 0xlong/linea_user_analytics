/*
    int_monthly_activity.sql
    
    Purpose: Track each user's monthly activity on the Linea network.
    This is the foundation for calculating retention rates.
    
    Business question answered: "Which months was each user active on Linea?"
*/

with linea_transactions as (

    select * from {{ ref('stg_linea_transactions') }}

),

monthly_activity as (

    select
        from_address as user_address,
        
        -- Activity month in YYYY-MM format
        to_char(block_timestamp, 'YYYY-MM') as activity_month,
        
        -- Activity metrics
        count(*) as transaction_count,
        count(distinct date(block_timestamp)) as active_days,
        count(distinct to_address) as unique_contracts_interacted,

        -- Volume metrics
        sum(value_eth) as total_eth_transacted,
        
        -- Timing
        min(block_timestamp) as first_tx_of_month,
        max(block_timestamp) as last_tx_of_month,
        
        -- Success rate
        sum(case when if_transaction_failed = false then 1 else 0 end) as successful_tx_count,
        sum(case when if_transaction_failed = true then 1 else 0 end) as failed_tx_count

    from linea_transactions
    group by from_address, to_char(block_timestamp, 'YYYY-MM')

),

-- Add activity intensity classification
enriched as (

    select
        *,
        
        -- Activity level for the month
        case
            when transaction_count >= 50 then 'Power User'
            when transaction_count >= 10 then 'Active'
            when transaction_count >= 3 then 'Casual'
            else 'Minimal'
        end as activity_level

    from monthly_activity

)

select * from enriched
