-- Aggregated activity buckets for Superset visualization
-- Pre-aggregated by month with user segment breakdown

WITH user_activity AS (
    SELECT
        fc.activity_month,
        fc.activity_bucket,
        fc.user_address,
        fc.transaction_hash
    FROM {{ ref('stg_function_categories') }} fc
),

-- Join with dim_users to get user segments
activity_with_segments AS (
    SELECT
        ua.activity_month,
        ua.activity_bucket,
        ua.user_address,
        ua.transaction_hash,
        COALESCE(du.user_segment, 'Unknown') AS user_segment,
        COALESCE(du.user_tier, 'Unknown') AS user_tier
    FROM user_activity ua
    LEFT JOIN {{ ref('dim_users') }} du
        ON ua.user_address = du.user_address
),

-- Aggregate by month, bucket, segment, and tier
monthly_activity AS (
    SELECT
        activity_month,
        activity_bucket,
        user_segment,
        user_tier,
        COUNT(*) AS transaction_count,
        COUNT(DISTINCT user_address) AS unique_users
    FROM activity_with_segments
    GROUP BY 1, 2, 3, 4
)

SELECT
    activity_month,
    activity_bucket,
    user_segment,
    user_tier,
    transaction_count,
    unique_users,
    -- Percentage of total transactions for that month
    ROUND(
        100.0 * transaction_count / NULLIF(SUM(transaction_count) OVER (PARTITION BY activity_month), 0),
        2
    ) AS pct_of_month_txns
FROM monthly_activity
ORDER BY activity_month, user_segment, transaction_count DESC
