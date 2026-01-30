-- Month 0 (bridge month) should always have activity
-- If a cohort has 0 active users in month 0, is possible but review if correct

select cohort_month, user_segment
from {{ ref('fct_cohort_retention') }}
where months_since_bridge = 0
  and active_users = 0
