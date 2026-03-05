-- USA 500+ Ward Earners (W0) — Profitability Distribution by Cohort Month
-- Cohort:    US users grouped by signup date
-- Segment:   Users who earned 500+ wards within their first 7 days (W0)
-- Question:  Of these high earners, what share are profitable in W0?

WITH cohort_users AS (
  SELECT
    customer_id
    ,DATE_TRUNC(DATE(signup_at), WEEK(MONDAY)) AS cohort_date
  FROM `weward-1548152103232.silver.customer_data`
  WHERE country = 'US'
    AND DATE(signup_at) >= DATE('2024-01-01')
    AND DATE_TRUNC(DATE(signup_at), WEEK(MONDAY)) < DATE_TRUNC(CURRENT_DATE, WEEK(MONDAY))
)

,suspicious_users AS (
  SELECT customer_id
  FROM (
    SELECT customer_id FROM `weward-1548152103232.user_activity.cheaters`
    UNION ALL
    SELECT customer_id FROM `weward-1548152103232.silver.customer_revenue`
    WHERE date >= DATE('2024-01-01')
      AND date < CURRENT_DATE
      AND campaign_category = 'offerwall'
      AND source <> 'adjoe'
    UNION ALL
    SELECT customer_id FROM `weward-1548152103232.user_activity.cheaters_revenue`
    UNION ALL
    SELECT customer_id FROM `weward-1548152103232.study.transaction_ow_survey_09_2025_to_02_2026`
    WHERE survey_institute = 'tapjoy_offerwall'
  )
  WHERE customer_id IS NOT NULL
)

,referral_users AS (
  SELECT customer_id
  FROM `weward-1548152103232.silver.sponsorship_transaction`
  WHERE sponsorship_type = 'godson'
    AND DATE(created_at) >= DATE('2024-01-01')
    AND DATE(created_at) < CURRENT_DATE
)

,w0_earners_500 AS (
  SELECT
    a.customer_id
    ,a.cohort_date
  FROM cohort_users AS a
  JOIN `weward-1548152103232.silver.customer_cost` AS b
    ON a.customer_id = b.customer_id
    AND DATE_DIFF(b.date, a.cohort_date, DAY) BETWEEN 0 AND 6
    AND b.amount_ward > 0
    AND b.date >= DATE('2024-01-01')
    AND b.date < CURRENT_DATE
  -- AND a.customer_id NOT IN (SELECT customer_id FROM suspicious_users)
  -- AND a.customer_id NOT IN (SELECT customer_id FROM referral_users)
  GROUP BY 1, 2
  HAVING SUM(b.amount_ward) > 500
)

,w0_revenue AS (
  SELECT
    a.customer_id
    ,SUM(cr.revenue_eur) AS revenue_eur
  FROM w0_earners_500 AS a
  JOIN `weward-1548152103232.silver.customer_revenue` AS cr
    ON a.customer_id = cr.customer_id
    AND DATE_DIFF(cr.date, a.cohort_date, DAY) BETWEEN 0 AND 6
    AND cr.date >= DATE('2024-01-01')
    AND cr.date < CURRENT_DATE
  GROUP BY 1
)

,w0_cashout AS (
  SELECT
    a.customer_id
    ,SUM(fc.cost_eur) AS cashout_eur
  FROM w0_earners_500 AS a
  JOIN `696845466639.costs.fct_cashout` AS fc
    ON a.customer_id = fc.customer_id
    AND DATE_DIFF(fc.created_at, a.cohort_date, DAY) BETWEEN 0 AND 6
    AND fc.created_at >= DATE('2024-01-01')
    AND fc.created_at < CURRENT_DATE
  GROUP BY 1
)

,user_profit AS (
  SELECT
    e.customer_id
    ,e.cohort_date
    ,COALESCE(rev.revenue_eur, 0) + COALESCE(co.cashout_eur, 0) AS profit_eur
  FROM w0_earners_500 AS e
  LEFT JOIN w0_revenue AS rev ON e.customer_id = rev.customer_id
  LEFT JOIN w0_cashout AS co  ON e.customer_id = co.customer_id
)

SELECT
  cohort_date                                                                 AS cohort_week
  ,COUNT(DISTINCT customer_id)                                                AS users
  ,COUNTIF(profit_eur > 0)                                                    AS profitable_users
  ,SAFE_DIVIDE(COUNTIF(profit_eur > 0), COUNT(DISTINCT customer_id))          AS pct_profitable
  ,SUM(profit_eur)                                                            AS total_profit_eur
  ,SAFE_DIVIDE(SUM(profit_eur), COUNT(DISTINCT customer_id))                  AS avg_profit_per_user
  ,SAFE_DIVIDE(SUM(IF(profit_eur > 0, profit_eur, NULL)), COUNTIF(profit_eur > 0)) AS avg_profit_per_profitable_user
FROM user_profit
GROUP BY 1
ORDER BY 1 DESC
