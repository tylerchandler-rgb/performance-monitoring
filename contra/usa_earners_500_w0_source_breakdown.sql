-- USA 500+ Ward Earners (W0) — Source Breakdown
-- Cohort:    US users grouped by signup date
-- Segment:   Users who earned 500+ wards within their first 7 days (W0)
-- Breakdown: Ward source by campaign_category from customer_cost

WITH cohort_users AS (
  SELECT
    customer_id
    ,DATE(signup_at) AS cohort_date
  FROM `weward-1548152103232.silver.customer_data`
  WHERE country = 'US'
    AND DATE(signup_at) >= DATE('2024-01-01')
    AND DATE(signup_at) <= DATE_SUB(CURRENT_DATE, INTERVAL 7 DAY)
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

SELECT
  cohort_month
  ,campaign_category
  ,users
  ,total_wards
  ,avg_wards_per_user
  ,SAFE_DIVIDE(total_wards, SUM(total_wards) OVER (PARTITION BY cohort_month)) AS pct_of_total_wards
  ,total_revenue_eur
  ,total_cashout_eur
  ,profit_eur
FROM (
  SELECT
    DATE_TRUNC(a.cohort_date, MONTH)                                AS cohort_month
    ,b.campaign_category
    ,COUNT(DISTINCT a.customer_id)                                  AS users
    ,SUM(b.amount_ward)                                             AS total_wards
    ,SAFE_DIVIDE(SUM(b.amount_ward), COUNT(DISTINCT a.customer_id)) AS avg_wards_per_user
    ,SUM(rev.revenue_eur)                                           AS total_revenue_eur
    ,SUM(co.cashout_eur)                                            AS total_cashout_eur
    ,SUM(COALESCE(rev.revenue_eur, 0)) + SUM(COALESCE(co.cashout_eur, 0)) AS profit_eur
  FROM w0_earners_500 AS a
  JOIN `weward-1548152103232.silver.customer_cost` AS b
    ON a.customer_id = b.customer_id
    AND DATE_DIFF(b.date, a.cohort_date, DAY) BETWEEN 0 AND 6
    AND b.amount_ward > 0
    AND b.date >= DATE('2024-01-01')
    AND b.date < CURRENT_DATE
  LEFT JOIN w0_revenue  AS rev ON a.customer_id = rev.customer_id
  LEFT JOIN w0_cashout  AS co  ON a.customer_id = co.customer_id
  GROUP BY 1, 2
)
ORDER BY cohort_month DESC, total_wards DESC
