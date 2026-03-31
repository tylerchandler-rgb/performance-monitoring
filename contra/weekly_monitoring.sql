-- Daily Cohort Monitoring — Earning & Redemption Rates by Platform x Country
-- Cohort:      All users grouped by signup date (DATE(signup_at)), last 90 days
-- Segments:    platform_group x country_splitter (US vs Rest of World)
-- Earning:     wards accumulated from customer_cost (amount_ward > 0 only)
-- Redemption:  monetary ward cashouts from fct_cashout (cost_eur < 0)
-- Maturity:    window columns are NULL when the cohort has not yet aged past the window boundary

WITH cohort_users AS (
  SELECT
    customer_id
    ,DATE(signup_at) AS cohort_date
    ,CONCAT(
      CASE
        WHEN LOWER(platform) LIKE '%android%' THEN 'Android'
        WHEN LOWER(platform) LIKE '%ios%'     THEN 'iOS'
        WHEN platform IS NULL                 THEN 'Missing'
        ELSE 'Other'
      END,
      ' - ',
      CASE
        WHEN country = 'US' THEN 'US'
        ELSE 'ROW'
      END
    ) AS platform_country
  FROM `weward-1548152103232.silver.customer_data`
  WHERE DATE(signup_at) >= CURRENT_DATE - 365
    AND DATE(signup_at) < CURRENT_DATE
)

,user_redemptions AS (
  SELECT
    a.customer_id
    -- ===== D0–D6 (EUR redeemed) =====
    ,SUM(CASE WHEN DATE_DIFF(DATE(b.created_at), a.cohort_date, DAY) BETWEEN 0 AND 0  THEN COALESCE(-1 * b.cost_eur, 0) ELSE 0 END) AS rdm_eur_d0
    ,SUM(CASE WHEN DATE_DIFF(DATE(b.created_at), a.cohort_date, DAY) BETWEEN 0 AND 1  THEN COALESCE(-1 * b.cost_eur, 0) ELSE 0 END) AS rdm_eur_d1
    ,SUM(CASE WHEN DATE_DIFF(DATE(b.created_at), a.cohort_date, DAY) BETWEEN 0 AND 2  THEN COALESCE(-1 * b.cost_eur, 0) ELSE 0 END) AS rdm_eur_d2
    ,SUM(CASE WHEN DATE_DIFF(DATE(b.created_at), a.cohort_date, DAY) BETWEEN 0 AND 3  THEN COALESCE(-1 * b.cost_eur, 0) ELSE 0 END) AS rdm_eur_d3
    ,SUM(CASE WHEN DATE_DIFF(DATE(b.created_at), a.cohort_date, DAY) BETWEEN 0 AND 4  THEN COALESCE(-1 * b.cost_eur, 0) ELSE 0 END) AS rdm_eur_d4
    ,SUM(CASE WHEN DATE_DIFF(DATE(b.created_at), a.cohort_date, DAY) BETWEEN 0 AND 5  THEN COALESCE(-1 * b.cost_eur, 0) ELSE 0 END) AS rdm_eur_d5
    ,SUM(CASE WHEN DATE_DIFF(DATE(b.created_at), a.cohort_date, DAY) BETWEEN 0 AND 6  THEN COALESCE(-1 * b.cost_eur, 0) ELSE 0 END) AS rdm_eur_d6
    -- ===== D0–D6 (wards redeemed) =====
    ,SUM(CASE WHEN DATE_DIFF(DATE(b.created_at), a.cohort_date, DAY) BETWEEN 0 AND 0  THEN COALESCE(-1 * b.amount, 0) ELSE 0 END) AS rdm_wards_d0
    ,SUM(CASE WHEN DATE_DIFF(DATE(b.created_at), a.cohort_date, DAY) BETWEEN 0 AND 1  THEN COALESCE(-1 * b.amount, 0) ELSE 0 END) AS rdm_wards_d1
    ,SUM(CASE WHEN DATE_DIFF(DATE(b.created_at), a.cohort_date, DAY) BETWEEN 0 AND 2  THEN COALESCE(-1 * b.amount, 0) ELSE 0 END) AS rdm_wards_d2
    ,SUM(CASE WHEN DATE_DIFF(DATE(b.created_at), a.cohort_date, DAY) BETWEEN 0 AND 3  THEN COALESCE(-1 * b.amount, 0) ELSE 0 END) AS rdm_wards_d3
    ,SUM(CASE WHEN DATE_DIFF(DATE(b.created_at), a.cohort_date, DAY) BETWEEN 0 AND 4  THEN COALESCE(-1 * b.amount, 0) ELSE 0 END) AS rdm_wards_d4
    ,SUM(CASE WHEN DATE_DIFF(DATE(b.created_at), a.cohort_date, DAY) BETWEEN 0 AND 5  THEN COALESCE(-1 * b.amount, 0) ELSE 0 END) AS rdm_wards_d5
    ,SUM(CASE WHEN DATE_DIFF(DATE(b.created_at), a.cohort_date, DAY) BETWEEN 0 AND 6  THEN COALESCE(-1 * b.amount, 0) ELSE 0 END) AS rdm_wards_d6
    -- ===== D0–D6 (has_redeemed — 1 if any EUR cashout within window) =====
    ,IF(SUM(CASE WHEN DATE_DIFF(DATE(b.created_at), a.cohort_date, DAY) BETWEEN 0 AND 0  THEN COALESCE(-1 * b.cost_eur, 0) ELSE 0 END) > 0, 1, 0) AS has_redeemed_d0
    ,IF(SUM(CASE WHEN DATE_DIFF(DATE(b.created_at), a.cohort_date, DAY) BETWEEN 0 AND 1  THEN COALESCE(-1 * b.cost_eur, 0) ELSE 0 END) > 0, 1, 0) AS has_redeemed_d1
    ,IF(SUM(CASE WHEN DATE_DIFF(DATE(b.created_at), a.cohort_date, DAY) BETWEEN 0 AND 2  THEN COALESCE(-1 * b.cost_eur, 0) ELSE 0 END) > 0, 1, 0) AS has_redeemed_d2
    ,IF(SUM(CASE WHEN DATE_DIFF(DATE(b.created_at), a.cohort_date, DAY) BETWEEN 0 AND 3  THEN COALESCE(-1 * b.cost_eur, 0) ELSE 0 END) > 0, 1, 0) AS has_redeemed_d3
    ,IF(SUM(CASE WHEN DATE_DIFF(DATE(b.created_at), a.cohort_date, DAY) BETWEEN 0 AND 4  THEN COALESCE(-1 * b.cost_eur, 0) ELSE 0 END) > 0, 1, 0) AS has_redeemed_d4
    ,IF(SUM(CASE WHEN DATE_DIFF(DATE(b.created_at), a.cohort_date, DAY) BETWEEN 0 AND 5  THEN COALESCE(-1 * b.cost_eur, 0) ELSE 0 END) > 0, 1, 0) AS has_redeemed_d5
    ,IF(SUM(CASE WHEN DATE_DIFF(DATE(b.created_at), a.cohort_date, DAY) BETWEEN 0 AND 6  THEN COALESCE(-1 * b.cost_eur, 0) ELSE 0 END) > 0, 1, 0) AS has_redeemed_d6
    -- ===== W0–W4 (EUR redeemed) =====
    ,SUM(CASE WHEN DATE_DIFF(DATE(b.created_at), a.cohort_date, DAY) BETWEEN 0 AND 6   THEN COALESCE(-1 * b.cost_eur, 0) ELSE 0 END) AS rdm_eur_w0
    ,SUM(CASE WHEN DATE_DIFF(DATE(b.created_at), a.cohort_date, DAY) BETWEEN 0 AND 13  THEN COALESCE(-1 * b.cost_eur, 0) ELSE 0 END) AS rdm_eur_w1
    ,SUM(CASE WHEN DATE_DIFF(DATE(b.created_at), a.cohort_date, DAY) BETWEEN 0 AND 20  THEN COALESCE(-1 * b.cost_eur, 0) ELSE 0 END) AS rdm_eur_w2
    ,SUM(CASE WHEN DATE_DIFF(DATE(b.created_at), a.cohort_date, DAY) BETWEEN 0 AND 27  THEN COALESCE(-1 * b.cost_eur, 0) ELSE 0 END) AS rdm_eur_w3
    ,SUM(CASE WHEN DATE_DIFF(DATE(b.created_at), a.cohort_date, DAY) BETWEEN 0 AND 34  THEN COALESCE(-1 * b.cost_eur, 0) ELSE 0 END) AS rdm_eur_w4
    -- ===== W0–W4 (wards redeemed) =====
    ,SUM(CASE WHEN DATE_DIFF(DATE(b.created_at), a.cohort_date, DAY) BETWEEN 0 AND 6   THEN COALESCE(-1 * b.amount, 0) ELSE 0 END) AS rdm_wards_w0
    ,SUM(CASE WHEN DATE_DIFF(DATE(b.created_at), a.cohort_date, DAY) BETWEEN 0 AND 13  THEN COALESCE(-1 * b.amount, 0) ELSE 0 END) AS rdm_wards_w1
    ,SUM(CASE WHEN DATE_DIFF(DATE(b.created_at), a.cohort_date, DAY) BETWEEN 0 AND 20  THEN COALESCE(-1 * b.amount, 0) ELSE 0 END) AS rdm_wards_w2
    ,SUM(CASE WHEN DATE_DIFF(DATE(b.created_at), a.cohort_date, DAY) BETWEEN 0 AND 27  THEN COALESCE(-1 * b.amount, 0) ELSE 0 END) AS rdm_wards_w3
    ,SUM(CASE WHEN DATE_DIFF(DATE(b.created_at), a.cohort_date, DAY) BETWEEN 0 AND 34  THEN COALESCE(-1 * b.amount, 0) ELSE 0 END) AS rdm_wards_w4
    -- ===== W0–W4 (has_redeemed) =====
    ,IF(SUM(CASE WHEN DATE_DIFF(DATE(b.created_at), a.cohort_date, DAY) BETWEEN 0 AND 6   THEN COALESCE(-1 * b.cost_eur, 0) ELSE 0 END) > 0, 1, 0) AS has_redeemed_w0
    ,IF(SUM(CASE WHEN DATE_DIFF(DATE(b.created_at), a.cohort_date, DAY) BETWEEN 0 AND 13  THEN COALESCE(-1 * b.cost_eur, 0) ELSE 0 END) > 0, 1, 0) AS has_redeemed_w1
    ,IF(SUM(CASE WHEN DATE_DIFF(DATE(b.created_at), a.cohort_date, DAY) BETWEEN 0 AND 20  THEN COALESCE(-1 * b.cost_eur, 0) ELSE 0 END) > 0, 1, 0) AS has_redeemed_w2
    ,IF(SUM(CASE WHEN DATE_DIFF(DATE(b.created_at), a.cohort_date, DAY) BETWEEN 0 AND 27  THEN COALESCE(-1 * b.cost_eur, 0) ELSE 0 END) > 0, 1, 0) AS has_redeemed_w3
    ,IF(SUM(CASE WHEN DATE_DIFF(DATE(b.created_at), a.cohort_date, DAY) BETWEEN 0 AND 34  THEN COALESCE(-1 * b.cost_eur, 0) ELSE 0 END) > 0, 1, 0) AS has_redeemed_w4
  FROM cohort_users AS a
  LEFT JOIN `696845466639.costs.fct_cashout` AS b
    ON a.customer_id = b.customer_id
    AND DATE(b.created_at) >= a.cohort_date
    AND DATE(b.created_at) >= DATE('2024-01-01')
    AND DATE(b.created_at) < CURRENT_DATE
  GROUP BY 1
)

,user_earnings AS (
  SELECT
    a.customer_id
    -- ===== D0–D6 (wards earned) =====
    ,SUM(CASE WHEN DATE_DIFF(b.date, a.cohort_date, DAY) BETWEEN 0 AND 0  THEN COALESCE(b.amount_ward, 0) ELSE 0 END) AS earned_wards_d0
    ,SUM(CASE WHEN DATE_DIFF(b.date, a.cohort_date, DAY) BETWEEN 0 AND 1  THEN COALESCE(b.amount_ward, 0) ELSE 0 END) AS earned_wards_d1
    ,SUM(CASE WHEN DATE_DIFF(b.date, a.cohort_date, DAY) BETWEEN 0 AND 2  THEN COALESCE(b.amount_ward, 0) ELSE 0 END) AS earned_wards_d2
    ,SUM(CASE WHEN DATE_DIFF(b.date, a.cohort_date, DAY) BETWEEN 0 AND 3  THEN COALESCE(b.amount_ward, 0) ELSE 0 END) AS earned_wards_d3
    ,SUM(CASE WHEN DATE_DIFF(b.date, a.cohort_date, DAY) BETWEEN 0 AND 4  THEN COALESCE(b.amount_ward, 0) ELSE 0 END) AS earned_wards_d4
    ,SUM(CASE WHEN DATE_DIFF(b.date, a.cohort_date, DAY) BETWEEN 0 AND 5  THEN COALESCE(b.amount_ward, 0) ELSE 0 END) AS earned_wards_d5
    ,SUM(CASE WHEN DATE_DIFF(b.date, a.cohort_date, DAY) BETWEEN 0 AND 6  THEN COALESCE(b.amount_ward, 0) ELSE 0 END) AS earned_wards_d6
    -- ===== D0–D6 (has_earned — 1 if any wards earned within window) =====
    ,IF(SUM(CASE WHEN DATE_DIFF(b.date, a.cohort_date, DAY) BETWEEN 0 AND 0  THEN COALESCE(b.amount_ward, 0) ELSE 0 END) > 0, 1, 0) AS has_earned_d0
    ,IF(SUM(CASE WHEN DATE_DIFF(b.date, a.cohort_date, DAY) BETWEEN 0 AND 1  THEN COALESCE(b.amount_ward, 0) ELSE 0 END) > 0, 1, 0) AS has_earned_d1
    ,IF(SUM(CASE WHEN DATE_DIFF(b.date, a.cohort_date, DAY) BETWEEN 0 AND 2  THEN COALESCE(b.amount_ward, 0) ELSE 0 END) > 0, 1, 0) AS has_earned_d2
    ,IF(SUM(CASE WHEN DATE_DIFF(b.date, a.cohort_date, DAY) BETWEEN 0 AND 3  THEN COALESCE(b.amount_ward, 0) ELSE 0 END) > 0, 1, 0) AS has_earned_d3
    ,IF(SUM(CASE WHEN DATE_DIFF(b.date, a.cohort_date, DAY) BETWEEN 0 AND 4  THEN COALESCE(b.amount_ward, 0) ELSE 0 END) > 0, 1, 0) AS has_earned_d4
    ,IF(SUM(CASE WHEN DATE_DIFF(b.date, a.cohort_date, DAY) BETWEEN 0 AND 5  THEN COALESCE(b.amount_ward, 0) ELSE 0 END) > 0, 1, 0) AS has_earned_d5
    ,IF(SUM(CASE WHEN DATE_DIFF(b.date, a.cohort_date, DAY) BETWEEN 0 AND 6  THEN COALESCE(b.amount_ward, 0) ELSE 0 END) > 0, 1, 0) AS has_earned_d6
    -- ===== W0–W4 (wards earned) =====
    ,SUM(CASE WHEN DATE_DIFF(b.date, a.cohort_date, DAY) BETWEEN 0 AND 6   THEN COALESCE(b.amount_ward, 0) ELSE 0 END) AS earned_wards_w0
    ,SUM(CASE WHEN DATE_DIFF(b.date, a.cohort_date, DAY) BETWEEN 0 AND 13  THEN COALESCE(b.amount_ward, 0) ELSE 0 END) AS earned_wards_w1
    ,SUM(CASE WHEN DATE_DIFF(b.date, a.cohort_date, DAY) BETWEEN 0 AND 20  THEN COALESCE(b.amount_ward, 0) ELSE 0 END) AS earned_wards_w2
    ,SUM(CASE WHEN DATE_DIFF(b.date, a.cohort_date, DAY) BETWEEN 0 AND 27  THEN COALESCE(b.amount_ward, 0) ELSE 0 END) AS earned_wards_w3
    ,SUM(CASE WHEN DATE_DIFF(b.date, a.cohort_date, DAY) BETWEEN 0 AND 34  THEN COALESCE(b.amount_ward, 0) ELSE 0 END) AS earned_wards_w4
    -- ===== W0–W4 (has_earned) =====
    ,IF(SUM(CASE WHEN DATE_DIFF(b.date, a.cohort_date, DAY) BETWEEN 0 AND 6   THEN COALESCE(b.amount_ward, 0) ELSE 0 END) > 0, 1, 0) AS has_earned_w0
    ,IF(SUM(CASE WHEN DATE_DIFF(b.date, a.cohort_date, DAY) BETWEEN 0 AND 13  THEN COALESCE(b.amount_ward, 0) ELSE 0 END) > 0, 1, 0) AS has_earned_w1
    ,IF(SUM(CASE WHEN DATE_DIFF(b.date, a.cohort_date, DAY) BETWEEN 0 AND 20  THEN COALESCE(b.amount_ward, 0) ELSE 0 END) > 0, 1, 0) AS has_earned_w2
    ,IF(SUM(CASE WHEN DATE_DIFF(b.date, a.cohort_date, DAY) BETWEEN 0 AND 27  THEN COALESCE(b.amount_ward, 0) ELSE 0 END) > 0, 1, 0) AS has_earned_w3
    ,IF(SUM(CASE WHEN DATE_DIFF(b.date, a.cohort_date, DAY) BETWEEN 0 AND 34  THEN COALESCE(b.amount_ward, 0) ELSE 0 END) > 0, 1, 0) AS has_earned_w4
    -- ===== D0–D6 (earners over 500 wards) =====
    ,IF(SUM(CASE WHEN DATE_DIFF(b.date, a.cohort_date, DAY) BETWEEN 0 AND 0  THEN COALESCE(b.amount_ward, 0) ELSE 0 END) > 500,  1, 0) AS earners_500_d0
    ,IF(SUM(CASE WHEN DATE_DIFF(b.date, a.cohort_date, DAY) BETWEEN 0 AND 1  THEN COALESCE(b.amount_ward, 0) ELSE 0 END) > 500,  1, 0) AS earners_500_d1
    ,IF(SUM(CASE WHEN DATE_DIFF(b.date, a.cohort_date, DAY) BETWEEN 0 AND 2  THEN COALESCE(b.amount_ward, 0) ELSE 0 END) > 500,  1, 0) AS earners_500_d2
    ,IF(SUM(CASE WHEN DATE_DIFF(b.date, a.cohort_date, DAY) BETWEEN 0 AND 3  THEN COALESCE(b.amount_ward, 0) ELSE 0 END) > 500,  1, 0) AS earners_500_d3
    ,IF(SUM(CASE WHEN DATE_DIFF(b.date, a.cohort_date, DAY) BETWEEN 0 AND 4  THEN COALESCE(b.amount_ward, 0) ELSE 0 END) > 500,  1, 0) AS earners_500_d4
    ,IF(SUM(CASE WHEN DATE_DIFF(b.date, a.cohort_date, DAY) BETWEEN 0 AND 5  THEN COALESCE(b.amount_ward, 0) ELSE 0 END) > 500,  1, 0) AS earners_500_d5
    ,IF(SUM(CASE WHEN DATE_DIFF(b.date, a.cohort_date, DAY) BETWEEN 0 AND 6  THEN COALESCE(b.amount_ward, 0) ELSE 0 END) > 500,  1, 0) AS earners_500_d6
    -- ===== D0–D6 (earners over 1000 wards) =====
    ,IF(SUM(CASE WHEN DATE_DIFF(b.date, a.cohort_date, DAY) BETWEEN 0 AND 0  THEN COALESCE(b.amount_ward, 0) ELSE 0 END) > 1000, 1, 0) AS earners_1000_d0
    ,IF(SUM(CASE WHEN DATE_DIFF(b.date, a.cohort_date, DAY) BETWEEN 0 AND 1  THEN COALESCE(b.amount_ward, 0) ELSE 0 END) > 1000, 1, 0) AS earners_1000_d1
    ,IF(SUM(CASE WHEN DATE_DIFF(b.date, a.cohort_date, DAY) BETWEEN 0 AND 2  THEN COALESCE(b.amount_ward, 0) ELSE 0 END) > 1000, 1, 0) AS earners_1000_d2
    ,IF(SUM(CASE WHEN DATE_DIFF(b.date, a.cohort_date, DAY) BETWEEN 0 AND 3  THEN COALESCE(b.amount_ward, 0) ELSE 0 END) > 1000, 1, 0) AS earners_1000_d3
    ,IF(SUM(CASE WHEN DATE_DIFF(b.date, a.cohort_date, DAY) BETWEEN 0 AND 4  THEN COALESCE(b.amount_ward, 0) ELSE 0 END) > 1000, 1, 0) AS earners_1000_d4
    ,IF(SUM(CASE WHEN DATE_DIFF(b.date, a.cohort_date, DAY) BETWEEN 0 AND 5  THEN COALESCE(b.amount_ward, 0) ELSE 0 END) > 1000, 1, 0) AS earners_1000_d5
    ,IF(SUM(CASE WHEN DATE_DIFF(b.date, a.cohort_date, DAY) BETWEEN 0 AND 6  THEN COALESCE(b.amount_ward, 0) ELSE 0 END) > 1000, 1, 0) AS earners_1000_d6
    -- ===== W0–W4 (earners over 500 wards) =====
    ,IF(SUM(CASE WHEN DATE_DIFF(b.date, a.cohort_date, DAY) BETWEEN 0 AND 6   THEN COALESCE(b.amount_ward, 0) ELSE 0 END) > 500,  1, 0) AS earners_500_w0
    ,IF(SUM(CASE WHEN DATE_DIFF(b.date, a.cohort_date, DAY) BETWEEN 0 AND 13  THEN COALESCE(b.amount_ward, 0) ELSE 0 END) > 500,  1, 0) AS earners_500_w1
    ,IF(SUM(CASE WHEN DATE_DIFF(b.date, a.cohort_date, DAY) BETWEEN 0 AND 20  THEN COALESCE(b.amount_ward, 0) ELSE 0 END) > 500,  1, 0) AS earners_500_w2
    ,IF(SUM(CASE WHEN DATE_DIFF(b.date, a.cohort_date, DAY) BETWEEN 0 AND 27  THEN COALESCE(b.amount_ward, 0) ELSE 0 END) > 500,  1, 0) AS earners_500_w3
    ,IF(SUM(CASE WHEN DATE_DIFF(b.date, a.cohort_date, DAY) BETWEEN 0 AND 34  THEN COALESCE(b.amount_ward, 0) ELSE 0 END) > 500,  1, 0) AS earners_500_w4
    -- ===== W0–W4 (earners over 1000 wards) =====
    ,IF(SUM(CASE WHEN DATE_DIFF(b.date, a.cohort_date, DAY) BETWEEN 0 AND 6   THEN COALESCE(b.amount_ward, 0) ELSE 0 END) > 1000, 1, 0) AS earners_1000_w0
    ,IF(SUM(CASE WHEN DATE_DIFF(b.date, a.cohort_date, DAY) BETWEEN 0 AND 13  THEN COALESCE(b.amount_ward, 0) ELSE 0 END) > 1000, 1, 0) AS earners_1000_w1
    ,IF(SUM(CASE WHEN DATE_DIFF(b.date, a.cohort_date, DAY) BETWEEN 0 AND 20  THEN COALESCE(b.amount_ward, 0) ELSE 0 END) > 1000, 1, 0) AS earners_1000_w2
    ,IF(SUM(CASE WHEN DATE_DIFF(b.date, a.cohort_date, DAY) BETWEEN 0 AND 27  THEN COALESCE(b.amount_ward, 0) ELSE 0 END) > 1000, 1, 0) AS earners_1000_w3
    ,IF(SUM(CASE WHEN DATE_DIFF(b.date, a.cohort_date, DAY) BETWEEN 0 AND 34  THEN COALESCE(b.amount_ward, 0) ELSE 0 END) > 1000, 1, 0) AS earners_1000_w4
  FROM cohort_users AS a
  LEFT JOIN `weward-1548152103232.silver.customer_cost` AS b
    ON a.customer_id = b.customer_id
    AND b.date >= a.cohort_date
    AND b.date >= DATE('2024-01-01')
    AND b.date < CURRENT_DATE
    AND b.amount_ward > 0  -- earning events only; excludes redemptions and zero-value participation rows
    AND b.campaign_category NOT IN ('bank_transfer', 'redeem')  -- belt-and-suspenders: exclude removal categories even if amount_ward is anomalously positive
  GROUP BY 1
)

,user_cashouts AS (
  SELECT
    a.customer_id
    -- ===== D0–D6 (delivered) =====
    ,SUM(CASE WHEN DATE_DIFF(DATE(b.created_at), a.cohort_date, DAY) BETWEEN 0 AND 0  AND b.status = 'delivered' THEN b.price ELSE NULL END) AS cashout_delivered_wards_d0
    ,IF(SUM(CASE WHEN DATE_DIFF(DATE(b.created_at), a.cohort_date, DAY) BETWEEN 0 AND 0  AND b.status = 'delivered' THEN 1 ELSE 0 END) > 0, 1, 0) AS cashout_delivered_users_d0
    ,SUM(CASE WHEN DATE_DIFF(DATE(b.created_at), a.cohort_date, DAY) BETWEEN 0 AND 1  AND b.status = 'delivered' THEN b.price ELSE NULL END) AS cashout_delivered_wards_d1
    ,IF(SUM(CASE WHEN DATE_DIFF(DATE(b.created_at), a.cohort_date, DAY) BETWEEN 0 AND 1  AND b.status = 'delivered' THEN 1 ELSE 0 END) > 0, 1, 0) AS cashout_delivered_users_d1
    ,SUM(CASE WHEN DATE_DIFF(DATE(b.created_at), a.cohort_date, DAY) BETWEEN 0 AND 2  AND b.status = 'delivered' THEN b.price ELSE NULL END) AS cashout_delivered_wards_d2
    ,IF(SUM(CASE WHEN DATE_DIFF(DATE(b.created_at), a.cohort_date, DAY) BETWEEN 0 AND 2  AND b.status = 'delivered' THEN 1 ELSE 0 END) > 0, 1, 0) AS cashout_delivered_users_d2
    ,SUM(CASE WHEN DATE_DIFF(DATE(b.created_at), a.cohort_date, DAY) BETWEEN 0 AND 3  AND b.status = 'delivered' THEN b.price ELSE NULL END) AS cashout_delivered_wards_d3
    ,IF(SUM(CASE WHEN DATE_DIFF(DATE(b.created_at), a.cohort_date, DAY) BETWEEN 0 AND 3  AND b.status = 'delivered' THEN 1 ELSE 0 END) > 0, 1, 0) AS cashout_delivered_users_d3
    ,SUM(CASE WHEN DATE_DIFF(DATE(b.created_at), a.cohort_date, DAY) BETWEEN 0 AND 4  AND b.status = 'delivered' THEN b.price ELSE NULL END) AS cashout_delivered_wards_d4
    ,IF(SUM(CASE WHEN DATE_DIFF(DATE(b.created_at), a.cohort_date, DAY) BETWEEN 0 AND 4  AND b.status = 'delivered' THEN 1 ELSE 0 END) > 0, 1, 0) AS cashout_delivered_users_d4
    ,SUM(CASE WHEN DATE_DIFF(DATE(b.created_at), a.cohort_date, DAY) BETWEEN 0 AND 5  AND b.status = 'delivered' THEN b.price ELSE NULL END) AS cashout_delivered_wards_d5
    ,IF(SUM(CASE WHEN DATE_DIFF(DATE(b.created_at), a.cohort_date, DAY) BETWEEN 0 AND 5  AND b.status = 'delivered' THEN 1 ELSE 0 END) > 0, 1, 0) AS cashout_delivered_users_d5
    ,SUM(CASE WHEN DATE_DIFF(DATE(b.created_at), a.cohort_date, DAY) BETWEEN 0 AND 6  AND b.status = 'delivered' THEN b.price ELSE NULL END) AS cashout_delivered_wards_d6
    ,IF(SUM(CASE WHEN DATE_DIFF(DATE(b.created_at), a.cohort_date, DAY) BETWEEN 0 AND 6  AND b.status = 'delivered' THEN 1 ELSE 0 END) > 0, 1, 0) AS cashout_delivered_users_d6
    -- ===== D0–D6 (pending) =====
    ,SUM(CASE WHEN DATE_DIFF(DATE(b.created_at), a.cohort_date, DAY) BETWEEN 0 AND 0  AND b.status = 'pending' THEN b.price ELSE NULL END) AS cashout_pending_wards_d0
    ,IF(SUM(CASE WHEN DATE_DIFF(DATE(b.created_at), a.cohort_date, DAY) BETWEEN 0 AND 0  AND b.status = 'pending' THEN 1 ELSE 0 END) > 0, 1, 0) AS cashout_pending_users_d0
    ,SUM(CASE WHEN DATE_DIFF(DATE(b.created_at), a.cohort_date, DAY) BETWEEN 0 AND 1  AND b.status = 'pending' THEN b.price ELSE NULL END) AS cashout_pending_wards_d1
    ,IF(SUM(CASE WHEN DATE_DIFF(DATE(b.created_at), a.cohort_date, DAY) BETWEEN 0 AND 1  AND b.status = 'pending' THEN 1 ELSE 0 END) > 0, 1, 0) AS cashout_pending_users_d1
    ,SUM(CASE WHEN DATE_DIFF(DATE(b.created_at), a.cohort_date, DAY) BETWEEN 0 AND 2  AND b.status = 'pending' THEN b.price ELSE NULL END) AS cashout_pending_wards_d2
    ,IF(SUM(CASE WHEN DATE_DIFF(DATE(b.created_at), a.cohort_date, DAY) BETWEEN 0 AND 2  AND b.status = 'pending' THEN 1 ELSE 0 END) > 0, 1, 0) AS cashout_pending_users_d2
    ,SUM(CASE WHEN DATE_DIFF(DATE(b.created_at), a.cohort_date, DAY) BETWEEN 0 AND 3  AND b.status = 'pending' THEN b.price ELSE NULL END) AS cashout_pending_wards_d3
    ,IF(SUM(CASE WHEN DATE_DIFF(DATE(b.created_at), a.cohort_date, DAY) BETWEEN 0 AND 3  AND b.status = 'pending' THEN 1 ELSE 0 END) > 0, 1, 0) AS cashout_pending_users_d3
    ,SUM(CASE WHEN DATE_DIFF(DATE(b.created_at), a.cohort_date, DAY) BETWEEN 0 AND 4  AND b.status = 'pending' THEN b.price ELSE NULL END) AS cashout_pending_wards_d4
    ,IF(SUM(CASE WHEN DATE_DIFF(DATE(b.created_at), a.cohort_date, DAY) BETWEEN 0 AND 4  AND b.status = 'pending' THEN 1 ELSE 0 END) > 0, 1, 0) AS cashout_pending_users_d4
    ,SUM(CASE WHEN DATE_DIFF(DATE(b.created_at), a.cohort_date, DAY) BETWEEN 0 AND 5  AND b.status = 'pending' THEN b.price ELSE NULL END) AS cashout_pending_wards_d5
    ,IF(SUM(CASE WHEN DATE_DIFF(DATE(b.created_at), a.cohort_date, DAY) BETWEEN 0 AND 5  AND b.status = 'pending' THEN 1 ELSE 0 END) > 0, 1, 0) AS cashout_pending_users_d5
    ,SUM(CASE WHEN DATE_DIFF(DATE(b.created_at), a.cohort_date, DAY) BETWEEN 0 AND 6  AND b.status = 'pending' THEN b.price ELSE NULL END) AS cashout_pending_wards_d6
    ,IF(SUM(CASE WHEN DATE_DIFF(DATE(b.created_at), a.cohort_date, DAY) BETWEEN 0 AND 6  AND b.status = 'pending' THEN 1 ELSE 0 END) > 0, 1, 0) AS cashout_pending_users_d6
    -- ===== D0–D6 (attempted = all except refunded) =====
    ,SUM(CASE WHEN DATE_DIFF(DATE(b.created_at), a.cohort_date, DAY) BETWEEN 0 AND 0  AND b.status != 'refunded' THEN b.price ELSE NULL END) AS cashout_attempted_wards_d0
    ,IF(SUM(CASE WHEN DATE_DIFF(DATE(b.created_at), a.cohort_date, DAY) BETWEEN 0 AND 0  AND b.status != 'refunded' THEN 1 ELSE 0 END) > 0, 1, 0) AS cashout_attempted_users_d0
    ,SUM(CASE WHEN DATE_DIFF(DATE(b.created_at), a.cohort_date, DAY) BETWEEN 0 AND 1  AND b.status != 'refunded' THEN b.price ELSE NULL END) AS cashout_attempted_wards_d1
    ,IF(SUM(CASE WHEN DATE_DIFF(DATE(b.created_at), a.cohort_date, DAY) BETWEEN 0 AND 1  AND b.status != 'refunded' THEN 1 ELSE 0 END) > 0, 1, 0) AS cashout_attempted_users_d1
    ,SUM(CASE WHEN DATE_DIFF(DATE(b.created_at), a.cohort_date, DAY) BETWEEN 0 AND 2  AND b.status != 'refunded' THEN b.price ELSE NULL END) AS cashout_attempted_wards_d2
    ,IF(SUM(CASE WHEN DATE_DIFF(DATE(b.created_at), a.cohort_date, DAY) BETWEEN 0 AND 2  AND b.status != 'refunded' THEN 1 ELSE 0 END) > 0, 1, 0) AS cashout_attempted_users_d2
    ,SUM(CASE WHEN DATE_DIFF(DATE(b.created_at), a.cohort_date, DAY) BETWEEN 0 AND 3  AND b.status != 'refunded' THEN b.price ELSE NULL END) AS cashout_attempted_wards_d3
    ,IF(SUM(CASE WHEN DATE_DIFF(DATE(b.created_at), a.cohort_date, DAY) BETWEEN 0 AND 3  AND b.status != 'refunded' THEN 1 ELSE 0 END) > 0, 1, 0) AS cashout_attempted_users_d3
    ,SUM(CASE WHEN DATE_DIFF(DATE(b.created_at), a.cohort_date, DAY) BETWEEN 0 AND 4  AND b.status != 'refunded' THEN b.price ELSE NULL END) AS cashout_attempted_wards_d4
    ,IF(SUM(CASE WHEN DATE_DIFF(DATE(b.created_at), a.cohort_date, DAY) BETWEEN 0 AND 4  AND b.status != 'refunded' THEN 1 ELSE 0 END) > 0, 1, 0) AS cashout_attempted_users_d4
    ,SUM(CASE WHEN DATE_DIFF(DATE(b.created_at), a.cohort_date, DAY) BETWEEN 0 AND 5  AND b.status != 'refunded' THEN b.price ELSE NULL END) AS cashout_attempted_wards_d5
    ,IF(SUM(CASE WHEN DATE_DIFF(DATE(b.created_at), a.cohort_date, DAY) BETWEEN 0 AND 5  AND b.status != 'refunded' THEN 1 ELSE 0 END) > 0, 1, 0) AS cashout_attempted_users_d5
    ,SUM(CASE WHEN DATE_DIFF(DATE(b.created_at), a.cohort_date, DAY) BETWEEN 0 AND 6  AND b.status != 'refunded' THEN b.price ELSE NULL END) AS cashout_attempted_wards_d6
    ,IF(SUM(CASE WHEN DATE_DIFF(DATE(b.created_at), a.cohort_date, DAY) BETWEEN 0 AND 6  AND b.status != 'refunded' THEN 1 ELSE 0 END) > 0, 1, 0) AS cashout_attempted_users_d6
    -- ===== W0–W4 (delivered) =====
    ,SUM(CASE WHEN DATE_DIFF(DATE(b.created_at), a.cohort_date, DAY) BETWEEN 0 AND 6   AND b.status = 'delivered' THEN b.price ELSE NULL END) AS cashout_delivered_wards_w0
    ,IF(SUM(CASE WHEN DATE_DIFF(DATE(b.created_at), a.cohort_date, DAY) BETWEEN 0 AND 6   AND b.status = 'delivered' THEN 1 ELSE 0 END) > 0, 1, 0) AS cashout_delivered_users_w0
    ,SUM(CASE WHEN DATE_DIFF(DATE(b.created_at), a.cohort_date, DAY) BETWEEN 0 AND 13  AND b.status = 'delivered' THEN b.price ELSE NULL END) AS cashout_delivered_wards_w1
    ,IF(SUM(CASE WHEN DATE_DIFF(DATE(b.created_at), a.cohort_date, DAY) BETWEEN 0 AND 13  AND b.status = 'delivered' THEN 1 ELSE 0 END) > 0, 1, 0) AS cashout_delivered_users_w1
    ,SUM(CASE WHEN DATE_DIFF(DATE(b.created_at), a.cohort_date, DAY) BETWEEN 0 AND 20  AND b.status = 'delivered' THEN b.price ELSE NULL END) AS cashout_delivered_wards_w2
    ,IF(SUM(CASE WHEN DATE_DIFF(DATE(b.created_at), a.cohort_date, DAY) BETWEEN 0 AND 20  AND b.status = 'delivered' THEN 1 ELSE 0 END) > 0, 1, 0) AS cashout_delivered_users_w2
    ,SUM(CASE WHEN DATE_DIFF(DATE(b.created_at), a.cohort_date, DAY) BETWEEN 0 AND 27  AND b.status = 'delivered' THEN b.price ELSE NULL END) AS cashout_delivered_wards_w3
    ,IF(SUM(CASE WHEN DATE_DIFF(DATE(b.created_at), a.cohort_date, DAY) BETWEEN 0 AND 27  AND b.status = 'delivered' THEN 1 ELSE 0 END) > 0, 1, 0) AS cashout_delivered_users_w3
    ,SUM(CASE WHEN DATE_DIFF(DATE(b.created_at), a.cohort_date, DAY) BETWEEN 0 AND 34  AND b.status = 'delivered' THEN b.price ELSE NULL END) AS cashout_delivered_wards_w4
    ,IF(SUM(CASE WHEN DATE_DIFF(DATE(b.created_at), a.cohort_date, DAY) BETWEEN 0 AND 34  AND b.status = 'delivered' THEN 1 ELSE 0 END) > 0, 1, 0) AS cashout_delivered_users_w4
    -- ===== W0–W4 (pending) =====
    ,SUM(CASE WHEN DATE_DIFF(DATE(b.created_at), a.cohort_date, DAY) BETWEEN 0 AND 6   AND b.status = 'pending' THEN b.price ELSE NULL END) AS cashout_pending_wards_w0
    ,IF(SUM(CASE WHEN DATE_DIFF(DATE(b.created_at), a.cohort_date, DAY) BETWEEN 0 AND 6   AND b.status = 'pending' THEN 1 ELSE 0 END) > 0, 1, 0) AS cashout_pending_users_w0
    ,SUM(CASE WHEN DATE_DIFF(DATE(b.created_at), a.cohort_date, DAY) BETWEEN 0 AND 13  AND b.status = 'pending' THEN b.price ELSE NULL END) AS cashout_pending_wards_w1
    ,IF(SUM(CASE WHEN DATE_DIFF(DATE(b.created_at), a.cohort_date, DAY) BETWEEN 0 AND 13  AND b.status = 'pending' THEN 1 ELSE 0 END) > 0, 1, 0) AS cashout_pending_users_w1
    ,SUM(CASE WHEN DATE_DIFF(DATE(b.created_at), a.cohort_date, DAY) BETWEEN 0 AND 20  AND b.status = 'pending' THEN b.price ELSE NULL END) AS cashout_pending_wards_w2
    ,IF(SUM(CASE WHEN DATE_DIFF(DATE(b.created_at), a.cohort_date, DAY) BETWEEN 0 AND 20  AND b.status = 'pending' THEN 1 ELSE 0 END) > 0, 1, 0) AS cashout_pending_users_w2
    ,SUM(CASE WHEN DATE_DIFF(DATE(b.created_at), a.cohort_date, DAY) BETWEEN 0 AND 27  AND b.status = 'pending' THEN b.price ELSE NULL END) AS cashout_pending_wards_w3
    ,IF(SUM(CASE WHEN DATE_DIFF(DATE(b.created_at), a.cohort_date, DAY) BETWEEN 0 AND 27  AND b.status = 'pending' THEN 1 ELSE 0 END) > 0, 1, 0) AS cashout_pending_users_w3
    ,SUM(CASE WHEN DATE_DIFF(DATE(b.created_at), a.cohort_date, DAY) BETWEEN 0 AND 34  AND b.status = 'pending' THEN b.price ELSE NULL END) AS cashout_pending_wards_w4
    ,IF(SUM(CASE WHEN DATE_DIFF(DATE(b.created_at), a.cohort_date, DAY) BETWEEN 0 AND 34  AND b.status = 'pending' THEN 1 ELSE 0 END) > 0, 1, 0) AS cashout_pending_users_w4
    -- ===== W0–W4 (attempted = all except refunded) =====
    ,SUM(CASE WHEN DATE_DIFF(DATE(b.created_at), a.cohort_date, DAY) BETWEEN 0 AND 6   AND b.status != 'refunded' THEN b.price ELSE NULL END) AS cashout_attempted_wards_w0
    ,IF(SUM(CASE WHEN DATE_DIFF(DATE(b.created_at), a.cohort_date, DAY) BETWEEN 0 AND 6   AND b.status != 'refunded' THEN 1 ELSE 0 END) > 0, 1, 0) AS cashout_attempted_users_w0
    ,SUM(CASE WHEN DATE_DIFF(DATE(b.created_at), a.cohort_date, DAY) BETWEEN 0 AND 13  AND b.status != 'refunded' THEN b.price ELSE NULL END) AS cashout_attempted_wards_w1
    ,IF(SUM(CASE WHEN DATE_DIFF(DATE(b.created_at), a.cohort_date, DAY) BETWEEN 0 AND 13  AND b.status != 'refunded' THEN 1 ELSE 0 END) > 0, 1, 0) AS cashout_attempted_users_w1
    ,SUM(CASE WHEN DATE_DIFF(DATE(b.created_at), a.cohort_date, DAY) BETWEEN 0 AND 20  AND b.status != 'refunded' THEN b.price ELSE NULL END) AS cashout_attempted_wards_w2
    ,IF(SUM(CASE WHEN DATE_DIFF(DATE(b.created_at), a.cohort_date, DAY) BETWEEN 0 AND 20  AND b.status != 'refunded' THEN 1 ELSE 0 END) > 0, 1, 0) AS cashout_attempted_users_w2
    ,SUM(CASE WHEN DATE_DIFF(DATE(b.created_at), a.cohort_date, DAY) BETWEEN 0 AND 27  AND b.status != 'refunded' THEN b.price ELSE NULL END) AS cashout_attempted_wards_w3
    ,IF(SUM(CASE WHEN DATE_DIFF(DATE(b.created_at), a.cohort_date, DAY) BETWEEN 0 AND 27  AND b.status != 'refunded' THEN 1 ELSE 0 END) > 0, 1, 0) AS cashout_attempted_users_w3
    ,SUM(CASE WHEN DATE_DIFF(DATE(b.created_at), a.cohort_date, DAY) BETWEEN 0 AND 34  AND b.status != 'refunded' THEN b.price ELSE NULL END) AS cashout_attempted_wards_w4
    ,IF(SUM(CASE WHEN DATE_DIFF(DATE(b.created_at), a.cohort_date, DAY) BETWEEN 0 AND 34  AND b.status != 'refunded' THEN 1 ELSE 0 END) > 0, 1, 0) AS cashout_attempted_users_w4
  FROM cohort_users AS a
  LEFT JOIN `696845466639.raw_data.store_order` AS b
    ON a.customer_id = b.customer_id
    AND DATE(b.created_at) >= a.cohort_date
    AND DATE(b.created_at) >= CURRENT_DATE - 90
    AND DATE(b.created_at) < CURRENT_DATE
    AND b.transfer_id IS NOT NULL
  GROUP BY 1
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

SELECT
  c.cohort_date
  ,c.platform_country
  ,COUNT(DISTINCT c.customer_id)                                     AS cohort_size
  ,DATE_DIFF(CURRENT_DATE, c.cohort_date, DAY)  AS days_since_cohort

  -- ===== D0–D6: Redemption Rate (% of cohort who made a monetary cashout) =====
  ,CASE WHEN DATE_DIFF(CURRENT_DATE, c.cohort_date, DAY) >= 0
    THEN SAFE_DIVIDE(SUM(r.has_redeemed_d0), COUNT(DISTINCT c.customer_id)) ELSE NULL END AS redemption_rate_d0
  ,CASE WHEN DATE_DIFF(CURRENT_DATE, c.cohort_date, DAY) >= 1
    THEN SAFE_DIVIDE(SUM(r.has_redeemed_d1), COUNT(DISTINCT c.customer_id)) ELSE NULL END AS redemption_rate_d1
  ,CASE WHEN DATE_DIFF(CURRENT_DATE, c.cohort_date, DAY) >= 2
    THEN SAFE_DIVIDE(SUM(r.has_redeemed_d2), COUNT(DISTINCT c.customer_id)) ELSE NULL END AS redemption_rate_d2
  ,CASE WHEN DATE_DIFF(CURRENT_DATE, c.cohort_date, DAY) >= 3
    THEN SAFE_DIVIDE(SUM(r.has_redeemed_d3), COUNT(DISTINCT c.customer_id)) ELSE NULL END AS redemption_rate_d3
  ,CASE WHEN DATE_DIFF(CURRENT_DATE, c.cohort_date, DAY) >= 4
    THEN SAFE_DIVIDE(SUM(r.has_redeemed_d4), COUNT(DISTINCT c.customer_id)) ELSE NULL END AS redemption_rate_d4
  ,CASE WHEN DATE_DIFF(CURRENT_DATE, c.cohort_date, DAY) >= 5
    THEN SAFE_DIVIDE(SUM(r.has_redeemed_d5), COUNT(DISTINCT c.customer_id)) ELSE NULL END AS redemption_rate_d5
  ,CASE WHEN DATE_DIFF(CURRENT_DATE, c.cohort_date, DAY) >= 6
    THEN SAFE_DIVIDE(SUM(r.has_redeemed_d6), COUNT(DISTINCT c.customer_id)) ELSE NULL END AS redemption_rate_d6

  -- ===== D0–D6: Avg EUR Redeemed Per User =====
  ,CASE WHEN DATE_DIFF(CURRENT_DATE, c.cohort_date, DAY) >= 0
    THEN SAFE_DIVIDE(SUM(r.rdm_eur_d0), COUNT(DISTINCT c.customer_id)) ELSE NULL END AS avg_rdm_eur_d0
  ,CASE WHEN DATE_DIFF(CURRENT_DATE, c.cohort_date, DAY) >= 1
    THEN SAFE_DIVIDE(SUM(r.rdm_eur_d1), COUNT(DISTINCT c.customer_id)) ELSE NULL END AS avg_rdm_eur_d1
  ,CASE WHEN DATE_DIFF(CURRENT_DATE, c.cohort_date, DAY) >= 2
    THEN SAFE_DIVIDE(SUM(r.rdm_eur_d2), COUNT(DISTINCT c.customer_id)) ELSE NULL END AS avg_rdm_eur_d2
  ,CASE WHEN DATE_DIFF(CURRENT_DATE, c.cohort_date, DAY) >= 3
    THEN SAFE_DIVIDE(SUM(r.rdm_eur_d3), COUNT(DISTINCT c.customer_id)) ELSE NULL END AS avg_rdm_eur_d3
  ,CASE WHEN DATE_DIFF(CURRENT_DATE, c.cohort_date, DAY) >= 4
    THEN SAFE_DIVIDE(SUM(r.rdm_eur_d4), COUNT(DISTINCT c.customer_id)) ELSE NULL END AS avg_rdm_eur_d4
  ,CASE WHEN DATE_DIFF(CURRENT_DATE, c.cohort_date, DAY) >= 5
    THEN SAFE_DIVIDE(SUM(r.rdm_eur_d5), COUNT(DISTINCT c.customer_id)) ELSE NULL END AS avg_rdm_eur_d5
  ,CASE WHEN DATE_DIFF(CURRENT_DATE, c.cohort_date, DAY) >= 6
    THEN SAFE_DIVIDE(SUM(r.rdm_eur_d6), COUNT(DISTINCT c.customer_id)) ELSE NULL END AS avg_rdm_eur_d6

  -- ===== D0–D6: Avg Wards Redeemed Per User =====
  ,CASE WHEN DATE_DIFF(CURRENT_DATE, c.cohort_date, DAY) >= 0
    THEN SAFE_DIVIDE(SUM(r.rdm_wards_d0), COUNT(DISTINCT c.customer_id)) ELSE NULL END AS avg_rdm_wards_d0
  ,CASE WHEN DATE_DIFF(CURRENT_DATE, c.cohort_date, DAY) >= 1
    THEN SAFE_DIVIDE(SUM(r.rdm_wards_d1), COUNT(DISTINCT c.customer_id)) ELSE NULL END AS avg_rdm_wards_d1
  ,CASE WHEN DATE_DIFF(CURRENT_DATE, c.cohort_date, DAY) >= 2
    THEN SAFE_DIVIDE(SUM(r.rdm_wards_d2), COUNT(DISTINCT c.customer_id)) ELSE NULL END AS avg_rdm_wards_d2
  ,CASE WHEN DATE_DIFF(CURRENT_DATE, c.cohort_date, DAY) >= 3
    THEN SAFE_DIVIDE(SUM(r.rdm_wards_d3), COUNT(DISTINCT c.customer_id)) ELSE NULL END AS avg_rdm_wards_d3
  ,CASE WHEN DATE_DIFF(CURRENT_DATE, c.cohort_date, DAY) >= 4
    THEN SAFE_DIVIDE(SUM(r.rdm_wards_d4), COUNT(DISTINCT c.customer_id)) ELSE NULL END AS avg_rdm_wards_d4
  ,CASE WHEN DATE_DIFF(CURRENT_DATE, c.cohort_date, DAY) >= 5
    THEN SAFE_DIVIDE(SUM(r.rdm_wards_d5), COUNT(DISTINCT c.customer_id)) ELSE NULL END AS avg_rdm_wards_d5
  ,CASE WHEN DATE_DIFF(CURRENT_DATE, c.cohort_date, DAY) >= 6
    THEN SAFE_DIVIDE(SUM(r.rdm_wards_d6), COUNT(DISTINCT c.customer_id)) ELSE NULL END AS avg_rdm_wards_d6

  -- ===== D0–D6: Earning Rate (% of cohort who earned any wards) =====
  ,CASE WHEN DATE_DIFF(CURRENT_DATE, c.cohort_date, DAY) >= 0
    THEN SAFE_DIVIDE(SUM(e.has_earned_d0), COUNT(DISTINCT c.customer_id)) ELSE NULL END AS earning_rate_d0
  ,CASE WHEN DATE_DIFF(CURRENT_DATE, c.cohort_date, DAY) >= 1
    THEN SAFE_DIVIDE(SUM(e.has_earned_d1), COUNT(DISTINCT c.customer_id)) ELSE NULL END AS earning_rate_d1
  ,CASE WHEN DATE_DIFF(CURRENT_DATE, c.cohort_date, DAY) >= 2
    THEN SAFE_DIVIDE(SUM(e.has_earned_d2), COUNT(DISTINCT c.customer_id)) ELSE NULL END AS earning_rate_d2
  ,CASE WHEN DATE_DIFF(CURRENT_DATE, c.cohort_date, DAY) >= 3
    THEN SAFE_DIVIDE(SUM(e.has_earned_d3), COUNT(DISTINCT c.customer_id)) ELSE NULL END AS earning_rate_d3
  ,CASE WHEN DATE_DIFF(CURRENT_DATE, c.cohort_date, DAY) >= 4
    THEN SAFE_DIVIDE(SUM(e.has_earned_d4), COUNT(DISTINCT c.customer_id)) ELSE NULL END AS earning_rate_d4
  ,CASE WHEN DATE_DIFF(CURRENT_DATE, c.cohort_date, DAY) >= 5
    THEN SAFE_DIVIDE(SUM(e.has_earned_d5), COUNT(DISTINCT c.customer_id)) ELSE NULL END AS earning_rate_d5
  ,CASE WHEN DATE_DIFF(CURRENT_DATE, c.cohort_date, DAY) >= 6
    THEN SAFE_DIVIDE(SUM(e.has_earned_d6), COUNT(DISTINCT c.customer_id)) ELSE NULL END AS earning_rate_d6

  -- ===== D0–D6: Avg Wards Earned Per User =====
  ,CASE WHEN DATE_DIFF(CURRENT_DATE, c.cohort_date, DAY) >= 0
    THEN SAFE_DIVIDE(SUM(e.earned_wards_d0), COUNT(DISTINCT c.customer_id)) ELSE NULL END AS avg_earned_wards_d0
  ,CASE WHEN DATE_DIFF(CURRENT_DATE, c.cohort_date, DAY) >= 1
    THEN SAFE_DIVIDE(SUM(e.earned_wards_d1), COUNT(DISTINCT c.customer_id)) ELSE NULL END AS avg_earned_wards_d1
  ,CASE WHEN DATE_DIFF(CURRENT_DATE, c.cohort_date, DAY) >= 2
    THEN SAFE_DIVIDE(SUM(e.earned_wards_d2), COUNT(DISTINCT c.customer_id)) ELSE NULL END AS avg_earned_wards_d2
  ,CASE WHEN DATE_DIFF(CURRENT_DATE, c.cohort_date, DAY) >= 3
    THEN SAFE_DIVIDE(SUM(e.earned_wards_d3), COUNT(DISTINCT c.customer_id)) ELSE NULL END AS avg_earned_wards_d3
  ,CASE WHEN DATE_DIFF(CURRENT_DATE, c.cohort_date, DAY) >= 4
    THEN SAFE_DIVIDE(SUM(e.earned_wards_d4), COUNT(DISTINCT c.customer_id)) ELSE NULL END AS avg_earned_wards_d4
  ,CASE WHEN DATE_DIFF(CURRENT_DATE, c.cohort_date, DAY) >= 5
    THEN SAFE_DIVIDE(SUM(e.earned_wards_d5), COUNT(DISTINCT c.customer_id)) ELSE NULL END AS avg_earned_wards_d5
  ,CASE WHEN DATE_DIFF(CURRENT_DATE, c.cohort_date, DAY) >= 6
    THEN SAFE_DIVIDE(SUM(e.earned_wards_d6), COUNT(DISTINCT c.customer_id)) ELSE NULL END AS avg_earned_wards_d6

  -- ===== W0–W4: Redemption Rate =====
  ,CASE WHEN DATE_DIFF(CURRENT_DATE, c.cohort_date, DAY) >= 7
    THEN SAFE_DIVIDE(SUM(r.has_redeemed_w0), COUNT(DISTINCT c.customer_id)) ELSE NULL END AS redemption_rate_w0
  ,CASE WHEN DATE_DIFF(CURRENT_DATE, c.cohort_date, DAY) >= 14
    THEN SAFE_DIVIDE(SUM(r.has_redeemed_w1), COUNT(DISTINCT c.customer_id)) ELSE NULL END AS redemption_rate_w1
  ,CASE WHEN DATE_DIFF(CURRENT_DATE, c.cohort_date, DAY) >= 21
    THEN SAFE_DIVIDE(SUM(r.has_redeemed_w2), COUNT(DISTINCT c.customer_id)) ELSE NULL END AS redemption_rate_w2
  ,CASE WHEN DATE_DIFF(CURRENT_DATE, c.cohort_date, DAY) >= 28
    THEN SAFE_DIVIDE(SUM(r.has_redeemed_w3), COUNT(DISTINCT c.customer_id)) ELSE NULL END AS redemption_rate_w3
  ,CASE WHEN DATE_DIFF(CURRENT_DATE, c.cohort_date, DAY) >= 35
    THEN SAFE_DIVIDE(SUM(r.has_redeemed_w4), COUNT(DISTINCT c.customer_id)) ELSE NULL END AS redemption_rate_w4

  -- ===== W0–W4: Avg EUR Redeemed Per User =====
  ,CASE WHEN DATE_DIFF(CURRENT_DATE, c.cohort_date, DAY) >= 7
    THEN SAFE_DIVIDE(SUM(r.rdm_eur_w0), COUNT(DISTINCT c.customer_id)) ELSE NULL END AS avg_rdm_eur_w0
  ,CASE WHEN DATE_DIFF(CURRENT_DATE, c.cohort_date, DAY) >= 14
    THEN SAFE_DIVIDE(SUM(r.rdm_eur_w1), COUNT(DISTINCT c.customer_id)) ELSE NULL END AS avg_rdm_eur_w1
  ,CASE WHEN DATE_DIFF(CURRENT_DATE, c.cohort_date, DAY) >= 21
    THEN SAFE_DIVIDE(SUM(r.rdm_eur_w2), COUNT(DISTINCT c.customer_id)) ELSE NULL END AS avg_rdm_eur_w2
  ,CASE WHEN DATE_DIFF(CURRENT_DATE, c.cohort_date, DAY) >= 28
    THEN SAFE_DIVIDE(SUM(r.rdm_eur_w3), COUNT(DISTINCT c.customer_id)) ELSE NULL END AS avg_rdm_eur_w3
  ,CASE WHEN DATE_DIFF(CURRENT_DATE, c.cohort_date, DAY) >= 35
    THEN SAFE_DIVIDE(SUM(r.rdm_eur_w4), COUNT(DISTINCT c.customer_id)) ELSE NULL END AS avg_rdm_eur_w4

  -- ===== W0–W4: Avg Wards Redeemed Per User =====
  ,CASE WHEN DATE_DIFF(CURRENT_DATE, c.cohort_date, DAY) >= 7
    THEN SAFE_DIVIDE(SUM(r.rdm_wards_w0), COUNT(DISTINCT c.customer_id)) ELSE NULL END AS avg_rdm_wards_w0
  ,CASE WHEN DATE_DIFF(CURRENT_DATE, c.cohort_date, DAY) >= 14
    THEN SAFE_DIVIDE(SUM(r.rdm_wards_w1), COUNT(DISTINCT c.customer_id)) ELSE NULL END AS avg_rdm_wards_w1
  ,CASE WHEN DATE_DIFF(CURRENT_DATE, c.cohort_date, DAY) >= 21
    THEN SAFE_DIVIDE(SUM(r.rdm_wards_w2), COUNT(DISTINCT c.customer_id)) ELSE NULL END AS avg_rdm_wards_w2
  ,CASE WHEN DATE_DIFF(CURRENT_DATE, c.cohort_date, DAY) >= 28
    THEN SAFE_DIVIDE(SUM(r.rdm_wards_w3), COUNT(DISTINCT c.customer_id)) ELSE NULL END AS avg_rdm_wards_w3
  ,CASE WHEN DATE_DIFF(CURRENT_DATE, c.cohort_date, DAY) >= 35
    THEN SAFE_DIVIDE(SUM(r.rdm_wards_w4), COUNT(DISTINCT c.customer_id)) ELSE NULL END AS avg_rdm_wards_w4

  -- ===== W0–W4: Earning Rate =====
  ,CASE WHEN DATE_DIFF(CURRENT_DATE, c.cohort_date, DAY) >= 7
    THEN SAFE_DIVIDE(SUM(e.has_earned_w0), COUNT(DISTINCT c.customer_id)) ELSE NULL END AS earning_rate_w0
  ,CASE WHEN DATE_DIFF(CURRENT_DATE, c.cohort_date, DAY) >= 14
    THEN SAFE_DIVIDE(SUM(e.has_earned_w1), COUNT(DISTINCT c.customer_id)) ELSE NULL END AS earning_rate_w1
  ,CASE WHEN DATE_DIFF(CURRENT_DATE, c.cohort_date, DAY) >= 21
    THEN SAFE_DIVIDE(SUM(e.has_earned_w2), COUNT(DISTINCT c.customer_id)) ELSE NULL END AS earning_rate_w2
  ,CASE WHEN DATE_DIFF(CURRENT_DATE, c.cohort_date, DAY) >= 28
    THEN SAFE_DIVIDE(SUM(e.has_earned_w3), COUNT(DISTINCT c.customer_id)) ELSE NULL END AS earning_rate_w3
  ,CASE WHEN DATE_DIFF(CURRENT_DATE, c.cohort_date, DAY) >= 35
    THEN SAFE_DIVIDE(SUM(e.has_earned_w4), COUNT(DISTINCT c.customer_id)) ELSE NULL END AS earning_rate_w4

  -- ===== W0–W4: Avg Wards Earned Per User =====
  ,CASE WHEN DATE_DIFF(CURRENT_DATE, c.cohort_date, DAY) >= 7
    THEN SAFE_DIVIDE(SUM(e.earned_wards_w0), COUNT(DISTINCT c.customer_id)) ELSE NULL END AS avg_earned_wards_w0
  ,CASE WHEN DATE_DIFF(CURRENT_DATE, c.cohort_date, DAY) >= 14
    THEN SAFE_DIVIDE(SUM(e.earned_wards_w1), COUNT(DISTINCT c.customer_id)) ELSE NULL END AS avg_earned_wards_w1
  ,CASE WHEN DATE_DIFF(CURRENT_DATE, c.cohort_date, DAY) >= 21
    THEN SAFE_DIVIDE(SUM(e.earned_wards_w2), COUNT(DISTINCT c.customer_id)) ELSE NULL END AS avg_earned_wards_w2
  ,CASE WHEN DATE_DIFF(CURRENT_DATE, c.cohort_date, DAY) >= 28
    THEN SAFE_DIVIDE(SUM(e.earned_wards_w3), COUNT(DISTINCT c.customer_id)) ELSE NULL END AS avg_earned_wards_w3
  ,CASE WHEN DATE_DIFF(CURRENT_DATE, c.cohort_date, DAY) >= 35
    THEN SAFE_DIVIDE(SUM(e.earned_wards_w4), COUNT(DISTINCT c.customer_id)) ELSE NULL END AS avg_earned_wards_w4

  -- ===== D0–D6: Users Earning 500+ Wards (count + share of cohort) =====
  ,CASE WHEN DATE_DIFF(CURRENT_DATE, c.cohort_date, DAY) >= 0
    THEN SUM(e.earners_500_d0) ELSE NULL END AS earners_500_d0
  ,CASE WHEN DATE_DIFF(CURRENT_DATE, c.cohort_date, DAY) >= 0
    THEN SAFE_DIVIDE(SUM(e.earners_500_d0), COUNT(DISTINCT c.customer_id)) ELSE NULL END AS earners_500_rate_d0
  ,CASE WHEN DATE_DIFF(CURRENT_DATE, c.cohort_date, DAY) >= 1
    THEN SUM(e.earners_500_d1) ELSE NULL END AS earners_500_d1
  ,CASE WHEN DATE_DIFF(CURRENT_DATE, c.cohort_date, DAY) >= 1
    THEN SAFE_DIVIDE(SUM(e.earners_500_d1), COUNT(DISTINCT c.customer_id)) ELSE NULL END AS earners_500_rate_d1
  ,CASE WHEN DATE_DIFF(CURRENT_DATE, c.cohort_date, DAY) >= 2
    THEN SUM(e.earners_500_d2) ELSE NULL END AS earners_500_d2
  ,CASE WHEN DATE_DIFF(CURRENT_DATE, c.cohort_date, DAY) >= 2
    THEN SAFE_DIVIDE(SUM(e.earners_500_d2), COUNT(DISTINCT c.customer_id)) ELSE NULL END AS earners_500_rate_d2
  ,CASE WHEN DATE_DIFF(CURRENT_DATE, c.cohort_date, DAY) >= 3
    THEN SUM(e.earners_500_d3) ELSE NULL END AS earners_500_d3
  ,CASE WHEN DATE_DIFF(CURRENT_DATE, c.cohort_date, DAY) >= 3
    THEN SAFE_DIVIDE(SUM(e.earners_500_d3), COUNT(DISTINCT c.customer_id)) ELSE NULL END AS earners_500_rate_d3
  ,CASE WHEN DATE_DIFF(CURRENT_DATE, c.cohort_date, DAY) >= 4
    THEN SUM(e.earners_500_d4) ELSE NULL END AS earners_500_d4
  ,CASE WHEN DATE_DIFF(CURRENT_DATE, c.cohort_date, DAY) >= 4
    THEN SAFE_DIVIDE(SUM(e.earners_500_d4), COUNT(DISTINCT c.customer_id)) ELSE NULL END AS earners_500_rate_d4
  ,CASE WHEN DATE_DIFF(CURRENT_DATE, c.cohort_date, DAY) >= 5
    THEN SUM(e.earners_500_d5) ELSE NULL END AS earners_500_d5
  ,CASE WHEN DATE_DIFF(CURRENT_DATE, c.cohort_date, DAY) >= 5
    THEN SAFE_DIVIDE(SUM(e.earners_500_d5), COUNT(DISTINCT c.customer_id)) ELSE NULL END AS earners_500_rate_d5
  ,CASE WHEN DATE_DIFF(CURRENT_DATE, c.cohort_date, DAY) >= 6
    THEN SUM(e.earners_500_d6) ELSE NULL END AS earners_500_d6
  ,CASE WHEN DATE_DIFF(CURRENT_DATE, c.cohort_date, DAY) >= 6
    THEN SAFE_DIVIDE(SUM(e.earners_500_d6), COUNT(DISTINCT c.customer_id)) ELSE NULL END AS earners_500_rate_d6

  -- ===== D0–D6: Users Earning 1000+ Wards (count + share of cohort) =====
  ,CASE WHEN DATE_DIFF(CURRENT_DATE, c.cohort_date, DAY) >= 0
    THEN SUM(e.earners_1000_d0) ELSE NULL END AS earners_1000_d0
  ,CASE WHEN DATE_DIFF(CURRENT_DATE, c.cohort_date, DAY) >= 0
    THEN SAFE_DIVIDE(SUM(e.earners_1000_d0), COUNT(DISTINCT c.customer_id)) ELSE NULL END AS earners_1000_rate_d0
  ,CASE WHEN DATE_DIFF(CURRENT_DATE, c.cohort_date, DAY) >= 1
    THEN SUM(e.earners_1000_d1) ELSE NULL END AS earners_1000_d1
  ,CASE WHEN DATE_DIFF(CURRENT_DATE, c.cohort_date, DAY) >= 1
    THEN SAFE_DIVIDE(SUM(e.earners_1000_d1), COUNT(DISTINCT c.customer_id)) ELSE NULL END AS earners_1000_rate_d1
  ,CASE WHEN DATE_DIFF(CURRENT_DATE, c.cohort_date, DAY) >= 2
    THEN SUM(e.earners_1000_d2) ELSE NULL END AS earners_1000_d2
  ,CASE WHEN DATE_DIFF(CURRENT_DATE, c.cohort_date, DAY) >= 2
    THEN SAFE_DIVIDE(SUM(e.earners_1000_d2), COUNT(DISTINCT c.customer_id)) ELSE NULL END AS earners_1000_rate_d2
  ,CASE WHEN DATE_DIFF(CURRENT_DATE, c.cohort_date, DAY) >= 3
    THEN SUM(e.earners_1000_d3) ELSE NULL END AS earners_1000_d3
  ,CASE WHEN DATE_DIFF(CURRENT_DATE, c.cohort_date, DAY) >= 3
    THEN SAFE_DIVIDE(SUM(e.earners_1000_d3), COUNT(DISTINCT c.customer_id)) ELSE NULL END AS earners_1000_rate_d3
  ,CASE WHEN DATE_DIFF(CURRENT_DATE, c.cohort_date, DAY) >= 4
    THEN SUM(e.earners_1000_d4) ELSE NULL END AS earners_1000_d4
  ,CASE WHEN DATE_DIFF(CURRENT_DATE, c.cohort_date, DAY) >= 4
    THEN SAFE_DIVIDE(SUM(e.earners_1000_d4), COUNT(DISTINCT c.customer_id)) ELSE NULL END AS earners_1000_rate_d4
  ,CASE WHEN DATE_DIFF(CURRENT_DATE, c.cohort_date, DAY) >= 5
    THEN SUM(e.earners_1000_d5) ELSE NULL END AS earners_1000_d5
  ,CASE WHEN DATE_DIFF(CURRENT_DATE, c.cohort_date, DAY) >= 5
    THEN SAFE_DIVIDE(SUM(e.earners_1000_d5), COUNT(DISTINCT c.customer_id)) ELSE NULL END AS earners_1000_rate_d5
  ,CASE WHEN DATE_DIFF(CURRENT_DATE, c.cohort_date, DAY) >= 6
    THEN SUM(e.earners_1000_d6) ELSE NULL END AS earners_1000_d6
  ,CASE WHEN DATE_DIFF(CURRENT_DATE, c.cohort_date, DAY) >= 6
    THEN SAFE_DIVIDE(SUM(e.earners_1000_d6), COUNT(DISTINCT c.customer_id)) ELSE NULL END AS earners_1000_rate_d6

  -- ===== W0–W4: Users Earning 500+ Wards (count + share of cohort) =====
  ,CASE WHEN DATE_DIFF(CURRENT_DATE, c.cohort_date, DAY) >= 7
    THEN SUM(e.earners_500_w0) ELSE NULL END AS earners_500_w0
  ,CASE WHEN DATE_DIFF(CURRENT_DATE, c.cohort_date, DAY) >= 7
    THEN SAFE_DIVIDE(SUM(e.earners_500_w0), COUNT(DISTINCT c.customer_id)) ELSE NULL END AS earners_500_rate_w0
  ,CASE WHEN DATE_DIFF(CURRENT_DATE, c.cohort_date, DAY) >= 14
    THEN SUM(e.earners_500_w1) ELSE NULL END AS earners_500_w1
  ,CASE WHEN DATE_DIFF(CURRENT_DATE, c.cohort_date, DAY) >= 14
    THEN SAFE_DIVIDE(SUM(e.earners_500_w1), COUNT(DISTINCT c.customer_id)) ELSE NULL END AS earners_500_rate_w1
  ,CASE WHEN DATE_DIFF(CURRENT_DATE, c.cohort_date, DAY) >= 21
    THEN SUM(e.earners_500_w2) ELSE NULL END AS earners_500_w2
  ,CASE WHEN DATE_DIFF(CURRENT_DATE, c.cohort_date, DAY) >= 21
    THEN SAFE_DIVIDE(SUM(e.earners_500_w2), COUNT(DISTINCT c.customer_id)) ELSE NULL END AS earners_500_rate_w2
  ,CASE WHEN DATE_DIFF(CURRENT_DATE, c.cohort_date, DAY) >= 28
    THEN SUM(e.earners_500_w3) ELSE NULL END AS earners_500_w3
  ,CASE WHEN DATE_DIFF(CURRENT_DATE, c.cohort_date, DAY) >= 28
    THEN SAFE_DIVIDE(SUM(e.earners_500_w3), COUNT(DISTINCT c.customer_id)) ELSE NULL END AS earners_500_rate_w3
  ,CASE WHEN DATE_DIFF(CURRENT_DATE, c.cohort_date, DAY) >= 35
    THEN SUM(e.earners_500_w4) ELSE NULL END AS earners_500_w4
  ,CASE WHEN DATE_DIFF(CURRENT_DATE, c.cohort_date, DAY) >= 35
    THEN SAFE_DIVIDE(SUM(e.earners_500_w4), COUNT(DISTINCT c.customer_id)) ELSE NULL END AS earners_500_rate_w4

  -- ===== W0–W4: Users Earning 1000+ Wards (count + share of cohort) =====
  ,CASE WHEN DATE_DIFF(CURRENT_DATE, c.cohort_date, DAY) >= 7
    THEN SUM(e.earners_1000_w0) ELSE NULL END AS earners_1000_w0
  ,CASE WHEN DATE_DIFF(CURRENT_DATE, c.cohort_date, DAY) >= 7
    THEN SAFE_DIVIDE(SUM(e.earners_1000_w0), COUNT(DISTINCT c.customer_id)) ELSE NULL END AS earners_1000_rate_w0
  ,CASE WHEN DATE_DIFF(CURRENT_DATE, c.cohort_date, DAY) >= 14
    THEN SUM(e.earners_1000_w1) ELSE NULL END AS earners_1000_w1
  ,CASE WHEN DATE_DIFF(CURRENT_DATE, c.cohort_date, DAY) >= 14
    THEN SAFE_DIVIDE(SUM(e.earners_1000_w1), COUNT(DISTINCT c.customer_id)) ELSE NULL END AS earners_1000_rate_w1
  ,CASE WHEN DATE_DIFF(CURRENT_DATE, c.cohort_date, DAY) >= 21
    THEN SUM(e.earners_1000_w2) ELSE NULL END AS earners_1000_w2
  ,CASE WHEN DATE_DIFF(CURRENT_DATE, c.cohort_date, DAY) >= 21
    THEN SAFE_DIVIDE(SUM(e.earners_1000_w2), COUNT(DISTINCT c.customer_id)) ELSE NULL END AS earners_1000_rate_w2
  ,CASE WHEN DATE_DIFF(CURRENT_DATE, c.cohort_date, DAY) >= 28
    THEN SUM(e.earners_1000_w3) ELSE NULL END AS earners_1000_w3
  ,CASE WHEN DATE_DIFF(CURRENT_DATE, c.cohort_date, DAY) >= 28
    THEN SAFE_DIVIDE(SUM(e.earners_1000_w3), COUNT(DISTINCT c.customer_id)) ELSE NULL END AS earners_1000_rate_w3
  ,CASE WHEN DATE_DIFF(CURRENT_DATE, c.cohort_date, DAY) >= 35
    THEN SUM(e.earners_1000_w4) ELSE NULL END AS earners_1000_w4
  ,CASE WHEN DATE_DIFF(CURRENT_DATE, c.cohort_date, DAY) >= 35
    THEN SAFE_DIVIDE(SUM(e.earners_1000_w4), COUNT(DISTINCT c.customer_id)) ELSE NULL END AS earners_1000_rate_w4

  -- ===== D0–D6: Cashout — delivered =====
  ,CASE WHEN DATE_DIFF(CURRENT_DATE, c.cohort_date, DAY) >= 0  THEN SUM(co.cashout_delivered_wards_d0) ELSE NULL END AS cashout_delivered_wards_d0
  ,CASE WHEN DATE_DIFF(CURRENT_DATE, c.cohort_date, DAY) >= 0  THEN SUM(co.cashout_delivered_users_d0) ELSE NULL END AS cashout_delivered_users_d0
  ,CASE WHEN DATE_DIFF(CURRENT_DATE, c.cohort_date, DAY) >= 0  THEN SAFE_DIVIDE(SUM(co.cashout_delivered_users_d0), COUNT(DISTINCT c.customer_id)) ELSE NULL END AS cashout_delivered_rate_d0
  ,CASE WHEN DATE_DIFF(CURRENT_DATE, c.cohort_date, DAY) >= 1  THEN SUM(co.cashout_delivered_wards_d1) ELSE NULL END AS cashout_delivered_wards_d1
  ,CASE WHEN DATE_DIFF(CURRENT_DATE, c.cohort_date, DAY) >= 1  THEN SUM(co.cashout_delivered_users_d1) ELSE NULL END AS cashout_delivered_users_d1
  ,CASE WHEN DATE_DIFF(CURRENT_DATE, c.cohort_date, DAY) >= 1  THEN SAFE_DIVIDE(SUM(co.cashout_delivered_users_d1), COUNT(DISTINCT c.customer_id)) ELSE NULL END AS cashout_delivered_rate_d1
  ,CASE WHEN DATE_DIFF(CURRENT_DATE, c.cohort_date, DAY) >= 2  THEN SUM(co.cashout_delivered_wards_d2) ELSE NULL END AS cashout_delivered_wards_d2
  ,CASE WHEN DATE_DIFF(CURRENT_DATE, c.cohort_date, DAY) >= 2  THEN SUM(co.cashout_delivered_users_d2) ELSE NULL END AS cashout_delivered_users_d2
  ,CASE WHEN DATE_DIFF(CURRENT_DATE, c.cohort_date, DAY) >= 2  THEN SAFE_DIVIDE(SUM(co.cashout_delivered_users_d2), COUNT(DISTINCT c.customer_id)) ELSE NULL END AS cashout_delivered_rate_d2
  ,CASE WHEN DATE_DIFF(CURRENT_DATE, c.cohort_date, DAY) >= 3  THEN SUM(co.cashout_delivered_wards_d3) ELSE NULL END AS cashout_delivered_wards_d3
  ,CASE WHEN DATE_DIFF(CURRENT_DATE, c.cohort_date, DAY) >= 3  THEN SUM(co.cashout_delivered_users_d3) ELSE NULL END AS cashout_delivered_users_d3
  ,CASE WHEN DATE_DIFF(CURRENT_DATE, c.cohort_date, DAY) >= 3  THEN SAFE_DIVIDE(SUM(co.cashout_delivered_users_d3), COUNT(DISTINCT c.customer_id)) ELSE NULL END AS cashout_delivered_rate_d3
  ,CASE WHEN DATE_DIFF(CURRENT_DATE, c.cohort_date, DAY) >= 4  THEN SUM(co.cashout_delivered_wards_d4) ELSE NULL END AS cashout_delivered_wards_d4
  ,CASE WHEN DATE_DIFF(CURRENT_DATE, c.cohort_date, DAY) >= 4  THEN SUM(co.cashout_delivered_users_d4) ELSE NULL END AS cashout_delivered_users_d4
  ,CASE WHEN DATE_DIFF(CURRENT_DATE, c.cohort_date, DAY) >= 4  THEN SAFE_DIVIDE(SUM(co.cashout_delivered_users_d4), COUNT(DISTINCT c.customer_id)) ELSE NULL END AS cashout_delivered_rate_d4
  ,CASE WHEN DATE_DIFF(CURRENT_DATE, c.cohort_date, DAY) >= 5  THEN SUM(co.cashout_delivered_wards_d5) ELSE NULL END AS cashout_delivered_wards_d5
  ,CASE WHEN DATE_DIFF(CURRENT_DATE, c.cohort_date, DAY) >= 5  THEN SUM(co.cashout_delivered_users_d5) ELSE NULL END AS cashout_delivered_users_d5
  ,CASE WHEN DATE_DIFF(CURRENT_DATE, c.cohort_date, DAY) >= 5  THEN SAFE_DIVIDE(SUM(co.cashout_delivered_users_d5), COUNT(DISTINCT c.customer_id)) ELSE NULL END AS cashout_delivered_rate_d5
  ,CASE WHEN DATE_DIFF(CURRENT_DATE, c.cohort_date, DAY) >= 6  THEN SUM(co.cashout_delivered_wards_d6) ELSE NULL END AS cashout_delivered_wards_d6
  ,CASE WHEN DATE_DIFF(CURRENT_DATE, c.cohort_date, DAY) >= 6  THEN SUM(co.cashout_delivered_users_d6) ELSE NULL END AS cashout_delivered_users_d6
  ,CASE WHEN DATE_DIFF(CURRENT_DATE, c.cohort_date, DAY) >= 6  THEN SAFE_DIVIDE(SUM(co.cashout_delivered_users_d6), COUNT(DISTINCT c.customer_id)) ELSE NULL END AS cashout_delivered_rate_d6

  -- ===== D0–D6: Cashout — pending =====
  ,CASE WHEN DATE_DIFF(CURRENT_DATE, c.cohort_date, DAY) >= 0  THEN SUM(co.cashout_pending_wards_d0) ELSE NULL END AS cashout_pending_wards_d0
  ,CASE WHEN DATE_DIFF(CURRENT_DATE, c.cohort_date, DAY) >= 0  THEN SUM(co.cashout_pending_users_d0) ELSE NULL END AS cashout_pending_users_d0
  ,CASE WHEN DATE_DIFF(CURRENT_DATE, c.cohort_date, DAY) >= 0  THEN SAFE_DIVIDE(SUM(co.cashout_pending_users_d0), COUNT(DISTINCT c.customer_id)) ELSE NULL END AS cashout_pending_rate_d0
  ,CASE WHEN DATE_DIFF(CURRENT_DATE, c.cohort_date, DAY) >= 1  THEN SUM(co.cashout_pending_wards_d1) ELSE NULL END AS cashout_pending_wards_d1
  ,CASE WHEN DATE_DIFF(CURRENT_DATE, c.cohort_date, DAY) >= 1  THEN SUM(co.cashout_pending_users_d1) ELSE NULL END AS cashout_pending_users_d1
  ,CASE WHEN DATE_DIFF(CURRENT_DATE, c.cohort_date, DAY) >= 1  THEN SAFE_DIVIDE(SUM(co.cashout_pending_users_d1), COUNT(DISTINCT c.customer_id)) ELSE NULL END AS cashout_pending_rate_d1
  ,CASE WHEN DATE_DIFF(CURRENT_DATE, c.cohort_date, DAY) >= 2  THEN SUM(co.cashout_pending_wards_d2) ELSE NULL END AS cashout_pending_wards_d2
  ,CASE WHEN DATE_DIFF(CURRENT_DATE, c.cohort_date, DAY) >= 2  THEN SUM(co.cashout_pending_users_d2) ELSE NULL END AS cashout_pending_users_d2
  ,CASE WHEN DATE_DIFF(CURRENT_DATE, c.cohort_date, DAY) >= 2  THEN SAFE_DIVIDE(SUM(co.cashout_pending_users_d2), COUNT(DISTINCT c.customer_id)) ELSE NULL END AS cashout_pending_rate_d2
  ,CASE WHEN DATE_DIFF(CURRENT_DATE, c.cohort_date, DAY) >= 3  THEN SUM(co.cashout_pending_wards_d3) ELSE NULL END AS cashout_pending_wards_d3
  ,CASE WHEN DATE_DIFF(CURRENT_DATE, c.cohort_date, DAY) >= 3  THEN SUM(co.cashout_pending_users_d3) ELSE NULL END AS cashout_pending_users_d3
  ,CASE WHEN DATE_DIFF(CURRENT_DATE, c.cohort_date, DAY) >= 3  THEN SAFE_DIVIDE(SUM(co.cashout_pending_users_d3), COUNT(DISTINCT c.customer_id)) ELSE NULL END AS cashout_pending_rate_d3
  ,CASE WHEN DATE_DIFF(CURRENT_DATE, c.cohort_date, DAY) >= 4  THEN SUM(co.cashout_pending_wards_d4) ELSE NULL END AS cashout_pending_wards_d4
  ,CASE WHEN DATE_DIFF(CURRENT_DATE, c.cohort_date, DAY) >= 4  THEN SUM(co.cashout_pending_users_d4) ELSE NULL END AS cashout_pending_users_d4
  ,CASE WHEN DATE_DIFF(CURRENT_DATE, c.cohort_date, DAY) >= 4  THEN SAFE_DIVIDE(SUM(co.cashout_pending_users_d4), COUNT(DISTINCT c.customer_id)) ELSE NULL END AS cashout_pending_rate_d4
  ,CASE WHEN DATE_DIFF(CURRENT_DATE, c.cohort_date, DAY) >= 5  THEN SUM(co.cashout_pending_wards_d5) ELSE NULL END AS cashout_pending_wards_d5
  ,CASE WHEN DATE_DIFF(CURRENT_DATE, c.cohort_date, DAY) >= 5  THEN SUM(co.cashout_pending_users_d5) ELSE NULL END AS cashout_pending_users_d5
  ,CASE WHEN DATE_DIFF(CURRENT_DATE, c.cohort_date, DAY) >= 5  THEN SAFE_DIVIDE(SUM(co.cashout_pending_users_d5), COUNT(DISTINCT c.customer_id)) ELSE NULL END AS cashout_pending_rate_d5
  ,CASE WHEN DATE_DIFF(CURRENT_DATE, c.cohort_date, DAY) >= 6  THEN SUM(co.cashout_pending_wards_d6) ELSE NULL END AS cashout_pending_wards_d6
  ,CASE WHEN DATE_DIFF(CURRENT_DATE, c.cohort_date, DAY) >= 6  THEN SUM(co.cashout_pending_users_d6) ELSE NULL END AS cashout_pending_users_d6
  ,CASE WHEN DATE_DIFF(CURRENT_DATE, c.cohort_date, DAY) >= 6  THEN SAFE_DIVIDE(SUM(co.cashout_pending_users_d6), COUNT(DISTINCT c.customer_id)) ELSE NULL END AS cashout_pending_rate_d6

  -- ===== D0–D6: Cashout — attempted (all except refunded) =====
  ,CASE WHEN DATE_DIFF(CURRENT_DATE, c.cohort_date, DAY) >= 0  THEN SUM(co.cashout_attempted_wards_d0) ELSE NULL END AS cashout_attempted_wards_d0
  ,CASE WHEN DATE_DIFF(CURRENT_DATE, c.cohort_date, DAY) >= 0  THEN SUM(co.cashout_attempted_users_d0) ELSE NULL END AS cashout_attempted_users_d0
  ,CASE WHEN DATE_DIFF(CURRENT_DATE, c.cohort_date, DAY) >= 0  THEN SAFE_DIVIDE(SUM(co.cashout_attempted_users_d0), COUNT(DISTINCT c.customer_id)) ELSE NULL END AS cashout_attempted_rate_d0
  ,CASE WHEN DATE_DIFF(CURRENT_DATE, c.cohort_date, DAY) >= 1  THEN SUM(co.cashout_attempted_wards_d1) ELSE NULL END AS cashout_attempted_wards_d1
  ,CASE WHEN DATE_DIFF(CURRENT_DATE, c.cohort_date, DAY) >= 1  THEN SUM(co.cashout_attempted_users_d1) ELSE NULL END AS cashout_attempted_users_d1
  ,CASE WHEN DATE_DIFF(CURRENT_DATE, c.cohort_date, DAY) >= 1  THEN SAFE_DIVIDE(SUM(co.cashout_attempted_users_d1), COUNT(DISTINCT c.customer_id)) ELSE NULL END AS cashout_attempted_rate_d1
  ,CASE WHEN DATE_DIFF(CURRENT_DATE, c.cohort_date, DAY) >= 2  THEN SUM(co.cashout_attempted_wards_d2) ELSE NULL END AS cashout_attempted_wards_d2
  ,CASE WHEN DATE_DIFF(CURRENT_DATE, c.cohort_date, DAY) >= 2  THEN SUM(co.cashout_attempted_users_d2) ELSE NULL END AS cashout_attempted_users_d2
  ,CASE WHEN DATE_DIFF(CURRENT_DATE, c.cohort_date, DAY) >= 2  THEN SAFE_DIVIDE(SUM(co.cashout_attempted_users_d2), COUNT(DISTINCT c.customer_id)) ELSE NULL END AS cashout_attempted_rate_d2
  ,CASE WHEN DATE_DIFF(CURRENT_DATE, c.cohort_date, DAY) >= 3  THEN SUM(co.cashout_attempted_wards_d3) ELSE NULL END AS cashout_attempted_wards_d3
  ,CASE WHEN DATE_DIFF(CURRENT_DATE, c.cohort_date, DAY) >= 3  THEN SUM(co.cashout_attempted_users_d3) ELSE NULL END AS cashout_attempted_users_d3
  ,CASE WHEN DATE_DIFF(CURRENT_DATE, c.cohort_date, DAY) >= 3  THEN SAFE_DIVIDE(SUM(co.cashout_attempted_users_d3), COUNT(DISTINCT c.customer_id)) ELSE NULL END AS cashout_attempted_rate_d3
  ,CASE WHEN DATE_DIFF(CURRENT_DATE, c.cohort_date, DAY) >= 4  THEN SUM(co.cashout_attempted_wards_d4) ELSE NULL END AS cashout_attempted_wards_d4
  ,CASE WHEN DATE_DIFF(CURRENT_DATE, c.cohort_date, DAY) >= 4  THEN SUM(co.cashout_attempted_users_d4) ELSE NULL END AS cashout_attempted_users_d4
  ,CASE WHEN DATE_DIFF(CURRENT_DATE, c.cohort_date, DAY) >= 4  THEN SAFE_DIVIDE(SUM(co.cashout_attempted_users_d4), COUNT(DISTINCT c.customer_id)) ELSE NULL END AS cashout_attempted_rate_d4
  ,CASE WHEN DATE_DIFF(CURRENT_DATE, c.cohort_date, DAY) >= 5  THEN SUM(co.cashout_attempted_wards_d5) ELSE NULL END AS cashout_attempted_wards_d5
  ,CASE WHEN DATE_DIFF(CURRENT_DATE, c.cohort_date, DAY) >= 5  THEN SUM(co.cashout_attempted_users_d5) ELSE NULL END AS cashout_attempted_users_d5
  ,CASE WHEN DATE_DIFF(CURRENT_DATE, c.cohort_date, DAY) >= 5  THEN SAFE_DIVIDE(SUM(co.cashout_attempted_users_d5), COUNT(DISTINCT c.customer_id)) ELSE NULL END AS cashout_attempted_rate_d5
  ,CASE WHEN DATE_DIFF(CURRENT_DATE, c.cohort_date, DAY) >= 6  THEN SUM(co.cashout_attempted_wards_d6) ELSE NULL END AS cashout_attempted_wards_d6
  ,CASE WHEN DATE_DIFF(CURRENT_DATE, c.cohort_date, DAY) >= 6  THEN SUM(co.cashout_attempted_users_d6) ELSE NULL END AS cashout_attempted_users_d6
  ,CASE WHEN DATE_DIFF(CURRENT_DATE, c.cohort_date, DAY) >= 6  THEN SAFE_DIVIDE(SUM(co.cashout_attempted_users_d6), COUNT(DISTINCT c.customer_id)) ELSE NULL END AS cashout_attempted_rate_d6

  -- ===== W0–W4: Cashout — delivered =====
  ,CASE WHEN DATE_DIFF(CURRENT_DATE, c.cohort_date, DAY) >= 7  THEN SUM(co.cashout_delivered_wards_w0) ELSE NULL END AS cashout_delivered_wards_w0
  ,CASE WHEN DATE_DIFF(CURRENT_DATE, c.cohort_date, DAY) >= 7  THEN SUM(co.cashout_delivered_users_w0) ELSE NULL END AS cashout_delivered_users_w0
  ,CASE WHEN DATE_DIFF(CURRENT_DATE, c.cohort_date, DAY) >= 7  THEN SAFE_DIVIDE(SUM(co.cashout_delivered_users_w0), COUNT(DISTINCT c.customer_id)) ELSE NULL END AS cashout_delivered_rate_w0
  ,CASE WHEN DATE_DIFF(CURRENT_DATE, c.cohort_date, DAY) >= 14 THEN SUM(co.cashout_delivered_wards_w1) ELSE NULL END AS cashout_delivered_wards_w1
  ,CASE WHEN DATE_DIFF(CURRENT_DATE, c.cohort_date, DAY) >= 14 THEN SUM(co.cashout_delivered_users_w1) ELSE NULL END AS cashout_delivered_users_w1
  ,CASE WHEN DATE_DIFF(CURRENT_DATE, c.cohort_date, DAY) >= 14 THEN SAFE_DIVIDE(SUM(co.cashout_delivered_users_w1), COUNT(DISTINCT c.customer_id)) ELSE NULL END AS cashout_delivered_rate_w1
  ,CASE WHEN DATE_DIFF(CURRENT_DATE, c.cohort_date, DAY) >= 21 THEN SUM(co.cashout_delivered_wards_w2) ELSE NULL END AS cashout_delivered_wards_w2
  ,CASE WHEN DATE_DIFF(CURRENT_DATE, c.cohort_date, DAY) >= 21 THEN SUM(co.cashout_delivered_users_w2) ELSE NULL END AS cashout_delivered_users_w2
  ,CASE WHEN DATE_DIFF(CURRENT_DATE, c.cohort_date, DAY) >= 21 THEN SAFE_DIVIDE(SUM(co.cashout_delivered_users_w2), COUNT(DISTINCT c.customer_id)) ELSE NULL END AS cashout_delivered_rate_w2
  ,CASE WHEN DATE_DIFF(CURRENT_DATE, c.cohort_date, DAY) >= 28 THEN SUM(co.cashout_delivered_wards_w3) ELSE NULL END AS cashout_delivered_wards_w3
  ,CASE WHEN DATE_DIFF(CURRENT_DATE, c.cohort_date, DAY) >= 28 THEN SUM(co.cashout_delivered_users_w3) ELSE NULL END AS cashout_delivered_users_w3
  ,CASE WHEN DATE_DIFF(CURRENT_DATE, c.cohort_date, DAY) >= 28 THEN SAFE_DIVIDE(SUM(co.cashout_delivered_users_w3), COUNT(DISTINCT c.customer_id)) ELSE NULL END AS cashout_delivered_rate_w3
  ,CASE WHEN DATE_DIFF(CURRENT_DATE, c.cohort_date, DAY) >= 35 THEN SUM(co.cashout_delivered_wards_w4) ELSE NULL END AS cashout_delivered_wards_w4
  ,CASE WHEN DATE_DIFF(CURRENT_DATE, c.cohort_date, DAY) >= 35 THEN SUM(co.cashout_delivered_users_w4) ELSE NULL END AS cashout_delivered_users_w4
  ,CASE WHEN DATE_DIFF(CURRENT_DATE, c.cohort_date, DAY) >= 35 THEN SAFE_DIVIDE(SUM(co.cashout_delivered_users_w4), COUNT(DISTINCT c.customer_id)) ELSE NULL END AS cashout_delivered_rate_w4

  -- ===== W0–W4: Cashout — pending =====
  ,CASE WHEN DATE_DIFF(CURRENT_DATE, c.cohort_date, DAY) >= 7  THEN SUM(co.cashout_pending_wards_w0) ELSE NULL END AS cashout_pending_wards_w0
  ,CASE WHEN DATE_DIFF(CURRENT_DATE, c.cohort_date, DAY) >= 7  THEN SUM(co.cashout_pending_users_w0) ELSE NULL END AS cashout_pending_users_w0
  ,CASE WHEN DATE_DIFF(CURRENT_DATE, c.cohort_date, DAY) >= 7  THEN SAFE_DIVIDE(SUM(co.cashout_pending_users_w0), COUNT(DISTINCT c.customer_id)) ELSE NULL END AS cashout_pending_rate_w0
  ,CASE WHEN DATE_DIFF(CURRENT_DATE, c.cohort_date, DAY) >= 14 THEN SUM(co.cashout_pending_wards_w1) ELSE NULL END AS cashout_pending_wards_w1
  ,CASE WHEN DATE_DIFF(CURRENT_DATE, c.cohort_date, DAY) >= 14 THEN SUM(co.cashout_pending_users_w1) ELSE NULL END AS cashout_pending_users_w1
  ,CASE WHEN DATE_DIFF(CURRENT_DATE, c.cohort_date, DAY) >= 14 THEN SAFE_DIVIDE(SUM(co.cashout_pending_users_w1), COUNT(DISTINCT c.customer_id)) ELSE NULL END AS cashout_pending_rate_w1
  ,CASE WHEN DATE_DIFF(CURRENT_DATE, c.cohort_date, DAY) >= 21 THEN SUM(co.cashout_pending_wards_w2) ELSE NULL END AS cashout_pending_wards_w2
  ,CASE WHEN DATE_DIFF(CURRENT_DATE, c.cohort_date, DAY) >= 21 THEN SUM(co.cashout_pending_users_w2) ELSE NULL END AS cashout_pending_users_w2
  ,CASE WHEN DATE_DIFF(CURRENT_DATE, c.cohort_date, DAY) >= 21 THEN SAFE_DIVIDE(SUM(co.cashout_pending_users_w2), COUNT(DISTINCT c.customer_id)) ELSE NULL END AS cashout_pending_rate_w2
  ,CASE WHEN DATE_DIFF(CURRENT_DATE, c.cohort_date, DAY) >= 28 THEN SUM(co.cashout_pending_wards_w3) ELSE NULL END AS cashout_pending_wards_w3
  ,CASE WHEN DATE_DIFF(CURRENT_DATE, c.cohort_date, DAY) >= 28 THEN SUM(co.cashout_pending_users_w3) ELSE NULL END AS cashout_pending_users_w3
  ,CASE WHEN DATE_DIFF(CURRENT_DATE, c.cohort_date, DAY) >= 28 THEN SAFE_DIVIDE(SUM(co.cashout_pending_users_w3), COUNT(DISTINCT c.customer_id)) ELSE NULL END AS cashout_pending_rate_w3
  ,CASE WHEN DATE_DIFF(CURRENT_DATE, c.cohort_date, DAY) >= 35 THEN SUM(co.cashout_pending_wards_w4) ELSE NULL END AS cashout_pending_wards_w4
  ,CASE WHEN DATE_DIFF(CURRENT_DATE, c.cohort_date, DAY) >= 35 THEN SUM(co.cashout_pending_users_w4) ELSE NULL END AS cashout_pending_users_w4
  ,CASE WHEN DATE_DIFF(CURRENT_DATE, c.cohort_date, DAY) >= 35 THEN SAFE_DIVIDE(SUM(co.cashout_pending_users_w4), COUNT(DISTINCT c.customer_id)) ELSE NULL END AS cashout_pending_rate_w4

  -- ===== W0–W4: Cashout — attempted (all except refunded) =====
  ,CASE WHEN DATE_DIFF(CURRENT_DATE, c.cohort_date, DAY) >= 7  THEN SUM(co.cashout_attempted_wards_w0) ELSE NULL END AS cashout_attempted_wards_w0
  ,CASE WHEN DATE_DIFF(CURRENT_DATE, c.cohort_date, DAY) >= 7  THEN SUM(co.cashout_attempted_users_w0) ELSE NULL END AS cashout_attempted_users_w0
  ,CASE WHEN DATE_DIFF(CURRENT_DATE, c.cohort_date, DAY) >= 7  THEN SAFE_DIVIDE(SUM(co.cashout_attempted_users_w0), COUNT(DISTINCT c.customer_id)) ELSE NULL END AS cashout_attempted_rate_w0
  ,CASE WHEN DATE_DIFF(CURRENT_DATE, c.cohort_date, DAY) >= 14 THEN SUM(co.cashout_attempted_wards_w1) ELSE NULL END AS cashout_attempted_wards_w1
  ,CASE WHEN DATE_DIFF(CURRENT_DATE, c.cohort_date, DAY) >= 14 THEN SUM(co.cashout_attempted_users_w1) ELSE NULL END AS cashout_attempted_users_w1
  ,CASE WHEN DATE_DIFF(CURRENT_DATE, c.cohort_date, DAY) >= 14 THEN SAFE_DIVIDE(SUM(co.cashout_attempted_users_w1), COUNT(DISTINCT c.customer_id)) ELSE NULL END AS cashout_attempted_rate_w1
  ,CASE WHEN DATE_DIFF(CURRENT_DATE, c.cohort_date, DAY) >= 21 THEN SUM(co.cashout_attempted_wards_w2) ELSE NULL END AS cashout_attempted_wards_w2
  ,CASE WHEN DATE_DIFF(CURRENT_DATE, c.cohort_date, DAY) >= 21 THEN SUM(co.cashout_attempted_users_w2) ELSE NULL END AS cashout_attempted_users_w2
  ,CASE WHEN DATE_DIFF(CURRENT_DATE, c.cohort_date, DAY) >= 21 THEN SAFE_DIVIDE(SUM(co.cashout_attempted_users_w2), COUNT(DISTINCT c.customer_id)) ELSE NULL END AS cashout_attempted_rate_w2
  ,CASE WHEN DATE_DIFF(CURRENT_DATE, c.cohort_date, DAY) >= 28 THEN SUM(co.cashout_attempted_wards_w3) ELSE NULL END AS cashout_attempted_wards_w3
  ,CASE WHEN DATE_DIFF(CURRENT_DATE, c.cohort_date, DAY) >= 28 THEN SUM(co.cashout_attempted_users_w3) ELSE NULL END AS cashout_attempted_users_w3
  ,CASE WHEN DATE_DIFF(CURRENT_DATE, c.cohort_date, DAY) >= 28 THEN SAFE_DIVIDE(SUM(co.cashout_attempted_users_w3), COUNT(DISTINCT c.customer_id)) ELSE NULL END AS cashout_attempted_rate_w3
  ,CASE WHEN DATE_DIFF(CURRENT_DATE, c.cohort_date, DAY) >= 35 THEN SUM(co.cashout_attempted_wards_w4) ELSE NULL END AS cashout_attempted_wards_w4
  ,CASE WHEN DATE_DIFF(CURRENT_DATE, c.cohort_date, DAY) >= 35 THEN SUM(co.cashout_attempted_users_w4) ELSE NULL END AS cashout_attempted_users_w4
  ,CASE WHEN DATE_DIFF(CURRENT_DATE, c.cohort_date, DAY) >= 35 THEN SAFE_DIVIDE(SUM(co.cashout_attempted_users_w4), COUNT(DISTINCT c.customer_id)) ELSE NULL END AS cashout_attempted_rate_w4

FROM cohort_users AS c
LEFT JOIN user_redemptions AS r  ON c.customer_id = r.customer_id
LEFT JOIN user_earnings AS e     ON c.customer_id = e.customer_id
LEFT JOIN user_cashouts AS co    ON c.customer_id = co.customer_id
WHERE c.cohort_date >= CURRENT_DATE - 60
  AND c.platform_country NOT LIKE 'Missing%'
-- AND c.customer_id NOT IN (SELECT customer_id FROM suspicious_users)
-- AND c.customer_id NOT IN (SELECT customer_id FROM referral_users)
GROUP BY 1, 2
ORDER BY 2, 1 DESC
