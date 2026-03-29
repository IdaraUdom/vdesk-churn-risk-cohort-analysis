--- Description:
--- Creates fact tables to enable analysis

--- Design Rules:
--- - Clear grain per dimension
--- - No metrics in dimensions
--- - Natural keys preserved; surrogate keys introduced where appropriate
--- - Fact tables sourced exclusively from clean layer

---Dependencies: 
--- 01_create_raw_tables.sql
--- 02_data_cleaning_from_staging_to_raw_tables.sql
--- 03_load_clean_from_raw.sql
--- 04_create_dim_tables.sql

--- Fact Subscription Revenue
--- Grain: One row per subscription per month
DROP TABLE IF EXISTS fact_subscription_revenue;
GO
SELECT
    s.subscription_id,
    s.account_id,
    d.date_key,
    s.mrr_amount,
    s.arr_amount,
	s.billing_frequency,
    s.is_active_subscription,
    s.seats,
    s.plan_tier,
    DATEDIFF(MONTH, s.start_date, d.[date]) AS months_since_start,
    CASE 
	WHEN d.[date] = DATEFROMPARTS(YEAR(s.start_date), MONTH(s.start_date), 1) 
    THEN 1 
	ELSE 0 
	END AS is_first_month,
    LAG(s.mrr_amount) OVER (PARTITION BY s.subscription_id ORDER BY d.[date]) AS prev_month_mrr,
    s.mrr_amount - LAG(s.mrr_amount) OVER (PARTITION BY s.subscription_id ORDER BY d.[date]) AS mrr_change
INTO fact_subscription_revenue
FROM clean_subscriptions s
JOIN dim_date d
  ON d.[date] = DATEFROMPARTS(YEAR(s.start_date), MONTH(s.start_date), 1)
GO

--- Fact Churn Events
--- Grain: One row per churn event
DROP TABLE IF EXISTS fact_churn_events;
GO
SELECT 
	ce.churn_event_id,
    ce.account_id,
	d.date_key,
	r.reason_key,
	ce.refund_amount_usd,
	ce.is_reactivation,
    DATEDIFF(DAY, a.signup_date, ce.churn_date) AS days_to_churn,
    DATEDIFF(MONTH, a.signup_date, ce.churn_date) AS months_to_churn,
    s.plan_tier AS plan_at_churn,
    cs.mrr_amount AS mrr_at_churn,
    s.seats AS seats_at_churn
INTO fact_churn_events
FROM clean_churn_events ce
JOIN dim_date d ON ce.churn_date = d.[date]
LEFT JOIN dim_reason r ON ce.reason_code = r.reason_code
LEFT JOIN dim_account a ON ce.account_id = a.account_id 
LEFT JOIN dim_subscription s ON ce.account_id = s.account_id 
	AND ce.churn_date BETWEEN s.start_date AND ISNULL(s.end_date, '2099-12-31')
LEFT JOIN clean_subscriptions cs ON ce.account_id = cs.account_id
    AND ce.churn_date BETWEEN cs.start_date AND ISNULL(cs.end_date, '2099-12-31');
GO

--- Fact Feature Usage
--- Grain: One row per subscription per feature per day
DROP TABLE IF EXISTS fact_feature_usage;
GO
SELECT 
    fu.subscription_id,
    f.feature_key,
    d.date_key,
    fu.usage_count,
    fu.usage_duration_secs,
    fu.error_count
INTO fact_feature_usage
FROM clean_feature_usage fu
JOIN dim_feature f
    ON fu.feature_name = f.feature_name
JOIN dim_date d
    ON fu.usage_date = d.[date];
GO

--- Fact Support Interactions
--- Grain: One row per ticket
--- Natural Key: Ticket Id
DROP TABLE IF EXISTS fact_support_interactions;
GO
SELECT 
	st.ticket_id,
	st.account_id,
	d.date_key,
	st.resolution_time_hours,
	st.satisfaction_score,
	st.priority,
	st.escalation_flag,
    CASE WHEN DATEPART(WEEKDAY, st.submitted_at) IN (1,7) THEN 1 ELSE 0 END AS is_weekend,
    DATEDIFF(HOUR, st.submitted_at, st.closed_at) AS resolution_time_hours_actual,
    CASE WHEN st.resolution_time_hours < 24 THEN 1 ELSE 0 END AS resolved_within_24h
INTO fact_support_interactions
FROM clean_support_tickets st
JOIN dim_date d
	ON st.submitted_at = d.[date]
GO

--- Fact Account Monthly Snapshot
--- Grain: One row per account per month
DROP TABLE IF EXISTS fact_account_monthly;
GO
WITH months AS (
    SELECT DISTINCT date_key, [date], [year], [month]
    FROM dim_date
    WHERE [date] BETWEEN '2023-01-01' AND GETDATE()
)
SELECT 
    a.account_id,
    m.date_key,
    m.[year],
    m.[month],
    CASE WHEN s.subscription_id IS NOT NULL THEN 1 ELSE 0 END AS has_active_subscription,
    s.seats AS current_seats,
    s.plan_tier AS current_plan_tier,
    DATEDIFF(MONTH, a.signup_date, m.[date]) AS account_tenure_months,
    CASE WHEN m.[date] = DATEFROMPARTS(YEAR(a.signup_date), MONTH(a.signup_date), 1) 
         THEN 1 ELSE 0 END AS is_new_account_month,
    CASE WHEN ce.churn_event_id IS NOT NULL THEN 1 ELSE 0 END AS churned_this_month
INTO fact_account_monthly
FROM dim_account a
CROSS JOIN months m
LEFT JOIN dim_subscription s 
    ON a.account_id = s.account_id 
    AND s.is_active_subscription = 1
    AND m.[date] BETWEEN DATEFROMPARTS(YEAR(s.start_date), MONTH(s.start_date), 1)
                     AND ISNULL(DATEFROMPARTS(YEAR(s.end_date), MONTH(s.end_date), 1), '2099-12-31')
LEFT JOIN clean_churn_events ce
    ON a.account_id = ce.account_id
    AND MONTH(ce.churn_date) = m.[month]
    AND YEAR(ce.churn_date) = m.[year]
GO

--- Fact Feature Adoption
--- Grain: One row per account per feature per month
DROP TABLE IF EXISTS fact_feature_adoption;
GO
SELECT 
    fu.subscription_id,
    a.account_id,
    d.date_key,
    d.[year],
    d.[month],
    fu.feature_name,
    SUM(fu.usage_count) AS total_usage_count,
    SUM(fu.usage_duration_secs) AS total_duration_secs,
    SUM(fu.error_count) AS total_error_count,
    COUNT(DISTINCT fu.usage_date) AS days_used,
    CASE WHEN SUM(fu.usage_count) > 0 THEN 1 ELSE 0 END AS used_feature_flag
INTO fact_feature_adoption
FROM clean_feature_usage fu
JOIN clean_subscriptions s ON fu.subscription_id = s.subscription_id
JOIN dim_account a ON s.account_id = a.account_id
JOIN dim_date d ON fu.usage_date = d.[date]
GROUP BY fu.subscription_id, a.account_id, d.date_key, d.[year], d.[month], fu.feature_name;
GO

--- Fact Customer Health Score
--- Grain: One row per account per month
DROP TABLE IF EXISTS fact_customer_health;
GO
SELECT 
    a.account_id,
    d.date_key,
    d.[year],
    d.[month],
    AVG(ISNULL(st.resolution_time_hours, 0)) AS avg_resolution_time_hours,
    AVG(ISNULL(st.satisfaction_score, 0)) AS avg_satisfaction_score,
    COUNT(st.ticket_id) AS support_ticket_count,
    SUM(CASE WHEN st.escalation_flag = 1 THEN 1 ELSE 0 END) AS escalation_count,
    AVG(ISNULL(fu.usage_count, 0)) AS avg_daily_usage,
    AVG(ISNULL(fu.error_count, 0)) AS avg_daily_errors,
    COUNT(DISTINCT fu.feature_name) AS unique_features_used,
    CASE 
        WHEN COUNT(st.ticket_id) = 0 THEN 100
        WHEN AVG(ISNULL(st.satisfaction_score, 0)) >= 4 THEN 80 
        WHEN AVG(ISNULL(st.satisfaction_score, 0)) >= 3 THEN 60
        WHEN AVG(ISNULL(st.satisfaction_score, 0)) >= 2 THEN 40
        ELSE 20
    END AS health_score
INTO fact_customer_health
FROM dim_account a
CROSS JOIN (SELECT DISTINCT date_key, [year], [month] FROM dim_date) d
LEFT JOIN clean_support_tickets st 
    ON a.account_id = st.account_id 
    AND MONTH(st.submitted_at) = d.[month]
    AND YEAR(st.submitted_at) = d.[year]
LEFT JOIN clean_feature_usage fu
    ON a.account_id = fu.subscription_id 
    AND MONTH(fu.usage_date) = d.[month]
    AND YEAR(fu.usage_date) = d.[year]
GROUP BY a.account_id, d.date_key, d.[year], d.[month];
GO