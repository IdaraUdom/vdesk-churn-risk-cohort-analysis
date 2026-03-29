---Description:
--- Creates standardized clean tables from raw tables
--- Resolves inconsistent attribute representations (plan tiers, priorities, flags)
--- Provides a stable, business-consistent foundation for dimensional modeling
--- Ensures analytical metrics (churn, retention, MRR) are unambiguous and reproducible


---Dependencies: 
--- 01_create_raw_tables.sql
--- 02_data_cleaning_from_staging_to_raw_tables.sql


--- Cleaned Accounts 
DROP TABLE IF EXISTS clean_accounts;
GO
SELECT
    account_id,
    account_name,
    LOWER(TRIM(industry)) AS industry,
    country,
    signup_date,
	referral_source,
    LOWER(TRIM(plan_tier)) AS account_plan_tier,
    seats,
    is_trial,
    churn_flag AS raw_churn_flag
INTO clean_accounts 
FROM raw_accounts;
GO

--- Cleaned Subscriptions 
DROP TABLE IF EXISTS clean_subscriptions;
GO
SELECT
    subscription_id,
    account_id,
    LOWER(TRIM(plan_tier)) AS plan_tier,
    start_date,
    end_date,
    seats,
    mrr_amount,
    arr_amount,
    is_trial,
    upgrade_flag,
    downgrade_flag,
    churn_flag,
    LOWER(TRIM(billing_frequency)) AS billing_frequency,
    auto_renew_flag,
    CASE WHEN end_date IS NULL
	THEN 1
	ELSE 0
	END AS is_active_subscription
INTO clean_subscriptions
FROM raw_subscriptions
WHERE start_date IS NOT NULL
  AND (end_date IS NULL OR end_date >= start_date);
GO

--- Cleaned Churn Events
DROP TABLE IF EXISTS clean_churn_events;
GO
SELECT
    churn_event_id,
    account_id,
    churn_date,
    LOWER(TRIM(reason_code)) AS reason_code,
    refund_amount_usd,
    preceding_upgrade_flag,
    preceding_downgrade_flag,
    is_reactivation,
    feedback_text
INTO clean_churn_events
FROM raw_churn_events
WHERE churn_date IS NOT NULL;
GO

--- Cleaned Feature Usage
DROP TABLE IF EXISTS clean_feature_usage;
GO
SELECT
    usage_id,
    subscription_id,
    usage_date,
    LOWER(TRIM(feature_name)) AS feature_name,
    usage_count,
    usage_duration_secs,
    error_count,
    is_beta_feature
INTO clean_feature_usage
FROM raw_feature_usage
WHERE usage_date IS NOT NULL;
GO


--- Cleaned Support Tickets
DROP TABLE IF EXISTS clean_support_tickets;
GO
SELECT
    ticket_id,
    account_id,
    submitted_at,
    closed_at,
    resolution_time_hours,
    LOWER(TRIM(priority)) AS priority,
    first_response_time_minutes,
    satisfaction_score,
    escalation_flag
INTO clean_support_tickets
FROM raw_support_tickets
WHERE submitted_at IS NOT NULL;
GO





