---Description:
--- Exploring staging tables ahead of load into raw tables to identify nulls and errors
--- Raw data has been imported into staging tables directly
--- Staging table data will be transported into raw tables
---Dependencies: 
--- 01_create_raw_tables.sql

---Checking for Null Values in Staging Accounts Table 
SELECT 
SUM (CASE WHEN account_id IS NULL THEN 1 ELSE 0 END) AS AccountIDNulls,
SUM (CASE WHEN account_name IS NULL THEN 1 ELSE 0 END) AS AccountNameNulls,
SUM (CASE WHEN industry IS NULL THEN 1 ELSE 0 END) AS IndustryNulls,
SUM (CASE WHEN country IS NULL THEN 1 ELSE 0 END) AS CountryNulls,
SUM (CASE WHEN signup_date IS NULL THEN 1 ELSE 0 END) AS SignUpDateNulls,
SUM (CASE WHEN referral_source IS NULL THEN 1 ELSE 0 END) AS ReferralSourceNulls,
SUM (CASE WHEN plan_tier IS NULL THEN 1 ELSE 0 END) AS PlanTierNulls,
SUM (CASE WHEN seats IS NULL THEN 1 ELSE 0 END) AS SeatsNulls,
SUM (CASE WHEN is_trial IS NULL THEN 1 ELSE 0 END) AS IsTrialNulls,
SUM (CASE WHEN churn_flag IS NULL THEN 1 ELSE 0 END) AS ChurnFlagNulls
FROM stg_accounts;

--- Load data from staging table into raw table 
TRUNCATE TABLE raw_accounts;

INSERT INTO raw_accounts(
    account_id,
    account_name,
    industry,
    country,
    signup_date,
    referral_source,
    plan_tier,
    seats,
    is_trial,
    churn_flag
)

SELECT 
    LTRIM(RTRIM(account_id)) AS account_id,
    LTRIM(RTRIM(account_name)) AS account_name,
    LTRIM(RTRIM(industry)) AS industry,
    LEFT(LTRIM(RTRIM(country)), 2) AS country,
    TRY_CONVERT(DATE, signup_date, 120) AS signup_date,
    LTRIM(RTRIM(referral_source)) AS referral_source,
    LTRIM(RTRIM(plan_tier)) AS plan_tier,
    TRY_CAST(seats AS INT) AS seats,
	is_trial,
    churn_flag
FROM stg_accounts
WHERE account_id IS NOT NULL
  AND LTRIM(RTRIM(account_id)) != '';


--- Checking for Null Values in Staging Churn Events Table 
SELECT 
SUM (CASE WHEN churn_event_id IS NULL THEN 1 ELSE 0 END) AS ChurnEventIDNulls,
SUM (CASE WHEN account_id IS NULL THEN 1 ELSE 0 END) AS AccountIDNulls,
SUM (CASE WHEN churn_date IS NULL THEN 1 ELSE 0 END) AS ChurnDateNulls,
SUM (CASE WHEN reason_code IS NULL THEN 1 ELSE 0 END) AS ReasonCodeNulls,
SUM (CASE WHEN refund_amount_usd IS NULL THEN 1 ELSE 0 END) AS RefundAmountNulls,
SUM (CASE WHEN preceding_upgrade_flag IS NULL THEN 1 ELSE 0 END) AS UpgradeNulls,
SUM (CASE WHEN preceding_downgrade_flag IS NULL THEN 1 ELSE 0 END) AS DowngradeNulls,
SUM (CASE WHEN is_reactivation IS NULL THEN 1 ELSE 0 END) AS IsReactivationNulls,
SUM (CASE WHEN feedback_text IS NULL THEN 1 ELSE 0 END) AS FeedbackTextNulls
FROM stg_churn_events;



--- Load data from staging table and correcting null values in raw table
TRUNCATE TABLE raw_churn_events;

INSERT INTO raw_churn_events(
    churn_event_id,
    account_id,
    churn_date,
    reason_code,
    refund_amount_usd,
    preceding_upgrade_flag,
    preceding_downgrade_flag,
    is_reactivation,
    feedback_text
)

SELECT 
    LTRIM(RTRIM(churn_event_id)) AS churn_event_id,
    LTRIM(RTRIM(account_id)) AS account_id,
    TRY_CONVERT(DATE, churn_date, 120) AS churn_date,
    LTRIM(RTRIM(reason_code)) AS reason_code,
    TRY_CAST(refund_amount_usd AS DECIMAL(10,2)) AS refund_amount_usd,
    preceding_upgrade_flag,
    preceding_downgrade_flag,
    is_reactivation, 
    ISNULL (LTRIM(RTRIM(feedback_text)),'None') AS feedback_text
FROM stg_churn_events
WHERE churn_event_id IS NOT NULL
  AND LTRIM(RTRIM(churn_event_id)) != '';


--- Checking for Null Values in Staging Feature Usage Table 
SELECT 
SUM (CASE WHEN usage_id IS NULL THEN 1 ELSE 0 END) AS UsageIDNulls,
SUM (CASE WHEN subscription_id IS NULL THEN 1 ELSE 0 END) AS SubscriptionIDNulls,
SUM (CASE WHEN usage_date IS NULL THEN 1 ELSE 0 END) AS UsageDateNulls,
SUM (CASE WHEN feature_name IS NULL THEN 1 ELSE 0 END) AS FeatureNameNulls,
SUM (CASE WHEN usage_count IS NULL THEN 1 ELSE 0 END) AS UsageCountNulls,
SUM (CASE WHEN usage_duration_secs IS NULL THEN 1 ELSE 0 END) AS UpgradeNulls,
SUM (CASE WHEN error_count IS NULL THEN 1 ELSE 0 END) AS ErrorCountNulls,
SUM (CASE WHEN is_beta_feature IS NULL THEN 1 ELSE 0 END) AS IsBetaNulls
FROM stg_feature_usage;


--- Load data from staging table into raw table 
TRUNCATE TABLE raw_feature_usage;

INSERT INTO raw_feature_usage(
    usage_id,
    subscription_id,
    usage_date,
    feature_name,
    usage_count,
    usage_duration_secs,
    error_count,
    is_beta_feature
)

SELECT 
    LTRIM(RTRIM(usage_id)) AS usage_id,
    LTRIM(RTRIM(subscription_id)) AS subscription_id,
    TRY_CONVERT(DATE, usage_date, 120) AS usage_date,
    LTRIM(RTRIM(feature_name)) AS feature_name,
    TRY_CAST(usage_count AS INT) AS usage_count,
    TRY_CAST(usage_duration_secs AS INT) AS usage_duration_secs,
    TRY_CAST(error_count AS TINYINT) AS error_count,
    is_beta_feature
FROM stg_feature_usage
WHERE usage_id IS NOT NULL
  AND LTRIM(RTRIM(usage_id)) != '';


--- Checking for Null Values in Staging Subscription Table 
--- Note that entries with no end date are active subscriptions so they will be left with null values
SELECT 
SUM (CASE WHEN subscription_id IS NULL THEN 1 ELSE 0 END) AS SubscriptionIDNulls,
SUM (CASE WHEN account_id IS NULL THEN 1 ELSE 0 END) AS AccountIDNulls,
SUM (CASE WHEN start_date IS NULL THEN 1 ELSE 0 END) AS StartDateNulls,
SUM (CASE WHEN end_date IS NULL THEN 1 ELSE 0 END) AS EndDateNulls,+
SUM (CASE WHEN plan_tier IS NULL THEN 1 ELSE 0 END) AS FeatureNameNulls,
SUM (CASE WHEN seats IS NULL THEN 1 ELSE 0 END) AS SeatsNulls,
SUM (CASE WHEN mrr_amount IS NULL THEN 1 ELSE 0 END) AS MrrNulls,
SUM (CASE WHEN arr_amount IS NULL THEN 1 ELSE 0 END) AS ErrorCountNulls,
SUM (CASE WHEN is_trial IS NULL THEN 1 ELSE 0 END) AS IsTrialNulls,
SUM (CASE WHEN upgrade_flag IS NULL THEN 1 ELSE 0 END) AS UpgradeFlagNulls,
SUM (CASE WHEN downgrade_flag IS NULL THEN 1 ELSE 0 END) AS DowngradeFlagNulls,
SUM (CASE WHEN churn_flag IS NULL THEN 1 ELSE 0 END) AS ChurnFlagNulls,
SUM (CASE WHEN billing_frequency IS NULL THEN 1 ELSE 0 END) AS BillingNulls,
SUM (CASE WHEN auto_renew_flag IS NULL THEN 1 ELSE 0 END) AS AutoRenewNulls
FROM stg_subscriptions;


--- Load data from staging table into raw table 
TRUNCATE TABLE raw_subscriptions;

INSERT INTO raw_subscriptions(
    subscription_id,
    account_id,
    start_date,
    end_date,
    plan_tier,
    seats,
    mrr_amount,
    arr_amount,
    is_trial,
    upgrade_flag,
    downgrade_flag,
    churn_flag,
    billing_frequency,
    auto_renew_flag
)

SELECT 
    LTRIM(RTRIM(subscription_id)) AS subscription_id,
    LTRIM(RTRIM(account_id)) AS account_id,
    TRY_CONVERT(DATE, start_date, 120) AS start_date, 
	CASE 
    WHEN end_date IS NULL THEN NULL 
    WHEN LTRIM(RTRIM(end_date)) = '' THEN NULL 
    ELSE TRY_CONVERT(DATE, LTRIM(RTRIM(end_date)), 120) 
	END AS end_date,    
	LTRIM(RTRIM(plan_tier)) AS plan_tier,
    TRY_CAST(seats AS INT) AS seats,
    TRY_CAST(mrr_amount AS DECIMAL(10,2)) AS mrr_amount,
    TRY_CAST(arr_amount AS DECIMAL(10,2)) AS arr_amount, 
    is_trial,
    upgrade_flag,
    downgrade_flag,
    churn_flag,
    LTRIM(RTRIM(billing_frequency)) AS billing_frequency, 
    auto_renew_flag
FROM stg_subscriptions
WHERE subscription_id IS NOT NULL
  AND LTRIM(RTRIM(subscription_id)) != '';


--- Checking for Null Values in Staging Support Tickets Table 
SELECT 
SUM (CASE WHEN ticket_id IS NULL THEN 1 ELSE 0 END) AS TicketIDNulls,
SUM (CASE WHEN account_id IS NULL THEN 1 ELSE 0 END) AS AccountIDNulls,
SUM (CASE WHEN submitted_at IS NULL THEN 1 ELSE 0 END) AS SubmittedNulls,
SUM (CASE WHEN closed_at IS NULL THEN 1 ELSE 0 END) AS ClosedAtNulls,
SUM (CASE WHEN resolution_time_hours IS NULL THEN 1 ELSE 0 END) AS ResolutionNulls,
SUM (CASE WHEN priority IS NULL THEN 1 ELSE 0 END) AS PriorityNulls,
SUM (CASE WHEN first_response_time_minutes IS NULL THEN 1 ELSE 0 END) AS ResponseTimeNulls,
SUM (CASE WHEN satisfaction_score IS NULL THEN 1 ELSE 0 END) AS SatScoreNulls,
SUM (CASE WHEN escalation_flag IS NULL THEN 1 ELSE 0 END) AS EscalationFlagNulls
FROM stg_support_tickets;


--- Load data from staging table into raw table 
TRUNCATE TABLE raw_support_tickets;

INSERT INTO raw_support_tickets(
    ticket_id,
    account_id,
    submitted_at,
    closed_at,
    resolution_time_hours,
    priority,
    first_response_time_minutes,
    satisfaction_score,
    escalation_flag
)

SELECT 
    LTRIM(RTRIM(ticket_id)) AS ticket_id,
    LTRIM(RTRIM(account_id)) AS account_id,
    TRY_CONVERT(DATE, submitted_at, 120) AS submitted_at,
    TRY_CONVERT(DATETIME, closed_at, 120) AS closed_at,
    LTRIM(RTRIM(resolution_time_hours)) AS resolution_time_hours,
    LTRIM(RTRIM(priority)) AS priority,
    LTRIM(RTRIM(first_response_time_minutes)) AS first_response_time_minutes,
     satisfaction_score,
    escalation_flag
FROM stg_support_tickets
WHERE ticket_id IS NOT NULL
  AND LTRIM(RTRIM(ticket_id)) != '';

---Row Counts to confirm successful load
SELECT 'raw_accounts' AS table_name, COUNT(*) AS row_count FROM raw_accounts
UNION ALL
SELECT 'raw_subscriptions', COUNT(*) FROM raw_subscriptions
UNION ALL
SELECT 'raw_churn_events', COUNT(*) FROM raw_churn_events
UNION ALL
SELECT 'raw_feature_usage', COUNT(*) FROM raw_feature_usage
UNION ALL
SELECT 'raw_support_tickets', COUNT(*) FROM raw_support_tickets;

SELECT *
FROM stg_support_tickets