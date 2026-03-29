--- Description:
--- Creates dimension tables to be used in revenue, churn, cohort and customer health fact models. 

--- Design Rules:
--- - Clear grain per dimension
--- - No metrics in dimensions
--- - Natural keys preserved; surrogate keys introduced where appropriate
--- - Dimensions sourced exclusively from clean layer

---Dependencies: 
--- 01_create_raw_tables.sql
--- 02_data_cleaning_from_staging_to_raw_tables.sql
--- 03_load_clean_from_raw.sql

--- Dim Account 
--- Grain: One row per account
--- Natural key: account_id
DROP TABLE IF EXISTS dim_account
GO
SELECT
    account_id,
    industry,
    country,
    signup_date,
	referral_source,
    account_plan_tier
INTO dim_account
FROM clean_accounts 
GO

--- Dim Subscription
--- Grain: One row per subscription
DROP TABLE IF EXISTS dim_subscription
GO
SELECT 
	subscription_id,
    account_id,
    plan_tier,
	billing_frequency,
	seats,
    start_date,
    end_date,
    is_active_subscription
INTO dim_subscription
FROM clean_subscriptions
GO

--- Dim Feature
--- Grain: One row per feature
--- Surrogate key generated for feature dimension
DROP TABLE IF EXISTS dim_feature
GO
SELECT 
	IDENTITY(INT,1,1) AS feature_key,
	feature_name, 
	is_beta_feature
INTO dim_feature
FROM clean_feature_usage
GROUP BY feature_name, is_beta_feature;
GO

--- Dim Date
--- Grain: One row per calendar day
DROP TABLE IF EXISTS dim_date;
GO
WITH DateRange AS (
    SELECT CAST('2023-01-01' AS DATE) AS CalendarDate
    UNION ALL
    SELECT DATEADD(DAY, 1, CalendarDate)
    FROM DateRange
    WHERE CalendarDate < '2025-12-31' 
)
SELECT 
    CAST(FORMAT(CalendarDate, 'yyyyMMdd') AS INT) AS date_key,
    CalendarDate AS [date],
    YEAR(CalendarDate) AS [year],
    MONTH(CalendarDate) AS [month],
    DATENAME(MONTH, CalendarDate) AS month_name,
    CONCAT(DATENAME(MONTH, CalendarDate), ' ', YEAR(CalendarDate)) AS month_year,
    YEAR(CalendarDate) * 100 + MONTH(CalendarDate) AS month_year_sort, 
    DATENAME(WEEKDAY, CalendarDate) AS day_name,
    DATEPART(QUARTER, CalendarDate) AS [quarter],
    DATEPART(WEEK, CalendarDate) AS [week],
    DATEPART(ISO_WEEK, CalendarDate) AS iso_week,
    DATEPART(WEEKDAY, CalendarDate) AS [day_of_week]
INTO dim_date 
FROM DateRange
OPTION (MAXRECURSION 0);
GO
--- Dim Reason
--- Grain: One row per churn reason
DROP TABLE IF EXISTS dim_reason
GO
SELECT
	IDENTITY(INT,1,1) AS reason_key,
	reason_code
INTO dim_reason
FROM clean_churn_events
WHERE reason_code IS NOT NULL
GO