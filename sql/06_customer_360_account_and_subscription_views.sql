--- Description:
--- Customer 360 Account Level and Subscription Level Views
--- Serves as clear consise views into customers across datasets
--- Churn is calculated on the churn events level not subscription level

---Dependencies: 
--- 01_create_raw_tables.sql
--- 02_data_cleaning_from_staging_to_raw_tables.sql
--- 03_load_clean_from_raw.sql
--- 04_create_dim_tables.sql
--- 05_create_fact_tables.sql

CREATE OR ALTER VIEW vw_customer_360_account AS
WITH main_accounts AS(
	SELECT 
		a.account_id,
		a.account_name,
		a.country,
		a.signup_date,
		a.account_plan_tier,
		COUNT(s.subscription_id) AS total_subscriptions,
		SUM(COALESCE(s.mrr_amount, 0)) AS total_mrr_amount,
		SUM(COALESCE(s.arr_amount, 0)) AS total_arr_amount,
		MIN(s.start_date) AS first_subscription_start_date,
		MAX(s.end_date) AS last_subscription_end_date
	FROM clean_accounts a
	LEFT JOIN clean_subscriptions s ON a.account_id = s.account_id
	GROUP BY 		
		a.account_id,
		a.account_name,
		a.country,
		a.signup_date,
		a.account_plan_tier
	),

	feature_usage_metrics AS(
		SELECT
			a.account_id,
			COUNT(DISTINCT CASE 
				WHEN fu.usage_date <= DATEADD(DAY, 14, a.signup_date)
				THEN fu.feature_name
			END) AS feature_usage_first_14d,
			COUNT(DISTINCT CASE 
				WHEN fu.usage_date <= DATEADD(DAY, 30, a.signup_date)
				THEN fu.feature_name
			END) AS feature_usage_first_30d,
			CASE 
			WHEN COUNT(DISTINCT CASE WHEN fu.usage_date <= DATEADD(DAY, 14, a.signup_date) THEN fu.feature_name END) >= 1
			THEN 1 ELSE 0 END AS features_used_in_first_14d,
			CASE 
			WHEN COUNT(DISTINCT CASE WHEN fu.usage_date <= DATEADD(DAY, 30, a.signup_date) THEN fu.feature_name END) >= 1
			THEN 1 ELSE 0 END AS features_used_in_first_30d,
			CASE 
			WHEN COUNT(DISTINCT CASE WHEN fu.usage_date <= DATEADD(DAY, 30, a.signup_date) THEN fu.feature_name END) >= 5
			THEN 1 ELSE 0  END AS active_user
		FROM clean_accounts a
		LEFT JOIN clean_subscriptions s ON a.account_id = s.account_id
		LEFT JOIN clean_feature_usage fu ON s.subscription_id = fu.subscription_id
		GROUP BY a.account_id
		),
	
	support_metrics AS (
		SELECT 
			account_id,
			COUNT(ticket_id) AS total_tickets,
			COUNT(CASE WHEN priority IN ('high', 'urgent') THEN 1 END) AS high_priority_tickets,
			AVG(DATEDIFF(DAY, submitted_at, closed_at)) AS avg_resolution_days
		FROM clean_support_tickets
		GROUP BY account_id
		),

	churn_status AS(
		SELECT 
			ce.account_id,
			MAX(ce.churn_date) AS churn_date,
			MAX(CASE WHEN ce.churn_date IS NULL THEN 0 ELSE 1 END) AS is_churned,
			MAX(DATEDIFF(DAY,ce.churn_date, CURRENT_TIMESTAMP)) AS days_since_churn,
			MAX(ce.reason_code) AS reason_code,
			SUM(ce.refund_amount_usd) AS total_refund_amount_usd
		FROM clean_churn_events ce 
		GROUP BY ce.account_id
		)

	SELECT
		ma.account_id,
		ma.account_name,
		ma.country,
		ma.signup_date,
		ma.account_plan_tier,
		ma.total_subscriptions,
		ma.total_mrr_amount,
		ma.total_arr_amount,
		ma.first_subscription_start_date,
		ma.last_subscription_end_date,
		COALESCE(fum.feature_usage_first_14d, 0) AS feature_usage_first_14d,
		COALESCE(fum.feature_usage_first_30d, 0) AS feature_usage_first_30d,
		COALESCE(sm.total_tickets,0) AS total_tickets,
		COALESCE(sm.high_priority_tickets, 0) AS high_priority_tickets,
		COALESCE(sm.avg_resolution_days, 0) AS avg_resolution_days,
		COALESCE(fum.features_used_in_first_14d, 0) AS features_used_in_first_14d,
		COALESCE(fum.features_used_in_first_30d, 0) AS features_used_in_first_30d,
		COALESCE(fum.active_user, 0) AS active_user,
		cs.churn_date,
		cs.is_churned,
		cs.reason_code,
		COALESCE(cs.days_since_churn, 0) AS days_since_churn,
		COALESCE(cs.total_refund_amount_usd, 0) AS total_refund_amount_usd,
		(CASE WHEN COALESCE(fum.feature_usage_first_30d, 0) = 0 THEN 50 ELSE 0 END) +
		(CASE WHEN COALESCE(sm.high_priority_tickets, 0) >= 3 THEN 30 ELSE 0 END) +
		(CASE WHEN COALESCE(sm.total_tickets,0) > 10 THEN 20 ELSE 0 END) AS risk_score,
		CASE
			WHEN (CASE WHEN COALESCE(fum.feature_usage_first_30d, 0) = 0 THEN 50 ELSE 0 END) +
				 (CASE WHEN COALESCE(sm.high_priority_tickets, 0) >= 3 THEN 30 ELSE 0 END) +
				 (CASE WHEN COALESCE(sm.total_tickets,0) > 10 THEN 20 ELSE 0 END) >= 80 THEN 'Elevated Risk'
			WHEN (CASE WHEN COALESCE(fum.feature_usage_first_30d, 0) = 0 THEN 50 ELSE 0 END) +
				 (CASE WHEN COALESCE(sm.high_priority_tickets, 0) >= 3 THEN 30 ELSE 0 END) +
				 (CASE WHEN COALESCE(sm.total_tickets,0) > 10 THEN 20 ELSE 0 END) >= 50 THEN 'High-Risk'
			WHEN (CASE WHEN COALESCE(fum.feature_usage_first_30d, 0) = 0 THEN 50 ELSE 0 END) +
				 (CASE WHEN COALESCE(sm.high_priority_tickets, 0) >= 3 THEN 30 ELSE 0 END) +
				 (CASE WHEN COALESCE(sm.total_tickets,0) > 10 THEN 20 ELSE 0 END) >= 20 THEN 'Moderate Risk'
			ELSE 'Low Risk'
		END AS risk_level,
			DATEDIFF(MONTH, ma.signup_date, COALESCE(cs.churn_date, GETDATE())) AS account_lifespan_months,
				CASE 
					WHEN DATEDIFF(MONTH, ma.signup_date, COALESCE(cs.churn_date, GETDATE())) <= 12 THEN 'New Account (1 year or less)'
					WHEN DATEDIFF(MONTH, ma.signup_date, COALESCE(cs.churn_date, GETDATE())) <= 36 THEN 'Emerging Account (1-3 years)'
					WHEN DATEDIFF(MONTH, ma.signup_date, COALESCE(cs.churn_date, GETDATE())) <= 60 THEN 'Established Account (3-5 years)'
					WHEN DATEDIFF(MONTH, ma.signup_date, COALESCE(cs.churn_date, GETDATE())) > 60 THEN 'Legacy Account (5+ years)'
					ELSE 'Active Account'
				END AS account_lifespan_category,
				CASE 
					WHEN cs.is_churned = 1 
					THEN DATEDIFF(MONTH, ma.signup_date, cs.churn_date)
					ELSE NULL
				END AS months_to_churn,
				CASE 
					WHEN cs.is_churned = 1 AND DATEDIFF(MONTH, ma.signup_date, cs.churn_date) <= 6 THEN 'Early (6 months or less)'
					WHEN cs.is_churned = 1 AND DATEDIFF(MONTH, ma.signup_date, cs.churn_date) <= 12 THEN 'First Year (12 months or less)'
					WHEN cs.is_churned = 1 AND DATEDIFF(MONTH, ma.signup_date, cs.churn_date) <= 18 THEN 'Mid Churn (13-18 months)'
					WHEN cs.is_churned = 1 AND DATEDIFF(MONTH, ma.signup_date, cs.churn_date) <= 24 THEN 'Second Year (19-24 months)'
					WHEN cs.is_churned = 1 AND DATEDIFF(MONTH, ma.signup_date, cs.churn_date) > 24 THEN 'Late Churn (2+ years)'
					ELSE 'Active'
				END AS churn_category,
				CASE 
					WHEN cs.is_churned = 1 
					THEN DATEDIFF(MONTH, cs.churn_date, GETDATE())
					ELSE NULL
				END AS months_since_churn,
				CASE 
					WHEN cs.is_churned = 1 
					THEN ma.total_mrr_amount * DATEDIFF(MONTH, cs.churn_date, GETDATE())
					ELSE 0
				END AS estimated_mrr_churned,
			CASE 
				WHEN cs.is_churned = 1 
				THEN ma.total_arr_amount * DATEDIFF(MONTH, cs.churn_date, GETDATE())
				ELSE 0
			END AS estimated_arr_churned,
		DATEFROMPARTS(YEAR(ma.signup_date), MONTH(ma.signup_date), 1) AS cohort_month,
		DATEDIFF(MONTH, DATEFROMPARTS(YEAR(ma.signup_date), MONTH(ma.signup_date), 1), GETDATE()) AS cohort_number,
		CASE 
			WHEN DATEDIFF(MONTH, DATEFROMPARTS(YEAR(ma.signup_date), MONTH(ma.signup_date), 1), GETDATE()) <= 3 THEN '0-3 Months'
			WHEN DATEDIFF(MONTH, DATEFROMPARTS(YEAR(ma.signup_date), MONTH(ma.signup_date), 1), GETDATE()) <= 6 THEN '3-6 Months'
			WHEN DATEDIFF(MONTH, DATEFROMPARTS(YEAR(ma.signup_date), MONTH(ma.signup_date), 1), GETDATE()) <= 12 THEN '6-12 Months'
			WHEN DATEDIFF(MONTH, DATEFROMPARTS(YEAR(ma.signup_date), MONTH(ma.signup_date), 1), GETDATE()) <= 24 THEN '12-24 Months'
			ELSE '24+ Months'
		END AS cohort_bucket,
			CASE 
				WHEN cs.is_churned = 1 
				THEN DATEDIFF(MONTH, DATEFROMPARTS(YEAR(ma.signup_date), MONTH(ma.signup_date), 1), cs.churn_date)
				ELSE NULL
			END AS cohort_age
	FROM main_accounts ma
	LEFT JOIN feature_usage_metrics fum ON ma.account_id = fum.account_id
	LEFT JOIN support_metrics sm ON ma.account_id = sm.account_id
	LEFT JOIN churn_status cs ON ma.account_id = cs.account_id

----------------------------------------------------------------------------------------------------
		
CREATE OR ALTER VIEW vw_customer_360_subscription AS
WITH main_accounts AS(
	SELECT 
		a.account_id,
		a.account_name,
		a.country,
		a.signup_date,
		a.account_plan_tier,
		s.subscription_id,
		s.mrr_amount,
		s.arr_amount,
		s.start_date,
		s.end_date,
		s.is_active_subscription
	FROM clean_accounts a
	LEFT JOIN clean_subscriptions s ON a.account_id = s.account_id
	),

	feature_usage_metrics AS(
		SELECT
			a.account_id,
			COUNT(DISTINCT CASE 
				WHEN fu.usage_date <= DATEADD(DAY, 14, a.signup_date)
				THEN fu.feature_name
			END) AS feature_usage_first_14d,
			COUNT(DISTINCT CASE 
				WHEN fu.usage_date <= DATEADD(DAY, 30, a.signup_date)
				THEN fu.feature_name
			END) AS feature_usage_first_30d,
			CASE 
			WHEN COUNT(DISTINCT CASE WHEN fu.usage_date <= DATEADD(DAY, 14, a.signup_date) THEN fu.feature_name END) >= 1
			THEN 1 ELSE 0 END AS features_used_in_first_14d,
			CASE 
			WHEN COUNT(DISTINCT CASE WHEN fu.usage_date <= DATEADD(DAY, 30, a.signup_date) THEN fu.feature_name END) >= 1
			THEN 1 ELSE 0 END AS features_used_in_first_30d,
			CASE 
			WHEN COUNT(DISTINCT CASE WHEN fu.usage_date <= DATEADD(DAY, 30, a.signup_date) THEN fu.feature_name END) >= 5
			THEN 1 ELSE 0  END AS active_user
		FROM clean_accounts a
		LEFT JOIN clean_subscriptions s ON a.account_id = s.account_id
		LEFT JOIN clean_feature_usage fu ON s.subscription_id = fu.subscription_id
		GROUP BY a.account_id
		),
	
	support_metrics AS (
		SELECT 
			account_id,
			COUNT(ticket_id) AS total_tickets,
			COUNT(CASE WHEN priority IN ('high', 'urgent') THEN 1 END) AS high_priority_tickets,
			AVG(DATEDIFF(DAY, submitted_at, closed_at)) AS avg_resolution_days
		FROM clean_support_tickets
		GROUP BY account_id
		),

	churn_status AS(
		SELECT 
			ce.account_id,
			ce.churn_date,
			CASE WHEN ce.churn_date IS NULL THEN 0 ELSE 1 END AS is_churned,
			DATEDIFF(DAY,churn_date, CURRENT_TIMESTAMP) AS days_since_churn,
			ce.reason_code,
			ce.refund_amount_usd
		FROM clean_churn_events ce 
		LEFT JOIN clean_subscriptions s ON ce.account_id = s.account_id
		)

	SELECT
		ma.account_id,
		ma.account_name,
		ma.country,
		ma.signup_date,
		ma.account_plan_tier,
		ma.subscription_id,
		ma.mrr_amount,
		ma.arr_amount,
		ma.start_date,
		ma.end_date,
		ma.is_active_subscription,
		COALESCE(fum.feature_usage_first_14d, 0) AS feature_usage_first_14d,
		COALESCE(fum.feature_usage_first_30d, 0) AS feature_usage_first_30d,
		COALESCE(sm.total_tickets,0) AS total_tickets,
		COALESCE(sm.high_priority_tickets, 0) AS high_priority_tickets,
		COALESCE(sm.avg_resolution_days, 0) AS avg_resolution_days,
		COALESCE(fum.features_used_in_first_14d, 0) AS features_used_in_first_14d,
		COALESCE(fum.features_used_in_first_30d, 0) AS features_used_in_first_30d,
		COALESCE(fum.active_user, 0) AS active_user,
		cs.churn_date,
		cs.is_churned,
		cs.reason_code,
		COALESCE(cs.days_since_churn, 0) AS days_since_churn,
		(CASE WHEN COALESCE(fum.feature_usage_first_30d, 0) = 0 THEN 50 ELSE 0 END) +
		(CASE WHEN COALESCE(sm.high_priority_tickets, 0) >= 3 THEN 30 ELSE 0 END) +
		(CASE WHEN COALESCE(sm.total_tickets,0) > 10 THEN 20 ELSE 0 END) AS risk_score,
		CASE
			WHEN (CASE WHEN COALESCE(fum.feature_usage_first_30d, 0) = 0 THEN 50 ELSE 0 END) +
				 (CASE WHEN COALESCE(sm.high_priority_tickets, 0) >= 3 THEN 30 ELSE 0 END) +
				 (CASE WHEN COALESCE(sm.total_tickets,0) > 10 THEN 20 ELSE 0 END) >= 80 THEN 'Elevated Risk'
			WHEN (CASE WHEN COALESCE(fum.feature_usage_first_30d, 0) = 0 THEN 50 ELSE 0 END) +
				 (CASE WHEN COALESCE(sm.high_priority_tickets, 0) >= 3 THEN 30 ELSE 0 END) +
				 (CASE WHEN COALESCE(sm.total_tickets,0) > 10 THEN 20 ELSE 0 END) >= 50 THEN 'High-Risk'
			WHEN (CASE WHEN COALESCE(fum.feature_usage_first_30d, 0) = 0 THEN 50 ELSE 0 END) +
				 (CASE WHEN COALESCE(sm.high_priority_tickets, 0) >= 3 THEN 30 ELSE 0 END) +
				 (CASE WHEN COALESCE(sm.total_tickets,0) > 10 THEN 20 ELSE 0 END) >= 20 THEN 'Moderate Risk'
			ELSE 'Low Risk'
		END AS risk_level,
			DATEDIFF(MONTH, ma.signup_date, COALESCE(cs.churn_date, GETDATE())) AS account_lifespan_months,
				CASE 
					WHEN DATEDIFF(MONTH, ma.signup_date, COALESCE(cs.churn_date, GETDATE())) <= 12 THEN 'New Account (1 year or less)'
					WHEN DATEDIFF(MONTH, ma.signup_date, COALESCE(cs.churn_date, GETDATE())) <= 36 THEN 'Emerging Account (1-3 years)'
					WHEN DATEDIFF(MONTH, ma.signup_date, COALESCE(cs.churn_date, GETDATE())) <= 60 THEN 'Established Account (3-5 years)'
					WHEN DATEDIFF(MONTH, ma.signup_date, COALESCE(cs.churn_date, GETDATE())) > 60 THEN 'Legacy Account (5+ years)'
					ELSE 'Active Account'
				END AS account_lifespan_category,
				CASE 
					WHEN cs.is_churned = 1 
					THEN DATEDIFF(MONTH, ma.signup_date, cs.churn_date)
					ELSE NULL
				END AS months_to_churn,
				CASE 
					WHEN cs.is_churned = 1 AND DATEDIFF(MONTH, ma.signup_date, cs.churn_date) <= 6 THEN 'Early (6 months or less)'
					WHEN cs.is_churned = 1 AND DATEDIFF(MONTH, ma.signup_date, cs.churn_date) <= 12 THEN 'First Year (12 months or less)'
					WHEN cs.is_churned = 1 AND DATEDIFF(MONTH, ma.signup_date, cs.churn_date) <= 18 THEN 'Mid Churn (13-18 months)'
					WHEN cs.is_churned = 1 AND DATEDIFF(MONTH, ma.signup_date, cs.churn_date) <= 24 THEN 'Second Year (19-24 months)'
					WHEN cs.is_churned = 1 AND DATEDIFF(MONTH, ma.signup_date, cs.churn_date) > 24 THEN 'Late Churn (2+ years)'
					ELSE 'Active'
				END AS churn_category,
				CASE 
					WHEN cs.is_churned = 1 
					THEN DATEDIFF(MONTH, cs.churn_date, GETDATE())
					ELSE NULL
				END AS months_since_churn,
				CASE 
					WHEN cs.is_churned = 1 
					THEN ma.mrr_amount * DATEDIFF(MONTH, cs.churn_date, GETDATE())
					ELSE 0
				END AS estimated_mrr_churned,
			CASE 
				WHEN cs.is_churned = 1 
				THEN ma.arr_amount * DATEDIFF(MONTH, cs.churn_date, GETDATE())
				ELSE 0
			END AS estimated_arr_churned,
		DATEFROMPARTS(YEAR(ma.signup_date), MONTH(ma.signup_date), 1) AS cohort_month,
		DATEDIFF(MONTH, DATEFROMPARTS(YEAR(ma.signup_date), MONTH(ma.signup_date), 1), GETDATE()) AS cohort_number,
		CASE 
			WHEN DATEDIFF(MONTH, DATEFROMPARTS(YEAR(ma.signup_date), MONTH(ma.signup_date), 1), GETDATE()) <= 3 THEN '0-3 Months'
			WHEN DATEDIFF(MONTH, DATEFROMPARTS(YEAR(ma.signup_date), MONTH(ma.signup_date), 1), GETDATE()) <= 6 THEN '3-6 Months'
			WHEN DATEDIFF(MONTH, DATEFROMPARTS(YEAR(ma.signup_date), MONTH(ma.signup_date), 1), GETDATE()) <= 12 THEN '6-12 Months'
			WHEN DATEDIFF(MONTH, DATEFROMPARTS(YEAR(ma.signup_date), MONTH(ma.signup_date), 1), GETDATE()) <= 24 THEN '12-24 Months'
			ELSE '24+ Months'
		END AS cohort_bucket,
			CASE 
				WHEN cs.is_churned = 1 
				THEN DATEDIFF(MONTH, DATEFROMPARTS(YEAR(ma.signup_date), MONTH(ma.signup_date), 1), cs.churn_date)
				ELSE NULL
			END AS cohort_age	
	FROM main_accounts ma
	LEFT JOIN feature_usage_metrics fum ON ma.account_id = fum.account_id
	LEFT JOIN support_metrics sm ON ma.account_id = sm.account_id
	LEFT JOIN churn_status cs ON ma.account_id = cs.account_id
		




