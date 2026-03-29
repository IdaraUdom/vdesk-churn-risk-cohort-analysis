--- Description:
--- RaveStack database creation and creation of normalised raw tables 


CREATE DATABASE RaveStackVDesk

DROP TABLE IF EXISTS raw_accounts 
CREATE TABLE raw_accounts(
	account_id VARCHAR(30) PRIMARY KEY,
	account_name VARCHAR(100),
	industry VARCHAR(100),
	country CHAR(2),
	signup_date DATE,
	referral_source VARCHAR(100),
	plan_tier VARCHAR(50),
	seats INT,
	is_trial BIT,
	churn_flag BIT,
	created_at DATETIME DEFAULT GETDATE()
	);

DROP TABLE IF EXISTS raw_churn_events
CREATE TABLE raw_churn_events(
	churn_event_id VARCHAR(30) PRIMARY KEY,
    account_id VARCHAR(30) NOT NULL,
    churn_date DATE,
    reason_code VARCHAR(50),
    refund_amount_usd DECIMAL(10,2),
    preceding_upgrade_flag BIT,
    preceding_downgrade_flag BIT,
    is_reactivation BIT,
    feedback_text VARCHAR(1000),
	created_at DATETIME DEFAULT GETDATE()
    );

DROP TABLE IF EXISTS raw_subscriptions
CREATE TABLE raw_subscriptions (
    subscription_id VARCHAR(30) PRIMARY KEY,
    account_id VARCHAR(30) NOT NULL,  
    start_date DATE,
    end_date DATE,                  
    plan_tier VARCHAR(50),
    seats INT,
    mrr_amount DECIMAL(10,2),
    arr_amount DECIMAL(10,2),
    is_trial BIT,
    upgrade_flag BIT,
    downgrade_flag BIT,
    churn_flag BIT,
    billing_frequency VARCHAR(25),    
    auto_renew_flag BIT,
	created_at DATETIME DEFAULT GETDATE()
);

DROP TABLE IF EXISTS raw_feature_usage
CREATE TABLE raw_feature_usage (
    usage_id VARCHAR(30),     
    subscription_id VARCHAR(30) NOT NULL,
    usage_date DATE,
    feature_name VARCHAR(100),
    usage_count INT,
    usage_duration_secs INT,                    
    error_count TINYINT,                       
    is_beta_feature BIT,
	created_at DATETIME DEFAULT GETDATE()
);

DROP TABLE IF EXISTS raw_support_tickets
CREATE TABLE raw_support_tickets (
    ticket_id VARCHAR(30) PRIMARY KEY,
    account_id VARCHAR(30) NOT NULL,
    submitted_at DATE,                          
    closed_at DATETIME,
    resolution_time_hours INT,                 
    priority VARCHAR(20),
    first_response_time_minutes INT,
    satisfaction_score TINYINT,
    escalation_flag BIT,
	created_at DATETIME DEFAULT GETDATE()
);