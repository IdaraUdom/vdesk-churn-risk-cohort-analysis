**Project Overview**

RaveStack VDesk had an unsustainable 70.4% account churn rate and $1.3 billion in lost annual recurring revenue (ARR). Accounts are discontinuing subscriptions at multiple stages of the lifecycle rendering the sales pipeline unstable. Stakeholders commissioned this analysis to clearly understand the root causes of churn and retention strategy focus points. As such the objective of this analysis was to identify key churn drivers and pinpoint where interventions would have the greatest impact. 

**Analytical Approach**

I led an end-to-end analysis covering:

**Data Engineering [SQL Server]**

Designed and implemented ETL pipelines which moved raw data from staging to normalized raw tables and then cleaned tables with standardised business logic.
Built  a star schema approach with fact and dimension tables to enable flexible analysis across accounts, subscriptions, and support operations
Created Customer 360 views which aggregated revenue, churn, and support metrics while maintaining clear analyses
Implemented data quality validation with null checks and standardization

**Analytics and Modeling**

Developed a weighted risk scoring system which took signals from feature usage and support ticket patterns
Built cohort analysis logic to track retention rates and revenue concentration over time
Created churn stage segmentation to identify churn driver patterns by account age

**Visualisation and Reporting [Power BI]**

Designed interactive dashboards to enable ongoing monitoring of account health and recurring revenue
Produced a comprehensive insights report with executive summary, findings, and strategic recommendations

**Key Findings**

Churn drivers shift by account age
Pricing sensitivity drives churn in early churn accounts (0–12 months). Support dissatisfaction drives churn in late churn accounts (13–24+ months)

Risk model fails to predict churn risk
Low Risk accounts made up 241 of 352 churned accounts; only one High-Risk account existed

$96 million in lost ARR is concentrated in mature cohorts
12–24 month and 24+ month cohorts represent $96M in churned ARR

Support dissatisfaction persists despite faster resolution times
Later stage churn accounts had faster resolution times but still churned due to support dissatisfaction

Retention issues result in higher cost per acquisition despite strong acquisition
Despite active subscribers repeated recoveries, the 29.6% retention rate results in higher cost per acquisition despite strong acquisition

**Strategic Recommendations**

**Target retention strategies by account age**

Price sensitivity is the leading churn reason in early churn accounts,thus interventions driving customer perceived value and addressing price sensitivity could be effective. Support dissatisfaction is the leading churn reason for late churn accounts, followed by feature gaps. Interventions addressing these could be effective alongside price and value interventions as price sensitivity remains a churn factor in late churn. 

**Audit high-priority ticket content**

A deeper audit of high-priority ticket content, particularly for late stage accounts, could identify common pain points and inform targeted retention strategies.

**Redesign risk scoring**

Additional behavioural data and further investigation could identify more effective churn risk flags.

**Create retention strategies targeted to later stage cohorts**

Later stage cohorts contributed $96 million in ARR from churned accounts alone. Retention strategies targeting later stage cohorts could recover and stabilise revenue with lower acquisition costs than sourcing new customers.

**Audit Basic tier accounts**

Basic tier accounts accounted for an ARR loss of $630 million despite having the lowest churn rate of all tiers, an audit could identify revenue drivers within existing or churned accounts. 


**Estimated Business Impact**

Improving the retention rate by 10% within mature cohorts would recover approximately $9.6 million in ARR. Increasing the retention rate by 10% would decrease the amount of new accounts needed to sustain revenue by approximately 25%. A 20% reduction in support driven churn among late stage accounts would recover approximately $8.4 million in ARR annually.  Refining risk signals could enable early churn risk identification. A reduction of churn by 5-10% across all risk segments could preserve from $65-130 million based on current churn levels. Preserving 5% of churned Basic tier ARR would recover $31.5 million in annual revenue. All together the total estimated business impact is $110-180 million in recovered or preserved ARR.

**Repository Contents**

`sql/` | All SQL scripts: table creation, ETL, clean tables, Customer 360 views

`report/` | Final insights report (PDF) 

`power-bi/` | Power BI dashboard file 

**Skills Used** 

SQL: Complex joins, window functions, CTEs, ETL design, dimensional modeling

Data Modeling: Star schema, fact/dimension tables, view creation

Analytics: Cohort analysis, risk scoring, churn driver segmentation

Visualization: Power BI dashboard design, executive reporting

Business Acumen: Stakeholder communication, strategic recommendations, ROI estimation

