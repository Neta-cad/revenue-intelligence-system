# Revenue Intelligence System 🧠

A production-grade Revenue Intelligence System built on PostgreSQL/Supabase that unifies CRM, activity, and billing data to surface actionable sales insights.

---

## 🎯 Revenue Problems Solved

- Inaccurate pipeline forecasting
- Missed churn signals before accounts are lost
- Poor visibility into deal health and stagnation
- Missed upsell and expansion opportunities
- Inconsistent sales rep performance tracking

---

## 🏗️ Architecture
CRM Data          Activity Logs       Billing System
(customers,       (activities,        (revenue_records,
contacts,         churn_signals)      forecasts)
deals)
\                 |                  /
\                |                 /
─────────────────────────────────
PostgreSQL / Supabase
─────────────────────────────────
/                |                 
/                 |                  
Analytical Views   Materialized Views   Functions
(pipeline_summary, (mv_pipeline_health, (get_deal_health_score,
deal_risk_scores,  mv_monthly_revenue,  get_revenue_forecast,
customer_health)   mv_customer_segments)get_customer_lifetime_value)



---

## 📊 Database Schema

| Table | Purpose | Source |
|---|---|---|
| `customers` | Company accounts | CRM Export |
| `contacts` | People within accounts | CRM Export |
| `deals` | Sales opportunities | CRM Export |
| `deal_stages` | Pipeline stage definitions | CRM Export |
| `products` | Product catalog | CRM Export |
| `deal_products` | Products per deal | CRM Export |
| `activities` | Calls, emails, meetings | Activity Logs |
| `revenue_records` | Actual revenue collected | Billing System |
| `forecasts` | Monthly forecast snapshots | Forecast System |
| `churn_signals` | At-risk account flags | Usage Logs |
| `cohorts` | Customer groupings | Analytical |
| `audit_logs` | Full audit trail | System |

---

## 🧠 Intelligence Capabilities

| # | Capability | Implementation |
|---|---|---|
| 1 | Pipeline scoring and risk detection | deal_risk_scores view + Query 1 |
| 2 | Revenue forecasting | get_revenue_forecast() function + Query 3 |
| 3 | Cohort analysis and MRR/ARR trends | mv_monthly_revenue_summary + Query 4 and 5 |
| 4 | Opportunity scoring | Query 9 upsell scoring |
| 5 | Churn and expansion signals | churn_signals table + Query 6 |
| 6 | Funnel conversion and velocity | Query 2 |
| 7 | Anomaly detection | Query 10 Z-score analysis |
| 8 | Customer segmentation | mv_customer_segments + Query 11 |

---

## 🔌 Database Connection

Platform: Supabase (PostgreSQL)

| Field | Value |
|---|---|
| Host | db.YOUR-PROJECT-REF.supabase.co |
| Port | 5432 |
| Database | postgres |
| Username | demo_judge |
| Password | RevIQ2024Judge! |

To get your exact host URL go to Supabase → Settings → Database → Connection String.

---

## 🚀 Sample Insight Queries

1. Check pipeline health instantly
SELECT * FROM pipeline_summary;

2. See all high risk deals
SELECT * FROM deal_risk_scores
WHERE risk_level IN ('Critical', 'High')
ORDER BY risk_score DESC;

3. Get revenue forecast for next 3 months
SELECT * FROM get_revenue_forecast(3);

4. See customer health scores
SELECT * FROM customer_health
ORDER BY health_score ASC;

5. Check MRR trends
SELECT * FROM mv_monthly_revenue_summary;

6. Get CLV for any customer
SELECT * FROM get_customer_lifetime_value(
    (SELECT customer_id FROM customers WHERE company_name = 'Nexus Corp')
);

---

## 📁 Repository Structure

revenue-intelligence-system/
├── schema/
│   ├── 01_create_tables.sql       12 core tables
│   ├── 02_indexes.sql             28 performance indexes
│   └── 03_triggers.sql            Audit and churn detection
├── seeds/
│   ├── 01_deal_stages.sql         7 pipeline stages
│   ├── 02_customers.sql           100 companies
│   ├── 03_contacts.sql            200 contacts
│   ├── 04_products.sql            12 products
│   ├── 05_deals.sql               200+ deals
│   ├── 06_deal_products.sql       Deal-product links
│   ├── 07_activities.sql          100 activities
│   ├── 08_revenue_records.sql     12 months revenue
│   ├── 09_forecasts.sql           36 forecast records
│   ├── 10_churn_signals.sql       50 risk signals
│   └── 11_cohorts.sql             33 cohort groups
├── analytics/
│   ├── 01_views.sql               5 analytical views
│   ├── 02_materialized_views.sql  3 materialized views
│   ├── 03_functions.sql           3 custom functions
│   └── 04_queries.sql             12 analytical queries
├── security/
│   └── 01_rls_policies.sql        RLS and demo user
├── diagrams/
│   └── er_diagram.png             ER diagram
└── README.md

---

## 🛠️ Tech Stack

| Component | Technology | Reason |
|---|---|---|
| Database | PostgreSQL 15 | Window functions, CTEs, JSONB |
| Hosting | Supabase | Live access, RLS, free tier |
| Views | PostgreSQL Views | Real-time analytical queries |
| Materialized Views | PostgreSQL | Pre-computed fast queries |
| Functions | PL/pgSQL | Custom revenue intelligence logic |
| Security | Row Level Security | Production-grade data protection |

---

## 📈 Real-World Revenue Impact

This system enables RevOps and sales leaders to:

- Reduce pipeline leakage by flagging stagnant deals before they die
- Improve forecast accuracy using historical win rates vs committed values
- Prevent churn by detecting at-risk accounts 30 to 60 days early
- Grow revenue by identifying upsell opportunities automatically
- Coach reps using data-driven performance leaderboards

---

## 👤 Author

Akingboye Ayomide
Remote Hustle Competition — Stage 2
Revenue Intelligence System







