-- =============================================
-- REVENUE INTELLIGENCE SYSTEM
-- File: 02_materialized_views.sql
-- Description: Materialized views for fast queries
-- =============================================

-- MATERIALIZED VIEW 1: Monthly Revenue Summary
CREATE MATERIALIZED VIEW mv_monthly_revenue_summary AS
SELECT
    DATE_TRUNC('month', rr.revenue_date)::DATE AS revenue_month,
    COUNT(DISTINCT rr.customer_id) AS paying_customers,
    COUNT(rr.revenue_id) AS total_transactions,
    SUM(rr.amount) AS total_revenue,
    SUM(rr.mrr_contribution) AS total_mrr,
    SUM(rr.arr_contribution) AS total_arr,
    SUM(CASE WHEN rr.revenue_type = 'New Business' THEN rr.amount ELSE 0 END) AS new_business_revenue,
    SUM(CASE WHEN rr.revenue_type = 'Expansion' THEN rr.amount ELSE 0 END) AS expansion_revenue,
    SUM(CASE WHEN rr.revenue_type = 'Renewal' THEN rr.amount ELSE 0 END) AS renewal_revenue,
    SUM(CASE WHEN rr.revenue_type = 'Churn' THEN rr.amount ELSE 0 END) AS churned_revenue,
    SUM(rr.amount) - LAG(SUM(rr.amount)) OVER (ORDER BY DATE_TRUNC('month', rr.revenue_date)) AS mom_change,
    ROUND(((SUM(rr.amount) - LAG(SUM(rr.amount)) OVER (
        ORDER BY DATE_TRUNC('month', rr.revenue_date)
    )) / NULLIF(LAG(SUM(rr.amount)) OVER (
        ORDER BY DATE_TRUNC('month', rr.revenue_date)
    ), 0)) * 100, 2) AS mom_growth_percent
FROM revenue_records rr
GROUP BY DATE_TRUNC('month', rr.revenue_date)
ORDER BY revenue_month;

CREATE UNIQUE INDEX idx_mv_monthly_revenue ON mv_monthly_revenue_summary(revenue_month);

-- MATERIALIZED VIEW 2: Pipeline Health
CREATE MATERIALIZED VIEW mv_pipeline_health AS
SELECT
    ds.stage_name,
    ds.stage_order,
    ds.probability,
    COUNT(d.deal_id) AS total_deals,
    SUM(d.deal_value) AS total_value,
    ROUND(SUM(d.deal_value * ds.probability / 100), 2) AS weighted_value,
    ROUND(AVG(d.deal_value), 2) AS avg_deal_value,
    ROUND(AVG(d.days_in_stage), 2) AS avg_days_in_stage,
    SUM(CASE WHEN d.days_in_stage > 30 THEN 1 ELSE 0 END) AS stagnant_deals,
    SUM(CASE WHEN d.expected_close_date < CURRENT_DATE THEN 1 ELSE 0 END) AS overdue_deals,
    SUM(CASE WHEN d.deal_source = 'Inbound' THEN 1 ELSE 0 END) AS inbound_deals,
    SUM(CASE WHEN d.deal_source = 'Outbound' THEN 1 ELSE 0 END) AS outbound_deals,
    SUM(CASE WHEN d.deal_source = 'Referral' THEN 1 ELSE 0 END) AS referral_deals,
    SUM(CASE WHEN d.deal_source = 'Partner' THEN 1 ELSE 0 END) AS partner_deals,
    SUM(CASE WHEN d.deal_source = 'Renewal' THEN 1 ELSE 0 END) AS renewal_deals
FROM deal_stages ds
LEFT JOIN deals d ON ds.stage_id = d.stage_id AND d.is_active = TRUE
GROUP BY ds.stage_name, ds.stage_order, ds.probability
ORDER BY ds.stage_order;

CREATE UNIQUE INDEX idx_mv_pipeline_health ON mv_pipeline_health(stage_name);

-- MATERIALIZED VIEW 3: Customer Segments
CREATE MATERIALIZED VIEW mv_customer_segments AS
SELECT
    c.customer_tier,
    c.industry,
    c.country,
    COUNT(DISTINCT c.customer_id) AS total_customers,
    COUNT(DISTINCT d.deal_id) AS total_deals,
    SUM(CASE WHEN ds.is_won THEN d.deal_value ELSE 0 END) AS total_revenue,
    ROUND(AVG(CASE WHEN ds.is_won THEN d.deal_value END), 2) AS avg_deal_value,
    ROUND(
        SUM(CASE WHEN ds.is_won THEN 1 ELSE 0 END)::NUMERIC /
        NULLIF(SUM(CASE WHEN ds.is_closed THEN 1 ELSE 0 END), 0) * 100
    , 2) AS win_rate_percent,
    SUM(CASE WHEN c.status = 'Churned' THEN 1 ELSE 0 END) AS churned_customers,
    SUM(CASE WHEN c.status = 'At-Risk' THEN 1 ELSE 0 END) AS at_risk_customers,
    ROUND(
        SUM(CASE WHEN c.status = 'Churned' THEN 1 ELSE 0 END)::NUMERIC /
        NULLIF(COUNT(DISTINCT c.customer_id), 0) * 100
    , 2) AS churn_rate_percent,
    SUM(rr.mrr_contribution) AS total_mrr,
    SUM(rr.arr_contribution) AS total_arr
FROM customers c
LEFT JOIN deals d ON c.customer_id = d.customer_id
LEFT JOIN deal_stages ds ON d.stage_id = ds.stage_id
LEFT JOIN revenue_records rr ON c.customer_id = rr.customer_id
GROUP BY c.customer_tier, c.industry, c.country
ORDER BY total_revenue DESC;

CREATE UNIQUE INDEX idx_mv_customer_segments
ON mv_customer_segments(customer_tier, industry, country);
