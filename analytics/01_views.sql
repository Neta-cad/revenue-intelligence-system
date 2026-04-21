-- =============================================
-- REVENUE INTELLIGENCE SYSTEM
-- File: 01_views.sql
-- Description: Analytical views
-- =============================================

-- VIEW 1: Pipeline Summary
CREATE OR REPLACE VIEW pipeline_summary AS
SELECT
    ds.stage_name,
    ds.stage_order,
    ds.probability,
    COUNT(d.deal_id) AS total_deals,
    SUM(d.deal_value) AS total_pipeline_value,
    ROUND(SUM(d.deal_value * ds.probability / 100), 2) AS weighted_pipeline_value,
    ROUND(AVG(d.deal_value), 2) AS avg_deal_value,
    ROUND(AVG(d.days_in_stage), 2) AS avg_days_in_stage,
    SUM(CASE WHEN d.days_in_stage > 30 THEN 1 ELSE 0 END) AS stagnant_deals,
    SUM(CASE WHEN d.expected_close_date < CURRENT_DATE AND d.is_active THEN 1 ELSE 0 END) AS overdue_deals
FROM deal_stages ds
LEFT JOIN deals d ON ds.stage_id = d.stage_id AND d.is_active = TRUE
GROUP BY ds.stage_name, ds.stage_order, ds.probability
ORDER BY ds.stage_order;

-- VIEW 2: Deal Risk Scores
CREATE OR REPLACE VIEW deal_risk_scores AS
SELECT
    d.deal_id,
    d.deal_name,
    c.company_name,
    ds.stage_name,
    d.deal_value,
    d.days_in_stage,
    d.expected_close_date,
    d.assigned_rep,
    CURRENT_DATE - d.expected_close_date AS days_overdue,
    COUNT(a.activity_id) AS total_activities,
    MAX(a.activity_date) AS last_activity_date,
    CURRENT_DATE - MAX(a.activity_date::DATE) AS days_since_last_activity,
    SUM(CASE WHEN a.outcome = 'Negative' THEN 1 ELSE 0 END) AS negative_signals,
    SUM(CASE WHEN a.outcome = 'No Response' THEN 1 ELSE 0 END) AS no_response_count,
    CASE
        WHEN CURRENT_DATE - MAX(a.activity_date::DATE) > 45 THEN 'Critical'
        WHEN CURRENT_DATE - MAX(a.activity_date::DATE) > 30 THEN 'High'
        WHEN CURRENT_DATE - d.expected_close_date > 14 THEN 'High'
        WHEN d.days_in_stage > 45 THEN 'High'
        WHEN CURRENT_DATE - MAX(a.activity_date::DATE) > 14 THEN 'Medium'
        WHEN d.days_in_stage > 30 THEN 'Medium'
        WHEN SUM(CASE WHEN a.outcome = 'Negative' THEN 1 ELSE 0 END) > 1 THEN 'Medium'
        ELSE 'Low'
    END AS risk_level,
    ROUND(
        (COALESCE(CURRENT_DATE - MAX(a.activity_date::DATE), 0) * 0.4) +
        (d.days_in_stage * 0.3) +
        (COALESCE(CURRENT_DATE - d.expected_close_date, 0) * 0.2) +
        (COALESCE(SUM(CASE WHEN a.outcome = 'Negative' THEN 1 ELSE 0 END), 0) * 10 * 0.1)
    , 2) AS risk_score
FROM deals d
JOIN customers c ON d.customer_id = c.customer_id
JOIN deal_stages ds ON d.stage_id = ds.stage_id
LEFT JOIN activities a ON d.deal_id = a.deal_id
WHERE d.is_active = TRUE
GROUP BY
    d.deal_id, d.deal_name, c.company_name,
    ds.stage_name, d.deal_value, d.days_in_stage,
    d.expected_close_date, d.assigned_rep
ORDER BY risk_score DESC;

-- VIEW 3: Revenue By Month
CREATE OR REPLACE VIEW revenue_by_month AS
SELECT
    DATE_TRUNC('month', rr.revenue_date) AS revenue_month,
    rr.revenue_type,
    COUNT(rr.revenue_id) AS total_transactions,
    SUM(rr.amount) AS total_revenue,
    ROUND(AVG(rr.amount), 2) AS avg_transaction_value,
    SUM(rr.mrr_contribution) AS total_mrr,
    SUM(rr.arr_contribution) AS total_arr,
    SUM(CASE WHEN rr.payment_status = 'Paid' THEN rr.amount ELSE 0 END) AS collected_revenue,
    SUM(CASE WHEN rr.payment_status = 'Overdue' THEN rr.amount ELSE 0 END) AS overdue_revenue,
    SUM(CASE WHEN rr.payment_status = 'Pending' THEN rr.amount ELSE 0 END) AS pending_revenue,
    ROUND(SUM(rr.amount) - LAG(SUM(rr.amount)) OVER (
        PARTITION BY rr.revenue_type
        ORDER BY DATE_TRUNC('month', rr.revenue_date)
    ), 2) AS mom_revenue_change,
    ROUND(((SUM(rr.amount) - LAG(SUM(rr.amount)) OVER (
        PARTITION BY rr.revenue_type
        ORDER BY DATE_TRUNC('month', rr.revenue_date)
    )) / NULLIF(LAG(SUM(rr.amount)) OVER (
        PARTITION BY rr.revenue_type
        ORDER BY DATE_TRUNC('month', rr.revenue_date)
    ), 0)) * 100, 2) AS mom_growth_percent
FROM revenue_records rr
GROUP BY
    DATE_TRUNC('month', rr.revenue_date),
    rr.revenue_type
ORDER BY
    revenue_month,
    rr.revenue_type;

-- VIEW 4: Rep Performance
CREATE OR REPLACE VIEW rep_performance AS
SELECT
    d.assigned_rep,
    COUNT(d.deal_id) AS total_deals,
    SUM(CASE WHEN ds.is_won THEN 1 ELSE 0 END) AS deals_won,
    SUM(CASE WHEN ds.is_closed AND NOT ds.is_won THEN 1 ELSE 0 END) AS deals_lost,
    SUM(CASE WHEN d.is_active THEN 1 ELSE 0 END) AS active_deals,
    ROUND(SUM(CASE WHEN ds.is_won THEN d.deal_value ELSE 0 END), 2) AS total_revenue_won,
    ROUND(AVG(CASE WHEN ds.is_won THEN d.deal_value END), 2) AS avg_won_deal_value,
    ROUND(SUM(CASE WHEN d.is_active THEN d.deal_value ELSE 0 END), 2) AS active_pipeline_value,
    ROUND(
        SUM(CASE WHEN ds.is_won THEN 1 ELSE 0 END)::NUMERIC /
        NULLIF(SUM(CASE WHEN ds.is_closed THEN 1 ELSE 0 END), 0) * 100
    , 2) AS win_rate_percent,
    ROUND(AVG(
        CASE WHEN ds.is_won THEN
            d.actual_close_date - d.created_at::DATE
        END
    ), 0) AS avg_days_to_close,
    SUM(CASE WHEN d.is_active AND d.expected_close_date < CURRENT_DATE THEN 1 ELSE 0 END) AS overdue_deals,
    ROUND(AVG(f.forecast_accuracy), 2) AS avg_forecast_accuracy
FROM deals d
JOIN deal_stages ds ON d.stage_id = ds.stage_id
LEFT JOIN forecasts f ON d.assigned_rep = f.assigned_rep
GROUP BY d.assigned_rep
ORDER BY total_revenue_won DESC;

-- VIEW 5: Customer Health
CREATE OR REPLACE VIEW customer_health AS
SELECT
    c.customer_id,
    c.company_name,
    c.customer_tier,
    c.industry,
    c.status,
    COUNT(DISTINCT d.deal_id) AS total_deals,
    SUM(CASE WHEN ds.is_won THEN d.deal_value ELSE 0 END) AS total_revenue,
    ROUND(AVG(CASE WHEN ds.is_won THEN d.deal_value END), 2) AS avg_deal_value,
    COUNT(DISTINCT a.activity_id) AS total_activities,
    MAX(a.activity_date) AS last_activity_date,
    CURRENT_DATE - MAX(a.activity_date::DATE) AS days_since_activity,
    COUNT(DISTINCT cs.signal_id) AS total_churn_signals,
    SUM(CASE WHEN cs.severity = 'Critical' THEN 1 ELSE 0 END) AS critical_signals,
    SUM(CASE WHEN cs.severity = 'High' THEN 1 ELSE 0 END) AS high_signals,
    SUM(CASE WHEN cs.resolved = FALSE THEN 1 ELSE 0 END) AS unresolved_signals,
    ROUND(
        CASE
            WHEN SUM(CASE WHEN cs.severity = 'Critical' AND cs.resolved = FALSE THEN 1 ELSE 0 END) > 0 THEN 20.00
            WHEN SUM(CASE WHEN cs.severity = 'High' AND cs.resolved = FALSE THEN 1 ELSE 0 END) > 0 THEN 45.00
            WHEN SUM(CASE WHEN cs.severity = 'Medium' AND cs.resolved = FALSE THEN 1 ELSE 0 END) > 0 THEN 65.00
            WHEN CURRENT_DATE - MAX(a.activity_date::DATE) > 30 THEN 70.00
            WHEN SUM(CASE WHEN cs.severity = 'Low' AND cs.resolved = FALSE THEN 1 ELSE 0 END) > 0 THEN 80.00
            ELSE 95.00
        END
    , 2) AS health_score,
    CASE
        WHEN SUM(CASE WHEN cs.severity = 'Critical' AND cs.resolved = FALSE THEN 1 ELSE 0 END) > 0 THEN 'Critical'
        WHEN SUM(CASE WHEN cs.severity = 'High' AND cs.resolved = FALSE THEN 1 ELSE 0 END) > 0 THEN 'At Risk'
        WHEN SUM(CASE WHEN cs.severity = 'Medium' AND cs.resolved = FALSE THEN 1 ELSE 0 END) > 0 THEN 'Needs Attention'
        WHEN CURRENT_DATE - MAX(a.activity_date::DATE) > 30 THEN 'Needs Attention'
        ELSE 'Healthy'
    END AS health_status
FROM customers c
LEFT JOIN deals d ON c.customer_id = d.customer_id
LEFT JOIN deal_stages ds ON d.stage_id = ds.stage_id
LEFT JOIN activities a ON c.customer_id = a.customer_id
LEFT JOIN churn_signals cs ON c.customer_id = cs.customer_id
GROUP BY
    c.customer_id, c.company_name, c.customer_tier,
    c.industry, c.status
ORDER BY health_score ASC;
