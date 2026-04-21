-- =============================================
-- REVENUE INTELLIGENCE SYSTEM
-- File: 03_functions.sql
-- Description: Custom PostgreSQL functions
-- =============================================

-- FUNCTION 1: Get Deal Health Score
CREATE OR REPLACE FUNCTION get_deal_health_score(p_deal_id UUID)
RETURNS TABLE (
    deal_name VARCHAR,
    company_name VARCHAR,
    deal_value NUMERIC,
    stage_name VARCHAR,
    days_in_stage INTEGER,
    days_since_activity INTEGER,
    activity_count BIGINT,
    negative_signals BIGINT,
    health_score NUMERIC,
    health_status VARCHAR
) AS $$
BEGIN
    RETURN QUERY
    SELECT
        d.deal_name,
        c.company_name,
        d.deal_value,
        ds.stage_name,
        d.days_in_stage,
        (CURRENT_DATE - MAX(a.activity_date::DATE))::INTEGER AS days_since_activity,
        COUNT(a.activity_id) AS activity_count,
        SUM(CASE WHEN a.outcome = 'Negative' THEN 1 ELSE 0 END) AS negative_signals,
        ROUND(
            100 -
            (COALESCE(CURRENT_DATE - MAX(a.activity_date::DATE), 0) * 0.5) -
            (d.days_in_stage * 0.3) -
            (COALESCE(SUM(CASE WHEN a.outcome = 'Negative' THEN 1 ELSE 0 END), 0) * 5)
        , 2) AS health_score,
        CASE
            WHEN 100 - (COALESCE(CURRENT_DATE - MAX(a.activity_date::DATE), 0) * 0.5) - (d.days_in_stage * 0.3) < 20 THEN 'Critical'
            WHEN 100 - (COALESCE(CURRENT_DATE - MAX(a.activity_date::DATE), 0) * 0.5) - (d.days_in_stage * 0.3) < 45 THEN 'At Risk'
            WHEN 100 - (COALESCE(CURRENT_DATE - MAX(a.activity_date::DATE), 0) * 0.5) - (d.days_in_stage * 0.3) < 65 THEN 'Needs Attention'
            ELSE 'Healthy'
        END::VARCHAR AS health_status
    FROM deals d
    JOIN customers c ON d.customer_id = c.customer_id
    JOIN deal_stages ds ON d.stage_id = ds.stage_id
    LEFT JOIN activities a ON d.deal_id = a.deal_id
    WHERE d.deal_id = p_deal_id
    GROUP BY d.deal_name, c.company_name, d.deal_value,
             ds.stage_name, d.days_in_stage;
END;
$$ LANGUAGE plpgsql;

-- FUNCTION 2: Get Revenue Forecast
CREATE OR REPLACE FUNCTION get_revenue_forecast(p_months INTEGER DEFAULT 3)
RETURNS TABLE (
    forecast_month DATE,
    active_deals INTEGER,
    total_pipeline NUMERIC,
    weighted_forecast NUMERIC,
    best_case NUMERIC,
    worst_case NUMERIC,
    historical_win_rate NUMERIC,
    predicted_revenue NUMERIC
) AS $$
DECLARE
    v_win_rate NUMERIC;
BEGIN
    SELECT ROUND(
        SUM(CASE WHEN ds.is_won THEN 1 ELSE 0 END)::NUMERIC /
        NULLIF(SUM(CASE WHEN ds.is_closed THEN 1 ELSE 0 END), 0) * 100
    , 2) INTO v_win_rate
    FROM deals d
    JOIN deal_stages ds ON d.stage_id = ds.stage_id;

    RETURN QUERY
    SELECT
        DATE_TRUNC('month', d.expected_close_date)::DATE AS forecast_month,
        COUNT(d.deal_id)::INTEGER AS active_deals,
        ROUND(SUM(d.deal_value), 2) AS total_pipeline,
        ROUND(SUM(d.deal_value * ds.probability / 100), 2) AS weighted_forecast,
        ROUND(SUM(d.deal_value * 0.90), 2) AS best_case,
        ROUND(SUM(d.deal_value * 0.40), 2) AS worst_case,
        v_win_rate AS historical_win_rate,
        ROUND(SUM(d.deal_value * v_win_rate / 100), 2) AS predicted_revenue
    FROM deals d
    JOIN deal_stages ds ON d.stage_id = ds.stage_id
    WHERE d.is_active = TRUE
    AND d.expected_close_date BETWEEN CURRENT_DATE
        AND CURRENT_DATE + (p_months || ' months')::INTERVAL
    GROUP BY DATE_TRUNC('month', d.expected_close_date)
    ORDER BY forecast_month;
END;
$$ LANGUAGE plpgsql;

-- FUNCTION 3: Get Customer Lifetime Value
CREATE OR REPLACE FUNCTION get_customer_lifetime_value(p_customer_id UUID)
RETURNS TABLE (
    company_name VARCHAR,
    customer_tier VARCHAR,
    industry VARCHAR,
    total_revenue NUMERIC,
    total_deals INTEGER,
    won_deals INTEGER,
    avg_deal_value NUMERIC,
    total_activities BIGINT,
    first_deal_date DATE,
    latest_deal_date DATE,
    customer_age_days INTEGER,
    avg_monthly_revenue NUMERIC,
    projected_annual_value NUMERIC,
    expansion_revenue NUMERIC,
    churn_risk_score NUMERIC,
    clv_estimate NUMERIC
) AS $$
BEGIN
    RETURN QUERY
    SELECT
        c.company_name,
        c.customer_tier,
        c.industry,
        ROUND(SUM(CASE WHEN ds.is_won THEN d.deal_value ELSE 0 END), 2) AS total_revenue,
        COUNT(DISTINCT d.deal_id)::INTEGER AS total_deals,
        SUM(CASE WHEN ds.is_won THEN 1 ELSE 0 END)::INTEGER AS won_deals,
        ROUND(AVG(CASE WHEN ds.is_won THEN d.deal_value END), 2) AS avg_deal_value,
        COUNT(DISTINCT a.activity_id) AS total_activities,
        MIN(d.created_at::DATE) AS first_deal_date,
        MAX(d.created_at::DATE) AS latest_deal_date,
        (CURRENT_DATE - MIN(d.created_at::DATE))::INTEGER AS customer_age_days,
        ROUND(
            SUM(CASE WHEN ds.is_won THEN d.deal_value ELSE 0 END) /
            NULLIF((CURRENT_DATE - MIN(d.created_at::DATE)) / 30, 0)
        , 2) AS avg_monthly_revenue,
        ROUND(
            (SUM(CASE WHEN ds.is_won THEN d.deal_value ELSE 0 END) /
            NULLIF((CURRENT_DATE - MIN(d.created_at::DATE)) / 30, 0)) * 12
        , 2) AS projected_annual_value,
        ROUND(SUM(CASE WHEN rr.revenue_type = 'Expansion' THEN rr.amount ELSE 0 END), 2) AS expansion_revenue,
        ROUND(
            COUNT(DISTINCT cs.signal_id)::NUMERIC * 10 +
            COALESCE(CURRENT_DATE - MAX(a.activity_date::DATE), 0) * 0.5
        , 2) AS churn_risk_score,
        ROUND(
            (SUM(CASE WHEN ds.is_won THEN d.deal_value ELSE 0 END) /
            NULLIF((CURRENT_DATE - MIN(d.created_at::DATE)) / 30, 0)) * 24
        , 2) AS clv_estimate
    FROM customers c
    LEFT JOIN deals d ON c.customer_id = d.customer_id
    LEFT JOIN deal_stages ds ON d.stage_id = ds.stage_id
    LEFT JOIN activities a ON c.customer_id = a.customer_id
    LEFT JOIN revenue_records rr ON c.customer_id = rr.customer_id
    LEFT JOIN churn_signals cs ON c.customer_id = cs.customer_id
        AND cs.resolved = FALSE
    WHERE c.customer_id = p_customer_id
    GROUP BY c.company_name, c.customer_tier, c.industry;
END;
$$ LANGUAGE plpgsql;
