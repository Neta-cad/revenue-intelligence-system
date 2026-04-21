-- =============================================
-- REVENUE INTELLIGENCE SYSTEM
-- File: 04_queries.sql
-- Description: 12 analytical queries for judges
-- =============================================

-- -----------------------------------------------
-- QUERY 1: PIPELINE SCORING & RISK DETECTION
-- -----------------------------------------------
WITH activity_summary AS (
    SELECT
        deal_id,
        COUNT(*) AS total_activities,
        MAX(activity_date::DATE) AS last_activity_date,
        SUM(CASE WHEN outcome = 'Negative' THEN 1 ELSE 0 END) AS negative_outcomes,
        SUM(CASE WHEN outcome = 'No Response' THEN 1 ELSE 0 END) AS no_responses
    FROM activities
    GROUP BY deal_id
),
churn_summary AS (
    SELECT
        deal_id,
        COUNT(*) AS total_signals,
        SUM(CASE WHEN severity = 'Critical' THEN 1 ELSE 0 END) AS critical_signals,
        SUM(CASE WHEN severity = 'High' THEN 1 ELSE 0 END) AS high_signals
    FROM churn_signals
    WHERE resolved = FALSE
    GROUP BY deal_id
)
SELECT
    d.deal_name,
    c.company_name,
    c.customer_tier,
    ds.stage_name,
    d.deal_value,
    d.expected_close_date,
    d.assigned_rep,
    d.days_in_stage,
    COALESCE(a.total_activities, 0) AS total_activities,
    COALESCE(a.last_activity_date, d.created_at::DATE) AS last_activity_date,
    COALESCE(CURRENT_DATE - a.last_activity_date, d.days_in_stage) AS days_since_activity,
    COALESCE(a.negative_outcomes, 0) AS negative_outcomes,
    COALESCE(a.no_responses, 0) AS no_responses,
    COALESCE(cs.total_signals, 0) AS churn_signals,
    COALESCE(cs.critical_signals, 0) AS critical_signals,
    CASE
        WHEN CURRENT_DATE > d.expected_close_date THEN 'OVERDUE'
        WHEN CURRENT_DATE > d.expected_close_date - 7 THEN 'CLOSING SOON'
        ELSE 'ON TRACK'
    END AS close_date_status,
    ROUND(
        (COALESCE(CURRENT_DATE - a.last_activity_date, 0) * 0.35) +
        (d.days_in_stage * 0.25) +
        (COALESCE(CURRENT_DATE - d.expected_close_date, 0) * 0.20) +
        (COALESCE(a.negative_outcomes, 0) * 8 * 0.10) +
        (COALESCE(a.no_responses, 0) * 6 * 0.10)
    , 2) AS risk_score,
    CASE
        WHEN COALESCE(CURRENT_DATE - a.last_activity_date, 0) > 45
            OR COALESCE(cs.critical_signals, 0) > 0 THEN '🔴 Critical'
        WHEN COALESCE(CURRENT_DATE - a.last_activity_date, 0) > 30
            OR d.days_in_stage > 45
            OR COALESCE(cs.high_signals, 0) > 0 THEN '🟠 High'
        WHEN COALESCE(CURRENT_DATE - a.last_activity_date, 0) > 14
            OR d.days_in_stage > 30 THEN '🟡 Medium'
        ELSE '🟢 Low'
    END AS risk_level,
    ROUND(d.deal_value * ds.probability / 100, 2) AS weighted_value
FROM deals d
JOIN customers c ON d.customer_id = c.customer_id
JOIN deal_stages ds ON d.stage_id = ds.stage_id
LEFT JOIN activity_summary a ON d.deal_id = a.deal_id
LEFT JOIN churn_summary cs ON d.deal_id = cs.deal_id
WHERE d.is_active = TRUE
ORDER BY risk_score DESC;

-- -----------------------------------------------
-- QUERY 2: FUNNEL CONVERSION & VELOCITY
-- -----------------------------------------------
WITH stage_totals AS (
    SELECT
        ds.stage_name,
        ds.stage_order,
        ds.probability,
        COUNT(d.deal_id) AS total_deals,
        SUM(d.deal_value) AS total_value,
        ROUND(AVG(d.days_in_stage), 2) AS avg_days_in_stage
    FROM deal_stages ds
    LEFT JOIN deals d ON ds.stage_id = d.stage_id
    GROUP BY ds.stage_name, ds.stage_order, ds.probability
),
won_lost AS (
    SELECT
        ds.stage_name,
        SUM(CASE WHEN ds.is_won THEN 1 ELSE 0 END) AS won_deals,
        SUM(CASE WHEN ds.is_closed AND NOT ds.is_won THEN 1 ELSE 0 END) AS lost_deals,
        SUM(CASE WHEN ds.is_won THEN d.deal_value ELSE 0 END) AS won_value,
        SUM(CASE WHEN ds.is_closed AND NOT ds.is_won THEN d.deal_value ELSE 0 END) AS lost_value
    FROM deal_stages ds
    LEFT JOIN deals d ON ds.stage_id = d.stage_id
    GROUP BY ds.stage_name
)
SELECT
    st.stage_name,
    st.stage_order,
    st.probability,
    st.total_deals,
    st.total_value,
    st.avg_days_in_stage,
    COALESCE(wl.won_deals, 0) AS won_deals,
    COALESCE(wl.lost_deals, 0) AS lost_deals,
    COALESCE(wl.won_value, 0) AS won_value,
    COALESCE(wl.lost_value, 0) AS lost_value,
    ROUND(
        COALESCE(wl.won_deals, 0)::NUMERIC /
        NULLIF(st.total_deals, 0) * 100
    , 2) AS conversion_rate_percent,
    ROUND(
        COALESCE(wl.lost_deals, 0)::NUMERIC /
        NULLIF(st.total_deals, 0) * 100
    , 2) AS loss_rate_percent,
    ROUND(
        st.total_deals::NUMERIC /
        NULLIF(LAG(st.total_deals) OVER (ORDER BY st.stage_order), 0) * 100
    , 2) AS stage_to_stage_conversion,
    ROUND(
        COALESCE(wl.lost_value, 0) /
        NULLIF(st.total_value, 0) * 100
    , 2) AS revenue_leak_percent,
    CASE
        WHEN st.avg_days_in_stage > 45 THEN 'Slow'
        WHEN st.avg_days_in_stage > 25 THEN 'Moderate'
        ELSE 'Fast'
    END AS velocity_status
FROM stage_totals st
LEFT JOIN won_lost wl ON st.stage_name = wl.stage_name
ORDER BY st.stage_order;

-- -----------------------------------------------
-- QUERY 3: REVENUE FORECASTING
-- -----------------------------------------------
WITH historical_rates AS (
    SELECT
        d.assigned_rep,
        ROUND(
            SUM(CASE WHEN ds.is_won THEN 1 ELSE 0 END)::NUMERIC /
            NULLIF(SUM(CASE WHEN ds.is_closed THEN 1 ELSE 0 END), 0) * 100
        , 2) AS win_rate,
        ROUND(AVG(CASE WHEN ds.is_won THEN d.deal_value END), 2) AS avg_won_value,
        ROUND(AVG(
            CASE WHEN ds.is_won THEN
                d.actual_close_date - d.created_at::DATE
            END
        ), 0) AS avg_days_to_close
    FROM deals d
    JOIN deal_stages ds ON d.stage_id = ds.stage_id
    GROUP BY d.assigned_rep
),
pipeline_forecast AS (
    SELECT
        DATE_TRUNC('month', d.expected_close_date)::DATE AS forecast_month,
        d.assigned_rep,
        COUNT(d.deal_id) AS deals_in_period,
        SUM(d.deal_value) AS total_pipeline,
        ROUND(SUM(d.deal_value * ds.probability / 100), 2) AS weighted_pipeline,
        ROUND(SUM(d.deal_value * 0.90), 2) AS best_case,
        ROUND(SUM(d.deal_value * 0.40), 2) AS worst_case
    FROM deals d
    JOIN deal_stages ds ON d.stage_id = ds.stage_id
    WHERE d.is_active = TRUE
    AND d.expected_close_date >= CURRENT_DATE
    GROUP BY
        DATE_TRUNC('month', d.expected_close_date),
        d.assigned_rep
)
SELECT
    pf.forecast_month,
    pf.assigned_rep,
    pf.deals_in_period,
    pf.total_pipeline,
    pf.weighted_pipeline,
    pf.best_case,
    pf.worst_case,
    hr.win_rate AS historical_win_rate,
    hr.avg_days_to_close,
    ROUND(pf.total_pipeline * hr.win_rate / 100, 2) AS predicted_revenue,
    ROUND(pf.weighted_pipeline - (pf.total_pipeline * hr.win_rate / 100), 2) AS forecast_gap,
    CASE
        WHEN pf.weighted_pipeline > (pf.total_pipeline * hr.win_rate / 100) THEN 'Optimistic'
        WHEN pf.weighted_pipeline < (pf.total_pipeline * hr.win_rate / 100) THEN 'Conservative'
        ELSE 'On Target'
    END AS forecast_bias
FROM pipeline_forecast pf
LEFT JOIN historical_rates hr ON pf.assigned_rep = hr.assigned_rep
ORDER BY pf.forecast_month, pf.assigned_rep;

-- -----------------------------------------------
-- QUERY 4: MRR/ARR TREND ANALYSIS
-- -----------------------------------------------
WITH monthly_metrics AS (
    SELECT
        DATE_TRUNC('month', rr.revenue_date)::DATE AS revenue_month,
        SUM(rr.mrr_contribution) AS total_mrr,
        SUM(rr.arr_contribution) AS total_arr,
        SUM(CASE WHEN rr.revenue_type = 'New Business' THEN rr.mrr_contribution ELSE 0 END) AS new_mrr,
        SUM(CASE WHEN rr.revenue_type = 'Expansion' THEN rr.mrr_contribution ELSE 0 END) AS expansion_mrr,
        SUM(CASE WHEN rr.revenue_type = 'Renewal' THEN rr.mrr_contribution ELSE 0 END) AS renewal_mrr,
        SUM(CASE WHEN rr.revenue_type = 'Churn' THEN rr.mrr_contribution ELSE 0 END) AS churned_mrr,
        COUNT(DISTINCT rr.customer_id) AS paying_customers
    FROM revenue_records rr
    GROUP BY DATE_TRUNC('month', rr.revenue_date)
)
SELECT
    revenue_month,
    ROUND(total_mrr, 2) AS total_mrr,
    ROUND(total_arr, 2) AS total_arr,
    ROUND(new_mrr, 2) AS new_mrr,
    ROUND(expansion_mrr, 2) AS expansion_mrr,
    ROUND(renewal_mrr, 2) AS renewal_mrr,
    ROUND(churned_mrr, 2) AS churned_mrr,
    paying_customers,
    ROUND(total_mrr - LAG(total_mrr) OVER (ORDER BY revenue_month), 2) AS mrr_change,
    ROUND(
        (total_mrr - LAG(total_mrr) OVER (ORDER BY revenue_month)) /
        NULLIF(LAG(total_mrr) OVER (ORDER BY revenue_month), 0) * 100
    , 2) AS mrr_growth_percent,
    ROUND(SUM(total_mrr) OVER (ORDER BY revenue_month), 2) AS cumulative_mrr,
    ROUND(total_mrr / NULLIF(paying_customers, 0), 2) AS arpu,
    CASE
        WHEN total_mrr > LAG(total_mrr) OVER (ORDER BY revenue_month) THEN '📈 Growing'
        WHEN total_mrr < LAG(total_mrr) OVER (ORDER BY revenue_month) THEN '📉 Declining'
        ELSE '➡️ Flat'
    END AS growth_trend
FROM monthly_metrics
ORDER BY revenue_month;

-- -----------------------------------------------
-- QUERY 5: COHORT ANALYSIS
-- -----------------------------------------------
WITH customer_cohorts AS (
    SELECT
        c.customer_id,
        c.company_name,
        c.customer_tier,
        c.industry,
        DATE_TRUNC('month', MIN(d.created_at))::DATE AS cohort_month,
        COUNT(DISTINCT d.deal_id) AS total_deals,
        SUM(CASE WHEN ds.is_won THEN d.deal_value ELSE 0 END) AS total_revenue,
        SUM(CASE WHEN ds.is_won THEN 1 ELSE 0 END) AS won_deals,
        SUM(CASE WHEN ds.is_closed AND NOT ds.is_won THEN 1 ELSE 0 END) AS lost_deals
    FROM customers c
    LEFT JOIN deals d ON c.customer_id = d.customer_id
    LEFT JOIN deal_stages ds ON d.stage_id = ds.stage_id
    GROUP BY c.customer_id, c.company_name, c.customer_tier, c.industry
),
cohort_summary AS (
    SELECT
        cohort_month,
        COUNT(DISTINCT customer_id) AS cohort_size,
        SUM(total_revenue) AS cohort_revenue,
        ROUND(AVG(total_revenue), 2) AS avg_revenue_per_customer,
        SUM(won_deals) AS total_won_deals,
        SUM(lost_deals) AS total_lost_deals,
        ROUND(
            SUM(won_deals)::NUMERIC /
            NULLIF(SUM(won_deals) + SUM(lost_deals), 0) * 100
        , 2) AS cohort_win_rate,
        SUM(CASE WHEN total_revenue = 0 THEN 1 ELSE 0 END) AS zero_revenue_customers
    FROM customer_cohorts
    GROUP BY cohort_month
)
SELECT
    cs.cohort_month,
    cs.cohort_size,
    cs.cohort_revenue,
    cs.avg_revenue_per_customer,
    cs.total_won_deals,
    cs.total_lost_deals,
    cs.cohort_win_rate,
    cs.zero_revenue_customers,
    ROUND(
        cs.cohort_revenue /
        NULLIF(SUM(cs.cohort_revenue) OVER (), 0) * 100
    , 2) AS revenue_contribution_percent,
    CASE
        WHEN cs.cohort_win_rate >= 70 THEN '🌟 Excellent'
        WHEN cs.cohort_win_rate >= 50 THEN '✅ Good'
        WHEN cs.cohort_win_rate >= 30 THEN '⚠️ Average'
        ELSE '🔴 Poor'
    END AS cohort_performance
FROM cohort_summary cs
ORDER BY cs.cohort_month;

-- -----------------------------------------------
-- QUERY 6: CHURN RISK RANKING
-- -----------------------------------------------
WITH activity_signals AS (
    SELECT
        c.customer_id,
        COUNT(DISTINCT a.activity_id) AS total_activities,
        MAX(a.activity_date::DATE) AS last_activity_date,
        CURRENT_DATE - MAX(a.activity_date::DATE) AS days_inactive,
        SUM(CASE WHEN a.outcome = 'Negative' THEN 1 ELSE 0 END) AS negative_activities,
        SUM(CASE WHEN a.outcome = 'No Response' THEN 1 ELSE 0 END) AS no_response_activities
    FROM customers c
    LEFT JOIN activities a ON c.customer_id = a.customer_id
    GROUP BY c.customer_id
),
revenue_signals AS (
    SELECT
        customer_id,
        SUM(CASE WHEN payment_status = 'Overdue' THEN amount ELSE 0 END) AS overdue_amount,
        COUNT(CASE WHEN payment_status = 'Overdue' THEN 1 END) AS overdue_count,
        SUM(CASE WHEN revenue_type = 'Expansion' THEN amount ELSE 0 END) AS expansion_revenue,
        SUM(amount) AS total_revenue
    FROM revenue_records
    GROUP BY customer_id
),
churn_signal_summary AS (
    SELECT
        customer_id,
        COUNT(*) AS total_signals,
        SUM(CASE WHEN severity = 'Critical' AND resolved = FALSE THEN 1 ELSE 0 END) AS critical_unresolved,
        SUM(CASE WHEN severity = 'High' AND resolved = FALSE THEN 1 ELSE 0 END) AS high_unresolved,
        SUM(CASE WHEN signal_type = 'Contract Expiring' THEN 1 ELSE 0 END) AS expiring_contracts,
        SUM(CASE WHEN signal_type = 'Competitor Mention' THEN 1 ELSE 0 END) AS competitor_mentions
    FROM churn_signals
    GROUP BY customer_id
)
SELECT
    c.company_name,
    c.customer_tier,
    c.industry,
    c.status,
    COALESCE(act.days_inactive, 999) AS days_inactive,
    COALESCE(rev.overdue_amount, 0) AS overdue_amount,
    COALESCE(rev.total_revenue, 0) AS total_revenue,
    COALESCE(cs.total_signals, 0) AS total_churn_signals,
    COALESCE(cs.critical_unresolved, 0) AS critical_signals,
    COALESCE(cs.expiring_contracts, 0) AS expiring_contracts,
    COALESCE(cs.competitor_mentions, 0) AS competitor_mentions,
    ROUND(
        (COALESCE(act.days_inactive, 0) * 0.25) +
        (COALESCE(cs.critical_unresolved, 0) * 25) +
        (COALESCE(cs.high_unresolved, 0) * 15) +
        (COALESCE(act.negative_activities, 0) * 8) +
        (COALESCE(rev.overdue_count, 0) * 10) +
        (COALESCE(cs.competitor_mentions, 0) * 18) +
        (COALESCE(cs.expiring_contracts, 0) * 12)
    , 2) AS churn_risk_score,
    CASE
        WHEN COALESCE(cs.critical_unresolved, 0) > 0
            OR c.status = 'Churned' THEN '🔴 Critical'
        WHEN COALESCE(cs.high_unresolved, 0) > 0
            OR COALESCE(rev.overdue_count, 0) > 0 THEN '🟠 High'
        WHEN COALESCE(act.days_inactive, 0) > 30
            OR COALESCE(cs.expiring_contracts, 0) > 0 THEN '🟡 Medium'
        WHEN COALESCE(rev.expansion_revenue, 0) > 0 THEN '🟢 Expanding'
        ELSE '✅ Healthy'
    END AS churn_risk_level
FROM customers c
LEFT JOIN activity_signals act ON c.customer_id = act.customer_id
LEFT JOIN revenue_signals rev ON c.customer_id = rev.customer_id
LEFT JOIN churn_signal_summary cs ON c.customer_id = cs.customer_id
ORDER BY churn_risk_score DESC;

-- -----------------------------------------------
-- QUERY 7: WIN/LOSS ANALYSIS
-- -----------------------------------------------
WITH deal_analysis AS (
    SELECT
        d.deal_id,
        c.customer_tier,
        c.industry,
        d.deal_value,
        d.deal_source,
        d.assigned_rep,
        ds.is_won,
        ds.is_closed,
        d.actual_close_date - d.created_at::DATE AS days_to_close,
        COUNT(DISTINCT a.activity_id) AS total_activities,
        SUM(CASE WHEN a.activity_type = 'Demo' THEN 1 ELSE 0 END) AS demos_given,
        SUM(CASE WHEN a.activity_type = 'Meeting' THEN 1 ELSE 0 END) AS meetings_held,
        SUM(CASE WHEN a.outcome = 'Positive' THEN 1 ELSE 0 END) AS positive_signals,
        SUM(CASE WHEN a.outcome = 'Negative' THEN 1 ELSE 0 END) AS negative_signals
    FROM deals d
    JOIN customers c ON d.customer_id = c.customer_id
    JOIN deal_stages ds ON d.stage_id = ds.stage_id
    LEFT JOIN activities a ON d.deal_id = a.deal_id
    WHERE ds.is_closed = TRUE
    GROUP BY
        d.deal_id, c.customer_tier, c.industry,
        d.deal_value, d.deal_source, d.assigned_rep,
        ds.is_won, ds.is_closed, d.actual_close_date, d.created_at
)
SELECT
    CASE WHEN is_won THEN 'Won' ELSE 'Lost' END AS outcome,
    COUNT(*) AS total_deals,
    ROUND(AVG(deal_value), 2) AS avg_deal_value,
    SUM(deal_value) AS total_value,
    ROUND(AVG(days_to_close), 0) AS avg_days_to_close,
    ROUND(AVG(total_activities), 2) AS avg_activities,
    ROUND(AVG(demos_given), 2) AS avg_demos,
    ROUND(AVG(meetings_held), 2) AS avg_meetings,
    ROUND(AVG(positive_signals), 2) AS avg_positive_signals,
    ROUND(AVG(negative_signals), 2) AS avg_negative_signals,
    customer_tier,
    industry,
    deal_source,
    ROUND(
        SUM(CASE WHEN is_won THEN 1 ELSE 0 END)::NUMERIC /
        NULLIF(COUNT(*), 0) * 100
    , 2) AS win_rate_percent
FROM deal_analysis
GROUP BY
    CASE WHEN is_won THEN 'Won' ELSE 'Lost' END,
    customer_tier, industry, deal_source
ORDER BY outcome, total_value DESC;

-- -----------------------------------------------
-- QUERY 8: REP LEADERBOARD
-- -----------------------------------------------
WITH rep_deals AS (
    SELECT
        d.assigned_rep,
        COUNT(d.deal_id) AS total_deals,
        SUM(CASE WHEN ds.is_won THEN 1 ELSE 0 END) AS won_deals,
        SUM(CASE WHEN ds.is_closed AND NOT ds.is_won THEN 1 ELSE 0 END) AS lost_deals,
        SUM(CASE WHEN d.is_active THEN 1 ELSE 0 END) AS active_deals,
        SUM(CASE WHEN ds.is_won THEN d.deal_value ELSE 0 END) AS total_revenue,
        SUM(CASE WHEN d.is_active THEN d.deal_value ELSE 0 END) AS active_pipeline,
        ROUND(AVG(CASE WHEN ds.is_won THEN d.deal_value END), 2) AS avg_won_value,
        ROUND(AVG(
            CASE WHEN ds.is_won THEN
                d.actual_close_date - d.created_at::DATE
            END
        ), 0) AS avg_days_to_close,
        SUM(CASE WHEN d.is_active AND d.expected_close_date < CURRENT_DATE THEN 1 ELSE 0 END) AS overdue_deals
    FROM deals d
    JOIN deal_stages ds ON d.stage_id = ds.stage_id
    GROUP BY d.assigned_rep
)
SELECT
    rd.assigned_rep,
    rd.total_deals,
    rd.won_deals,
    rd.lost_deals,
    rd.active_deals,
    ROUND(rd.total_revenue, 2) AS total_revenue,
    ROUND(rd.active_pipeline, 2) AS active_pipeline,
    rd.avg_won_value,
    rd.avg_days_to_close,
    rd.overdue_deals,
    ROUND(
        rd.won_deals::NUMERIC /
        NULLIF(rd.won_deals + rd.lost_deals, 0) * 100
    , 2) AS win_rate_percent,
    ROUND(
        rd.total_revenue /
        NULLIF(SUM(rd.total_revenue) OVER (), 0) * 100
    , 2) AS revenue_share_percent,
    RANK() OVER (ORDER BY rd.total_revenue DESC) AS revenue_rank,
    CASE
        WHEN rd.won_deals::NUMERIC / NULLIF(rd.won_deals + rd.lost_deals, 0) >= 0.70 THEN '🌟 Top Performer'
        WHEN rd.won_deals::NUMERIC / NULLIF(rd.won_deals + rd.lost_deals, 0) >= 0.50 THEN '✅ On Track'
        WHEN rd.won_deals::NUMERIC / NULLIF(rd.won_deals + rd.lost_deals, 0) >= 0.30 THEN '⚠️ Needs Coaching'
        ELSE '🔴 Underperforming'
    END AS performance_status
FROM rep_deals rd
ORDER BY revenue_rank;

-- -----------------------------------------------
-- QUERY 9: UPSELL OPPORTUNITIES
-- -----------------------------------------------
WITH customer_products AS (
    SELECT
        c.customer_id,
        c.company_name,
        c.customer_tier,
        c.industry,
        COUNT(DISTINCT dp.product_id) AS products_owned,
        SUM(dp.total_value) AS total_product_value,
        STRING_AGG(DISTINCT p.product_name, ', ') AS owned_products
    FROM customers c
    JOIN deals d ON c.customer_id = d.customer_id
    JOIN deal_stages ds ON d.stage_id = ds.stage_id
    JOIN deal_products dp ON d.deal_id = dp.deal_id
    JOIN products p ON dp.product_id = p.product_id
    WHERE ds.is_won = TRUE
    GROUP BY c.customer_id, c.company_name, c.customer_tier, c.industry
),
missing_products AS (
    SELECT
        cp.customer_id,
        COUNT(DISTINCT p.product_id) AS missing_product_count,
        SUM(p.unit_price) AS potential_expansion_value
    FROM customer_products cp
    CROSS JOIN products p
    WHERE p.product_id NOT IN (
        SELECT dp.product_id
        FROM deals d
        JOIN deal_products dp ON d.deal_id = dp.deal_id
        JOIN deal_stages ds ON d.stage_id = ds.stage_id
        WHERE d.customer_id = cp.customer_id
        AND ds.is_won = TRUE
    )
    AND p.is_active = TRUE
    GROUP BY cp.customer_id
)
SELECT
    cp.company_name,
    cp.customer_tier,
    cp.industry,
    cp.products_owned,
    cp.total_product_value AS current_rev
