-- =============================================
-- REVENUE INTELLIGENCE SYSTEM
-- File: 02_indexes.sql
-- Description: Performance indexes
-- =============================================

-- Customer indexes
CREATE INDEX idx_customers_status ON customers(status);
CREATE INDEX idx_customers_tier ON customers(customer_tier);
CREATE INDEX idx_customers_industry ON customers(industry);

-- Contacts indexes
CREATE INDEX idx_contacts_customer ON contacts(customer_id);
CREATE INDEX idx_contacts_email ON contacts(email);

-- Deals indexes
CREATE INDEX idx_deals_customer ON deals(customer_id);
CREATE INDEX idx_deals_stage ON deals(stage_id);
CREATE INDEX idx_deals_close_date ON deals(expected_close_date);
CREATE INDEX idx_deals_active ON deals(is_active);
CREATE INDEX idx_deals_source ON deals(deal_source);

-- Activities indexes
CREATE INDEX idx_activities_deal ON activities(deal_id);
CREATE INDEX idx_activities_customer ON activities(customer_id);
CREATE INDEX idx_activities_date ON activities(activity_date);
CREATE INDEX idx_activities_type ON activities(activity_type);

-- Revenue records indexes
CREATE INDEX idx_revenue_customer ON revenue_records(customer_id);
CREATE INDEX idx_revenue_date ON revenue_records(revenue_date);
CREATE INDEX idx_revenue_type ON revenue_records(revenue_type);

-- Churn signals indexes
CREATE INDEX idx_churn_customer ON churn_signals(customer_id);
CREATE INDEX idx_churn_severity ON churn_signals(severity);
CREATE INDEX idx_churn_resolved ON churn_signals(resolved);

-- Audit logs indexes
CREATE INDEX idx_audit_table ON audit_logs(table_name);
CREATE INDEX idx_audit_changed_at ON audit_logs(changed_at);
CREATE INDEX idx_audit_record ON audit_logs(record_id);

-- Forecasts indexes
CREATE INDEX idx_forecasts_period ON forecasts(forecast_month);
CREATE INDEX idx_forecasts_rep ON forecasts(assigned_rep);
