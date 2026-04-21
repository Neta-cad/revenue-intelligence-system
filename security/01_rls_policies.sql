-- =============================================
-- REVENUE INTELLIGENCE SYSTEM
-- File: 01_rls_policies.sql
-- Description: Row Level Security policies
-- =============================================

-- Enable RLS on all key tables
ALTER TABLE customers ENABLE ROW LEVEL SECURITY;
ALTER TABLE contacts ENABLE ROW LEVEL SECURITY;
ALTER TABLE deals ENABLE ROW LEVEL SECURITY;
ALTER TABLE deal_stages ENABLE ROW LEVEL SECURITY;
ALTER TABLE products ENABLE ROW LEVEL SECURITY;
ALTER TABLE deal_products ENABLE ROW LEVEL SECURITY;
ALTER TABLE activities ENABLE ROW LEVEL SECURITY;
ALTER TABLE revenue_records ENABLE ROW LEVEL SECURITY;
ALTER TABLE forecasts ENABLE ROW LEVEL SECURITY;
ALTER TABLE churn_signals ENABLE ROW LEVEL SECURITY;
ALTER TABLE cohorts ENABLE ROW LEVEL SECURITY;
ALTER TABLE audit_logs ENABLE ROW LEVEL SECURITY;

-- Read policies for all tables
CREATE POLICY "Allow read access on customers"
ON customers FOR SELECT USING (true);

CREATE POLICY "Allow read access on contacts"
ON contacts FOR SELECT USING (true);

CREATE POLICY "Allow read access on deals"
ON deals FOR SELECT USING (true);

CREATE POLICY "Allow read access on deal_stages"
ON deal_stages FOR SELECT USING (true);

CREATE POLICY "Allow read access on products"
ON products FOR SELECT USING (true);

CREATE POLICY "Allow read access on deal_products"
ON deal_products FOR SELECT USING (true);

CREATE POLICY "Allow read access on activities"
ON activities FOR SELECT USING (true);

CREATE POLICY "Allow read access on revenue_records"
ON revenue_records FOR SELECT USING (true);

CREATE POLICY "Allow read access on forecasts"
ON forecasts FOR SELECT USING (true);

CREATE POLICY "Allow read access on churn_signals"
ON churn_signals FOR SELECT USING (true);

CREATE POLICY "Allow read access on cohorts"
ON cohorts FOR SELECT USING (true);

CREATE POLICY "Allow read access on audit_logs"
ON audit_logs FOR SELECT USING (true);

-- Create demo judge user
CREATE USER demo_judge WITH PASSWORD 'RevIQ2024Judge!';
GRANT CONNECT ON DATABASE postgres TO demo_judge;
GRANT USAGE ON SCHEMA public TO demo_judge;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO demo_judge;
GRANT SELECT ON ALL SEQUENCES IN SCHEMA public TO demo_judge;
ALTER DEFAULT PRIVILEGES IN SCHEMA public
GRANT SELECT ON TABLES TO demo_judge;
