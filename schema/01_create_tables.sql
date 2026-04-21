-- =============================================
-- REVENUE INTELLIGENCE SYSTEM
-- File: 01_create_tables.sql
-- Description: Core table definitions
-- =============================================

CREATE TABLE deal_stages (
    stage_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    stage_name VARCHAR(100) NOT NULL UNIQUE,
    stage_order INTEGER NOT NULL UNIQUE,
    probability NUMERIC(5,2) CHECK (probability BETWEEN 0 AND 100),
    is_closed BOOLEAN DEFAULT FALSE,
    is_won BOOLEAN DEFAULT FALSE,
    created_at TIMESTAMP DEFAULT NOW()
);

CREATE TABLE customers (
    customer_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    company_name VARCHAR(150) NOT NULL,
    industry VARCHAR(100),
    country VARCHAR(100),
    city VARCHAR(100),
    annual_revenue NUMERIC(15,2),
    employee_count INTEGER,
    customer_tier VARCHAR(20) CHECK (customer_tier IN ('Enterprise', 'Mid-Market', 'SMB')),
    status VARCHAR(20) DEFAULT 'Active' CHECK (status IN ('Active', 'Churned', 'At-Risk', 'Prospect')),
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW()
);

CREATE TABLE contacts (
    contact_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    customer_id UUID NOT NULL REFERENCES customers(customer_id) ON DELETE CASCADE,
    first_name VARCHAR(100) NOT NULL,
    last_name VARCHAR(100) NOT NULL,
    email VARCHAR(150) UNIQUE NOT NULL,
    phone VARCHAR(20),
    job_title VARCHAR(100),
    decision_maker BOOLEAN DEFAULT FALSE,
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW()
);

CREATE TABLE deals (
    deal_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    customer_id UUID NOT NULL REFERENCES customers(customer_id) ON DELETE RESTRICT,
    contact_id UUID REFERENCES contacts(contact_id) ON DELETE SET NULL,
    stage_id UUID NOT NULL REFERENCES deal_stages(stage_id) ON DELETE RESTRICT,
    deal_name VARCHAR(200) NOT NULL,
    deal_value NUMERIC(15,2) NOT NULL CHECK (deal_value > 0),
    currency VARCHAR(10) DEFAULT 'USD',
    expected_close_date DATE NOT NULL,
    actual_close_date DATE,
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW(),
    deal_source VARCHAR(50) CHECK (deal_source IN ('Inbound', 'Outbound', 'Referral', 'Partner', 'Renewal')),
    assigned_rep VARCHAR(150),
    days_in_stage INTEGER DEFAULT 0,
    is_active BOOLEAN DEFAULT TRUE
);

CREATE TABLE products (
    product_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    product_name VARCHAR(150) NOT NULL UNIQUE,
    product_category VARCHAR(100),
    unit_price NUMERIC(15,2) NOT NULL CHECK (unit_price > 0),
    billing_type VARCHAR(20) CHECK (billing_type IN ('One-Time', 'Monthly', 'Annual')),
    is_active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW()
);

CREATE TABLE deal_products (
    deal_product_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    deal_id UUID NOT NULL REFERENCES deals(deal_id) ON DELETE CASCADE,
    product_id UUID NOT NULL REFERENCES products(product_id) ON DELETE RESTRICT,
    quantity INTEGER NOT NULL DEFAULT 1 CHECK (quantity > 0),
    unit_price NUMERIC(15,2) NOT NULL CHECK (unit_price > 0),
    discount_percent NUMERIC(5,2) DEFAULT 0 CHECK (discount_percent BETWEEN 0 AND 100),
    total_value NUMERIC(15,2) GENERATED ALWAYS AS
        (quantity * unit_price * (1 - discount_percent / 100)) STORED,
    created_at TIMESTAMP DEFAULT NOW()
);

CREATE TABLE activities (
    activity_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    deal_id UUID NOT NULL REFERENCES deals(deal_id) ON DELETE CASCADE,
    contact_id UUID REFERENCES contacts(contact_id) ON DELETE SET NULL,
    customer_id UUID NOT NULL REFERENCES customers(customer_id) ON DELETE CASCADE,
    activity_type VARCHAR(50) CHECK (activity_type IN ('Call', 'Email', 'Meeting', 'Demo', 'Follow-Up', 'Proposal Sent', 'Contract Sent')),
    activity_date TIMESTAMP NOT NULL,
    outcome VARCHAR(50) CHECK (outcome IN ('Positive', 'Neutral', 'Negative', 'No Response')),
    notes TEXT,
    created_at TIMESTAMP DEFAULT NOW()
);

CREATE TABLE revenue_records (
    revenue_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    deal_id UUID NOT NULL REFERENCES deals(deal_id) ON DELETE RESTRICT,
    customer_id UUID NOT NULL REFERENCES customers(customer_id) ON DELETE RESTRICT,
    revenue_type VARCHAR(50) CHECK (revenue_type IN ('New Business', 'Renewal', 'Expansion', 'Contraction', 'Churn')),
    amount NUMERIC(15,2) NOT NULL CHECK (amount > 0),
    currency VARCHAR(10) DEFAULT 'USD',
    revenue_date DATE NOT NULL,
    billing_period VARCHAR(20) CHECK (billing_period IN ('Monthly', 'Quarterly', 'Annual', 'One-Time')),
    payment_status VARCHAR(20) DEFAULT 'Paid' CHECK (payment_status IN ('Paid', 'Pending', 'Overdue', 'Failed')),
    mrr_contribution NUMERIC(15,2),
    arr_contribution NUMERIC(15,2),
    created_at TIMESTAMP DEFAULT NOW()
);

CREATE TABLE forecasts (
    forecast_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    forecast_period VARCHAR(20) NOT NULL,
    forecast_month DATE NOT NULL,
    assigned_rep VARCHAR(150),
    pipeline_value NUMERIC(15,2),
    weighted_pipeline NUMERIC(15,2),
    best_case NUMERIC(15,2),
    worst_case NUMERIC(15,2),
    committed_value NUMERIC(15,2),
    actual_revenue NUMERIC(15,2),
    forecast_accuracy NUMERIC(5,2),
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW()
);

CREATE TABLE churn_signals (
    signal_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    customer_id UUID NOT NULL REFERENCES customers(customer_id) ON DELETE CASCADE,
    deal_id UUID REFERENCES deals(deal_id) ON DELETE SET NULL,
    signal_type VARCHAR(100) CHECK (signal_type IN (
        'No Activity',
        'Negative Sentiment',
        'Payment Failure',
        'Contract Expiring',
        'Downgrade Request',
        'Low Engagement',
        'Competitor Mention'
    )),
    severity VARCHAR(20) CHECK (severity IN ('Low', 'Medium', 'High', 'Critical')),
    detected_at TIMESTAMP DEFAULT NOW(),
    resolved BOOLEAN DEFAULT FALSE,
    resolved_at TIMESTAMP,
    resolution_notes TEXT,
    created_at TIMESTAMP DEFAULT NOW()
);

CREATE TABLE cohorts (
    cohort_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    cohort_name VARCHAR(150) NOT NULL UNIQUE,
    cohort_type VARCHAR(50) CHECK (cohort_type IN (
        'Acquisition Month',
        'Industry',
        'Customer Tier',
        'Deal Source',
        'Revenue Band',
        'Geography'
    )),
    cohort_period DATE,
    total_customers INTEGER DEFAULT 0,
    total_revenue NUMERIC(15,2) DEFAULT 0,
    avg_deal_value NUMERIC(15,2) DEFAULT 0,
    churn_rate NUMERIC(5,2) DEFAULT 0,
    expansion_rate NUMERIC(5,2) DEFAULT 0,
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW()
);

CREATE TABLE audit_logs (
    log_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    table_name VARCHAR(100) NOT NULL,
    record_id UUID,
    action VARCHAR(20) CHECK (action IN ('INSERT', 'UPDATE', 'DELETE')),
    old_data JSONB,
    new_data JSONB,
    changed_by VARCHAR(150),
    changed_at TIMESTAMP DEFAULT NOW(),
    ip_address VARCHAR(50),
    session_id VARCHAR(150)
);
