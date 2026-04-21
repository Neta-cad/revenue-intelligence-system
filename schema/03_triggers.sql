-- =============================================
-- REVENUE INTELLIGENCE SYSTEM
-- File: 03_triggers.sql
-- Description: Audit and churn detection triggers
-- =============================================

-- Audit log function
CREATE OR REPLACE FUNCTION log_audit_changes()
RETURNS TRIGGER AS $$
DECLARE
    v_record_id UUID;
    v_json JSONB;
BEGIN
    IF TG_OP = 'DELETE' THEN
        v_json := to_jsonb(OLD);
    ELSE
        v_json := to_jsonb(NEW);
    END IF;

    v_record_id := COALESCE(
        (v_json ->> (TG_TABLE_NAME || '_id'))::UUID,
        (v_json ->> REGEXP_REPLACE(TG_TABLE_NAME, 's$', '') || '_id')::UUID,
        (v_json ->> 'id')::UUID
    );

    IF TG_OP = 'INSERT' THEN
        INSERT INTO audit_logs (table_name, record_id, action, new_data, changed_by)
        VALUES (TG_TABLE_NAME, v_record_id, TG_OP, v_json, current_user);
        RETURN NEW;
    ELSIF TG_OP = 'UPDATE' THEN
        INSERT INTO audit_logs (table_name, record_id, action, old_data, new_data, changed_by)
        VALUES (TG_TABLE_NAME, v_record_id, TG_OP, to_jsonb(OLD), v_json, current_user);
        RETURN NEW;
    ELSIF TG_OP = 'DELETE' THEN
        INSERT INTO audit_logs (table_name, record_id, action, old_data, changed_by)
        VALUES (TG_TABLE_NAME, v_record_id, TG_OP, v_json, current_user);
        RETURN OLD;
    END IF;
END;
$$ LANGUAGE plpgsql;

-- Attach audit triggers to key tables
CREATE TRIGGER audit_customers
AFTER INSERT OR UPDATE OR DELETE ON customers
FOR EACH ROW EXECUTE FUNCTION log_audit_changes();

CREATE TRIGGER audit_deals
AFTER INSERT OR UPDATE OR DELETE ON deals
FOR EACH ROW EXECUTE FUNCTION log_audit_changes();

CREATE TRIGGER audit_revenue_records
AFTER INSERT OR UPDATE OR DELETE ON revenue_records
FOR EACH ROW EXECUTE FUNCTION log_audit_changes();

CREATE TRIGGER audit_churn_signals
AFTER INSERT OR UPDATE OR DELETE ON churn_signals
FOR EACH ROW EXECUTE FUNCTION log_audit_changes();

-- Churn detection function
CREATE OR REPLACE FUNCTION detect_churn_signals()
RETURNS TRIGGER AS $$
DECLARE
    v_days_since_activity INTEGER;
    v_existing_signal INTEGER;
BEGIN
    SELECT COALESCE(CURRENT_DATE - MAX(activity_date::DATE), 999)
    INTO v_days_since_activity
    FROM activities
    WHERE deal_id = NEW.deal_id;

    SELECT COUNT(*)
    INTO v_existing_signal
    FROM churn_signals
    WHERE deal_id = NEW.deal_id
    AND resolved = FALSE
    AND signal_type = 'No Activity';

    IF v_days_since_activity > 30 AND v_existing_signal = 0 THEN
        INSERT INTO churn_signals (
            customer_id,
            deal_id,
            signal_type,
            severity,
            detected_at,
            resolved
        )
        SELECT
            d.customer_id,
            NEW.deal_id,
            'No Activity',
            CASE
                WHEN v_days_since_activity > 60 THEN 'Critical'
                WHEN v_days_since_activity > 45 THEN 'High'
                WHEN v_days_since_activity > 30 THEN 'Medium'
                ELSE 'Low'
            END,
            NOW(),
            FALSE
        FROM deals d
        WHERE d.deal_id = NEW.deal_id;
    END IF;

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Attach churn detection trigger
CREATE TRIGGER trg_detect_churn
AFTER INSERT OR UPDATE ON activities
FOR EACH ROW EXECUTE FUNCTION detect_churn_signals();
