-- PostgreSQL initialization script for reporting-nps
-- Creates fraud_stats table and v_full_call_info view

-- ============================================
-- Fraud statistics table
-- ============================================
CREATE TABLE IF NOT EXISTS fraud_stats (
    id BIGSERIAL PRIMARY KEY,
    phone VARCHAR(20) NOT NULL,
    call_id VARCHAR(36),
    pattern VARCHAR(50) NOT NULL,
    severity VARCHAR(20) NOT NULL,
    count INTEGER NOT NULL DEFAULT 1,
    last_alert_at TIMESTAMP NOT NULL DEFAULT NOW(),
    agent_id VARCHAR(50)
);

CREATE INDEX IF NOT EXISTS idx_fraud_stats_phone ON fraud_stats(phone);
CREATE INDEX IF NOT EXISTS idx_fraud_stats_pattern ON fraud_stats(pattern);
CREATE INDEX IF NOT EXISTS idx_fraud_stats_severity ON fraud_stats(severity);
CREATE INDEX IF NOT EXISTS idx_fraud_stats_agent_id ON fraud_stats(agent_id);

-- ============================================
-- Full call info view
-- ============================================
CREATE OR REPLACE VIEW v_full_call_info AS
SELECT
    cm.call_id,
    cm.customer_phone,
    cm.agent_id,
    cm.call_start_time,
    cm.call_end_time,
    cm.call_duration,
    cm.call_status,
    cm.queue_name,
    cm.ivr_selection,
    cm.call_purpose,
    cm.sentiment_score,
    cm.sentiment,
    cm.urgency,
    cm.problem,
    cm.solution,
    cm.confidence,
    cm.segment,
    cm.risk_level,
    cm.priority,
    cm.created_at,
    cm.updated_at,
    ct.transcription_id,
    ct.transcription_text,
    ct.language,
    ct.confidence_score,
    ct.processing_status,
    ct.error_message
FROM call_metadata cm
LEFT JOIN call_transcriptions ct ON cm.call_id = ct.call_id;
