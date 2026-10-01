-- PostgreSQL initialization script for call_platform database
-- Creates all required tables, indexes, and user permissions
-- This script runs ONCE when the database is first created (docker-entrypoint-initdb.d)

-- Enable UUID extension
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- ============================================
-- Call metadata table
-- ============================================
CREATE TABLE IF NOT EXISTS call_metadata (
    call_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    
    -- Basic call info
    customer_phone VARCHAR(20) NOT NULL,
    agent_id VARCHAR(50) NOT NULL,
    call_start_time TIMESTAMP NOT NULL DEFAULT NOW(),
    call_end_time TIMESTAMP,
    call_duration INTEGER,
    call_status VARCHAR(50) NOT NULL DEFAULT 'INITIATED',
    
    -- IVR/Call info
    queue_name VARCHAR(100),
    ivr_selection VARCHAR(255),
    call_purpose VARCHAR(255),
    
    -- Analysis fields (from transcription-analyzer, reporting-nps)
    sentiment_score DECIMAL(3, 2),
    sentiment VARCHAR(20),
    urgency VARCHAR(20),
    problem VARCHAR(50),
    solution VARCHAR(50),
    confidence DOUBLE PRECISION,
    
    -- Additional fields (from reporting-nps)
    segment VARCHAR(50),
    risk_level VARCHAR(20),
    priority VARCHAR(20),
    
    -- Timestamps
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP NOT NULL DEFAULT NOW()
);

-- ============================================
-- Call transcriptions table
-- ============================================
CREATE TABLE IF NOT EXISTS call_transcriptions (
    transcription_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    call_id UUID NOT NULL,
    
    -- Transcription data
    transcription_text TEXT,
    language VARCHAR(10) NOT NULL DEFAULT 'ru-RU',
    confidence_score DECIMAL(3, 2),
    segment_start_time INTERVAL,
    segment_end_time INTERVAL,
    audio_file_path VARCHAR(500),
    
    -- Analysis fields
    problem VARCHAR(50),
    solution VARCHAR(50),
    sentiment VARCHAR(20),
    urgency VARCHAR(20),
    confidence DOUBLE PRECISION,
    
    -- Additional fields
    segment TEXT,
    risk_level TEXT,
    priority TEXT,
    
    -- Processing info
    processing_status VARCHAR(50) NOT NULL DEFAULT 'PENDING',
    error_message TEXT,
    
    -- Timestamps
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP NOT NULL DEFAULT NOW(),
    
    CONSTRAINT fk_call FOREIGN KEY (call_id) REFERENCES call_metadata(call_id) ON DELETE CASCADE
);

-- Unique constraint on call_id for Kafka Connect upsert
ALTER TABLE call_transcriptions ADD CONSTRAINT uq_call_id UNIQUE (call_id);

-- ============================================
-- Fraud statistics table
-- ============================================
CREATE TABLE IF NOT EXISTS fraud_stats (
    id BIGSERIAL PRIMARY KEY,
    phone VARCHAR(20) NOT NULL,
    call_id VARCHAR(100),
    pattern VARCHAR(50) NOT NULL,
    severity VARCHAR(20) NOT NULL,
    count INTEGER NOT NULL DEFAULT 1,
    last_alert_at TIMESTAMP DEFAULT NOW(),
    agent_id VARCHAR(50)
);

-- ============================================
-- Indexes for performance
-- ============================================

-- call_metadata indexes
CREATE INDEX IF NOT EXISTS idx_call_metadata_customer_phone ON call_metadata(customer_phone);
CREATE INDEX IF NOT EXISTS idx_call_metadata_agent_id ON call_metadata(agent_id);
CREATE INDEX IF NOT EXISTS idx_call_metadata_call_start_time ON call_metadata(call_start_time);
CREATE INDEX IF NOT EXISTS idx_call_metadata_call_status ON call_metadata(call_status);
CREATE INDEX IF NOT EXISTS idx_call_metadata_sentiment ON call_metadata(sentiment) WHERE sentiment IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_call_metadata_urgency ON call_metadata(urgency) WHERE urgency IS NOT NULL;

-- call_transcriptions indexes
CREATE INDEX IF NOT EXISTS idx_call_transcriptions_call_id ON call_transcriptions(call_id);
CREATE INDEX IF NOT EXISTS idx_call_transcriptions_processing_status ON call_transcriptions(processing_status);
CREATE INDEX IF NOT EXISTS idx_call_transcriptions_problem ON call_transcriptions(problem) WHERE problem IS NOT NULL;

-- fraud_stats indexes
CREATE INDEX IF NOT EXISTS idx_fraud_stats_phone ON fraud_stats(phone);
CREATE INDEX IF NOT EXISTS idx_fraud_stats_agent_id ON fraud_stats(agent_id);

-- ============================================
-- Full call info view (for reporting-nps)
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

-- ============================================
-- User permissions (kafka-connect-user)
-- ============================================
DO $$
BEGIN
    IF NOT EXISTS (SELECT FROM pg_catalog.pg_roles WHERE rolname = 'kafka-connect-user') THEN
        CREATE USER "kafka-connect-user" WITH PASSWORD 'kafka-connect-secret';
    END IF;
END
$$;

-- Grant SELECT on call_transcriptions only
GRANT SELECT ON call_transcriptions TO "kafka-connect-user";

-- Grant INSERT on call_transcriptions only
GRANT INSERT ON call_transcriptions TO "kafka-connect-user";

-- Grant usage on schema
GRANT USAGE ON SCHEMA public TO "kafka-connect-user";
