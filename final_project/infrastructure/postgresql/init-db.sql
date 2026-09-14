-- PostgreSQL initialization script for call_platform database
-- Creates tables, indexes, and user permissions

-- Enable UUID extension
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- Call metadata table
CREATE TABLE IF NOT EXISTS call_metadata (
    call_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    customer_phone VARCHAR(20) NOT NULL,
    agent_id VARCHAR(50) NOT NULL,
    call_start_time TIMESTAMP NOT NULL DEFAULT NOW(),
    call_end_time TIMESTAMP,
    call_duration INTEGER,
    call_status VARCHAR(50) NOT NULL DEFAULT 'INITIATED',
    queue_name VARCHAR(100),
    ivr_selection VARCHAR(255),
    call_purpose VARCHAR(255),
    sentiment_score DECIMAL(3, 2),
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP NOT NULL DEFAULT NOW()
);

-- Call transcriptions table
CREATE TABLE IF NOT EXISTS call_transcriptions (
    transcription_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    call_id UUID NOT NULL,
    transcription_text TEXT,
    language VARCHAR(10) NOT NULL DEFAULT 'ru-RU',
    confidence_score DECIMAL(3, 2),
    segment_start_time INTERVAL,
    segment_end_time INTERVAL,
    audio_file_path VARCHAR(500),
    processing_status VARCHAR(50) NOT NULL DEFAULT 'PENDING',
    error_message TEXT,
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP NOT NULL DEFAULT NOW(),
    FOREIGN KEY (call_id) REFERENCES call_metadata(call_id) ON DELETE CASCADE
);

-- Indexes for performance
CREATE INDEX IF NOT EXISTS idx_call_metadata_customer_phone ON call_metadata(customer_phone);
CREATE INDEX IF NOT EXISTS idx_call_metadata_agent_id ON call_metadata(agent_id);
CREATE INDEX IF NOT EXISTS idx_call_metadata_call_start_time ON call_metadata(call_start_time);
CREATE INDEX IF NOT EXISTS idx_call_metadata_call_status ON call_metadata(call_status);
CREATE INDEX IF NOT EXISTS idx_call_transcriptions_call_id ON call_transcriptions(call_id);
CREATE INDEX IF NOT EXISTS idx_call_transcriptions_processing_status ON call_transcriptions(processing_status);

-- Grant permissions to kafka-connect-user (limited access)
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

-- Revoke all access to call_metadata from kafka-connect-user
REVOKE ALL ON call_metadata FROM "kafka-connect-user";

-- Grant usage on schema
GRANT USAGE ON SCHEMA public TO "kafka-connect-user";
