-- PostgreSQL schema for transcription-analyzer

-- Call metadata table (shared with call-processor)
CREATE TABLE IF NOT EXISTS call_metadata (
    call_id VARCHAR(255) PRIMARY KEY,
    status VARCHAR(50) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Call transcriptions table
CREATE TABLE IF NOT EXISTS call_transcriptions (
    call_id VARCHAR(255) PRIMARY KEY REFERENCES call_metadata(call_id),
    transcription_text TEXT NOT NULL,
    sentiment VARCHAR(20) NOT NULL,
    urgency VARCHAR(20) NOT NULL,
    problem VARCHAR(50) NOT NULL,
    solution VARCHAR(50) NOT NULL,
    confidence FLOAT NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
