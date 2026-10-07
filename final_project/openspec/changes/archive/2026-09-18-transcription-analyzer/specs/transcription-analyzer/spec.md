## Purpose

Определяет требования к модулю обработки транскрипции и суммаризации диалогов: генерация синтетического текста транскрипции (фиктивный текст на основе duration звонка, без аудио), broadcast enrichment с профилем клиента, synthetic summary generation через keyword matching (без реальной LLM), dual-write в Kafka и PostgreSQL, управление жизненным циклом метаданных звонка.

## ADDED Requirements

### Requirement: Synthetic Transcription Generation (Producer)

The system SHALL generate synthetic (fake) transcription text for completed calls and publish it to Kafka. No real audio processing or STT — text is purely placeholder.

#### Scenario: Synthetic transcription produced to transcription.raw
- **WHEN** a call event arrives from calls.completed
- **THEN** the system generates a synthetic (fake) transcription text based on call duration and produces it to transcription.raw with callId as key and Avro format

#### Scenario: Transcription text length correlates with duration
- **WHEN** transcription is generated
- **THEN** the generated text length is proportional to the call duration (e.g., ~150 words per minute of call)

#### Scenario: Transcription includes quality and language parameters
- **WHEN** transcription is produced
- **THEN** it includes quality (0.0-1.0) and language (ru/en) parameters in the Avro record

### Requirement: Summary Generator Reads from transcription.raw

The system SHALL consume raw transcriptions from the transcription.raw topic and produce summaries to transcription.summary.

#### Scenario: Summary generator consumes from transcription.raw
- **WHEN** summary generator consumes from transcription.raw topic
- **THEN** it processes each record and produces summary to transcription.summary with callId as key and Avro format

#### Scenario: Default values when no keywords match
- **WHEN** synthetic summary generation processes a transcription with no matching keywords
- **THEN** it produces summary with default values: problem=other, solution=resolved_on_call, sentiment=neutral, urgency=low, confidence=0.0

### Requirement: Synthetic Summary Generation (Keyword-based, No LLM)

The system SHALL generate synthetic (fake) summary from transcriptions using keyword matching. No real LLM API or ML model — purely heuristic-based extraction.

#### Scenario: Summary includes problem field
- **WHEN** synthetic summary generation processes a transcription
- **THEN** it extracts the problem category (card_loss, credit_inquiry, complaint, fraud_suspected, other) via keyword matching

#### Scenario: Summary includes solution field
- **WHEN** synthetic summary generation processes a transcription
- **THEN** it extracts the solution category (card_blocked, info_provided, escalation_created, transfer_to_specialist, resolved_on_call) via keyword matching

#### Scenario: Summary includes sentiment field
- **WHEN** synthetic summary generation processes a transcription
- **THEN** it extracts sentiment (positive, neutral, negative) via keyword matching

#### Scenario: Summary includes urgency field
- **WHEN** synthetic summary generation processes a transcription
- **THEN** it extracts urgency level (low, medium, high, critical) via keyword matching on keywords like fraud, complaint, escalation

#### Scenario: Summary includes confidence score
- **WHEN** synthetic summary generation processes a transcription
- **THEN** it generates a confidence score (0.0-1.0) based on keyword match quality

#### Scenario: Summary produced to transcription.summary
- **WHEN** synthetic summary generation completes
- **THEN** it produces the summary to transcription.summary with callId as key and Avro format

### Requirement: Customer Profile Broadcast Enrichment

The system SHALL enrich transcription summaries with customer profile data via broadcast (not KTable join). The customers.profile table is broadcast to every event to avoid key mismatch between transcription.summary (key=callId) and customers.profile (key=phone).

#### Scenario: Customer profile broadcast loaded on startup
- **WHEN** the service starts
- **THEN** it loads the customers.profile data (from Kafka consumers.profile topic or external source) into an in-memory map keyed by phone number

#### Scenario: Enriched event includes customer data
- **WHEN** enrichment is complete
- **THEN** the enriched event includes: segment (premium, standard, corporate), risk_level (low, medium, high), priority (calculated from segment + risk_level per the priority matrix below)

#### Scenario: Missing customer profile handled gracefully
- **WHEN** a call has no matching customer profile
- **THEN** the system produces the enriched event with default values: segment=standard, risk_level=low, priority=normal

#### Scenario: Priority calculated from segment and risk_level
- **WHEN** enrichment is complete with a matching customer profile
- **THEN** priority is calculated per the following matrix:

| segment   | risk_level → | low     | medium | high     |
|-----------|-------------|---------|--------|----------|
| premium   |             | high    | high   | critical |
| standard  |             | normal  | normal | high     |
| corporate |             | normal  | high   | critical |

### Requirement: Dual-Write Strategy

The system SHALL write enriched transcription data to both Kafka (transcription.enriched) and PostgreSQL (call_transcriptions table). Kafka produce happens before PostgreSQL write.

#### Scenario: Enriched event produced to transcription.enriched
- **WHEN** enrichment is complete
- **THEN** the system produces the enriched event to transcription.enriched with callId as key and Avro format

#### Scenario: Enriched event written to PostgreSQL
- **WHEN** enrichment is complete
- **THEN** the system writes the enriched data to the call_transcriptions table in PostgreSQL via JDBC

#### Scenario: PostgreSQL failure with Kafka success
- **WHEN** PostgreSQL write fails after Kafka produce succeeds
- **THEN** the system retries the JDBC write (3 attempts, exponential backoff), logs the error, and sends the failed event to internal DLQ for manual review. The Kafka event is already consumed by reporting-nps — PostgreSQL will be updated via retry or manual reconciliation.

### Requirement: Metadata Lifecycle Management

The system SHALL manage and update the call metadata lifecycle through status transitions. The initial PENDING status is set by call-processor (see call-processor spec). transcription-analyzer manages TRANSCRIBING → SUMMARIZING → COMPLETED transitions.

#### Scenario: Status transition TRANSCRIBING
- **WHEN** raw transcription is produced to transcription.raw
- **THEN** the system produces a metadata event with status=TRANSCRIBING to calls.metadata

#### Scenario: Status transition SUMMARIZING
- **WHEN** synthetic summary generation begins processing
- **THEN** the system produces a metadata event with status=SUMMARIZING to calls.metadata

#### Scenario: Status transition COMPLETED
- **WHEN** dual-write (Kafka + PostgreSQL) is complete
- **THEN** the system produces a metadata event with status=COMPLETED to calls.metadata

#### Scenario: Metadata uses compaction
- **WHEN** metadata events are produced to calls.metadata
- **THEN** they use callId as key with compaction enabled (latest status wins)

### Requirement: Transcription Text Storage

The system SHALL store the full transcription text in PostgreSQL for archival and search.

#### Scenario: Full transcription stored in PostgreSQL
- **WHEN** dual-write is performed
- **THEN** the call_transcriptions table stores: call_id (FK), transcription_text, sentiment, urgency, problem, solution, confidence

#### Scenario: Foreign key constraint maintained
- **WHEN** a transcription record is inserted
- **THEN** call_id must reference an existing record in call_metadata table

### Requirement: Keyword Extraction

The system SHALL extract keywords from transcriptions to inform problem categorization. Keyword matching is case-insensitive.

#### Scenario: Keywords extracted from transcription text
- **WHEN** synthetic summary generation processes a transcription
- **THEN** it extracts keywords: card, loan, fraud, complaint, transfer, block, limit, payment, balance

#### Scenario: Keywords influence problem category
- **WHEN** keywords are extracted
- **THEN** the presence of "fraud" or "unauthorized" increases fraud_suspected probability
- **THEN** the presence of "block" or "lost" increases card_loss probability

#### Scenario: Confidence score based on keyword frequency
- **WHEN** confidence score is calculated
- **THEN** it is computed as: (matched_keywords / total_keywords_in_dictionary) × 0.5 + (1 if exact category match else 0) × 0.5, clamped to [0.0, 1.0]

### Requirement: Health Check

The system SHALL expose a health check endpoint for monitoring.

#### Scenario: Health check returns 200 when healthy
- **WHEN** the service is running and both Kafka and PostgreSQL are reachable
- **THEN** GET /api/health returns HTTP 200 with status OK

#### Scenario: Health check returns 503 when Kafka is down
- **WHEN** the service is running but Kafka is unreachable
- **THEN** GET /api/health returns HTTP 503 with Kafka connection error details

#### Scenario: Health check returns 503 when PostgreSQL is down
- **WHEN** the service is running but PostgreSQL is unreachable
- **THEN** GET /api/health returns HTTP 503 with PostgreSQL connection error details
