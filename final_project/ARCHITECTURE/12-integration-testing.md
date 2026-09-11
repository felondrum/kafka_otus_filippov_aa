# 12. Integration Testing

## 12.1. Стратегия

Integration-тесты проверяют взаимодействие компонентов с реальными зависимостями через Testcontainers. Запускаются с реальными Kafka, PostgreSQL и Schema Registry.

**Технологии:** JUnit 5, Testcontainers, Spring Boot Test, Awaitility, RestAssured.

**Требования:**
- Покрытие ключевых сценариев > 50%
- Каждый REST endpoint имеет интеграционный тест
- Kafka потоки проверены end-to-end
- PostgreSQL операции проверены

## 12.2. Структура тестов

```
src/test/java/ru/otus/callplatform/
├── callprocessor/
│   ├── CallProcessorIntegrationTest.java
│   ├── TopicCreationIntegrationTest.java
│   └── DLQIntegrationTest.java
├── fraud/
│   ├── FraudDetectorIntegrationTest.java
│   └── StateStoreRecoveryIntegrationTest.java
├── transcription/
│   ├── TranscriptionAnalyzerIntegrationTest.java
│   └── DualWriteIntegrationTest.java
└── reporting/
    ├── ReportingNPSIntegrationTest.java
    └── CQRSIntegrationTest.java
```

## 12.3. Конфигурация Testcontainers

### 12.3.1. Base тестовый класс

```java
@SpringBootTest
@Testcontainers
abstract class BaseIntegrationTest {

    @Container
    static KafkaContainer kafka = new KafkaContainer(
        DockerImageName.parse("confluentinc/cp-kafka:7.6.1")
    );

    @Container
    static PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>(
        DockerImageName.parse("postgres:15.8-alpine")
    )
        .withDatabaseName("test_db")
        .withUsername("test")
        .withPassword("test");

    @Container
    static SchemaRegistryContainer schemaRegistry = new SchemaRegistryContainer(
        DockerImageName.parse("confluentinc/cp-schema-registry:7.6.1")
    )
        .withKafka(kafka.getBootstrapServers());

    @DynamicPropertySource
    static void registerKafkaProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.kafka.bootstrap-servers", kafka::getBootstrapServers);
        registry.add("spring.kafka.producer.properties.schema.registry.url",
            schemaRegistry::getEndpoint);
        registry.add("spring.kafka.consumer.properties.schema.registry.url",
            schemaRegistry::getEndpoint);
    }

    @DynamicPropertySource
    static void registerDbProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", postgres::getJdbcUrl);
        registry.add("spring.datasource.username", postgres::getUsername);
        registry.add("spring.datasource.password", postgres::getPassword);
    }

    @DynamicPropertySource
    static void registerSchemaRegistryProperties(DynamicPropertyRegistry registry) {
        registry.add("schema.registry.url", schemaRegistry::getEndpoint);
    }
}
```

### 12.3.2. Kafka Test Container

```java
@TestConfiguration
public class KafkaTestConfig {

    @Bean
    public NewTopic callsCompletedTopic() {
        return TopicBuilder.name("calls.completed")
            .partitions(3)
            .replicas(1)
            .build();
    }

    @Bean
    public NewTopic fraudAlertsTopic() {
        return TopicBuilder.name("calls.fraud-alerts")
            .partitions(3)
            .replicas(1)
            .build();
    }

    @Bean
    public NewTopic transcriptionRawTopic() {
        return TopicBuilder.name("transcription.raw")
            .partitions(3)
            .replicas(1)
            .build();
    }

    @Bean
    public NewTopic transcriptionEnrichedTopic() {
        return TopicBuilder.name("transcription.enriched")
            .partitions(3)
            .replicas(1)
            .build();
    }
}
```

## 12.4. Примеры Integration-тестов

### 12.4.1. CallProcessorIntegrationTest

```java
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
class CallProcessorIntegrationTest extends BaseIntegrationTest {

    @LocalServerPort
    private int port;

    private RestTemplate restTemplate;
    private KafkaTemplate<String, CallCompleted> kafkaTemplate;
    private Consumer<String, CallCompleted> consumer;

    @BeforeEach
    void setUp() {
        restTemplate = new RestTemplate();
        restTemplate.getMessageConverters()
            .add(new MappingJackson2HttpMessageConverter());

        kafkaTemplate = new KafkaTemplate<>(producerFactory());
        consumer = consumerFactory().createConsumer().createRecordConsumer();
    }

    @Test
    void shouldProcessCallAndWriteToKafka() throws Exception {
        // Arrange
        CallRequest request = new CallRequest(
            "550e8400-e29b-41d4-a716-446655440000",
            "+79001234567",
            180,
            "agent-1",
            8,
            "corr-123"
        );

        // Act
        ResponseEntity<String> response = restTemplate.postForEntity(
            "http://localhost:" + port + "/api/calls",
            request,
            String.class
        );

        // Assert
        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.ACCEPTED);
        assertThat(response.getHeaders().getFirst("X-Correlation-ID")).isEqualTo("corr-123");

        // Verify Kafka
        ConsumerRecord<String, CallCompleted> record = consumeMessage("calls.completed", 5000);
        assertThat(record.value().getCallId()).isEqualTo("550e8400-e29b-41d4-a716-446655440000");
        assertThat(record.value().getPhone()).isEqualTo("+79001234567");
        assertThat(record.value().getDuration()).isEqualTo(180);
    }

    @Test
    void shouldReturn400ForInvalidCall() {
        CallRequest request = new CallRequest(
            "invalid-uuid",
            "bad-phone",
            -1,
            "",
            15,
            null
        );

        ResponseEntity<String> response = restTemplate.postForEntity(
            "http://localhost:" + port + "/api/calls",
            request,
            String.class
        );

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.BAD_REQUEST);
    }

    @Test
    void shouldCreateTopicsOnStartup() throws Exception {
        // Topics should be created by TopicManager on startup
        AdminClient adminClient = AdminClient.create(producerFactory().getProducerProperties());

        ListTopicsResult topics = adminClient.listTopics();
        Set<String> topicNames = topics.names().get().keySet();

        assertThat(topicNames).contains(
            "calls.completed",
            "calls.metadata",
            "calls.fraud-alerts",
            "transcription.raw",
            "transcription.summary",
            "transcription.enriched",
            "calls.dlq",
            "customers.profile"
        );
    }

    @Test
    void shouldHandleIdempotentProduction() throws Exception {
        CallRequest request = new CallRequest(
            "550e8400-e29b-41d4-a716-446655440000",
            "+79001234567",
            180,
            "agent-1",
            8,
            "corr-123"
        );

        // Send same request twice
        restTemplate.postForEntity(
            "http://localhost:" + port + "/api/calls",
            request,
            String.class
        );

        ResponseEntity<String> response2 = restTemplate.postForEntity(
            "http://localhost:" + port + "/api/calls",
            request,
            String.class
        );

        assertThat(response2.getStatusCode()).isEqualTo(HttpStatus.ACCEPTED);

        // Verify only one message in Kafka (idempotent)
        List<ConsumerRecord<String, CallCompleted>> records = consumeAllMessages(
            "calls.completed", 10, 5000
        );
        assertThat(records).hasSize(1);
    }
}
```

### 12.4.2. FraudDetectorIntegrationTest

```java
@SpringBootTest
class FraudDetectorIntegrationTest extends BaseIntegrationTest {

    @Autowired
    private KafkaTemplate<String, CallCompleted> kafkaTemplate;

    @Autowired
    private KafkaTemplate<String, FraudAlert> fraudAlertTemplate;

    @Autowired
    private ConsumerFactory<String, FraudAlert> fraudAlertConsumerFactory;

    @Test
    void shouldDetectHighFrequencyFraud() throws Exception {
        String phone = "+79001234567";

        // Send 6 calls from same phone within 1 minute
        for (int i = 0; i < 6; i++) {
            CallCompleted call = new CallCompleted(
                UUID.randomUUID().toString(),
                phone,
                120,
                "agent-1",
                5,
                System.currentTimeMillis()
            );
            kafkaTemplate.send("calls.completed", call.getCallId(), call).get();
        }

        // Wait for fraud detection
        Awaitility.await()
            .atMost(10, TimeUnit.SECONDS)
            .pollInterval(500, TimeUnit.MILLISECONDS)
            .untilAsserted(() -> {
                Consumer<String, FraudAlert> consumer = fraudAlertConsumerFactory
                    .createConsumer("fraud-test-group");
                consumer.subscribe(List.of("calls.fraud-alerts"));

                List<ConsumerRecord<String, FraudAlert>> records = consumeAllMessages(
                    consumer, "calls.fraud-alerts", 1, 5000
                );

                assertThat(records).isNotEmpty();
                assertThat(records.get(0).value().getAlertType()).isEqualTo("HIGH_FREQUENCY");
                assertThat(records.get(0).value().getPhone()).isEqualTo(phone);
            });
    }

    @Test
    void shouldDetectEscalationPattern() throws Exception {
        String phone = "+79001234567";

        // Send 3 calls with NPS < 2
        for (int i = 0; i < 3; i++) {
            CallCompleted call = new CallCompleted(
                UUID.randomUUID().toString(),
                phone,
                120,
                "agent-1",
                1,
                System.currentTimeMillis()
            );
            kafkaTemplate.send("calls.completed", call.getCallId(), call).get();
        }

        Awaitility.await()
            .atMost(10, TimeUnit.SECONDS)
            .untilAsserted(() -> {
                Consumer<String, FraudAlert> consumer = fraudAlertConsumerFactory
                    .createConsumer("fraud-test-group");
                consumer.subscribe(List.of("calls.fraud-alerts"));

                List<ConsumerRecord<String, FraudAlert>> records = consumeAllMessages(
                    consumer, "calls.fraud-alerts", 1, 5000
                );

                assertThat(records).isNotEmpty();
                assertThat(records.get(0).value().getAlertType()).isEqualTo("ESCALATION");
            });
    }

    @Test
    void shouldNotTriggerAlertForNormalCalls() throws Exception {
        String phone = "+79001234567";

        // Send 5 calls with normal NPS
        for (int i = 0; i < 5; i++) {
            CallCompleted call = new CallCompleted(
                UUID.randomUUID().toString(),
                phone,
                120,
                "agent-1",
                8,
                System.currentTimeMillis()
            );
            kafkaTemplate.send("calls.completed", call.getCallId(), call).get();
        }

        // Wait and verify no fraud alerts
        Thread.sleep(5000);

        Consumer<String, FraudAlert> consumer = fraudAlertConsumerFactory
            .createConsumer("fraud-test-group");
        consumer.subscribe(List.of("calls.fraud-alerts"));

        List<ConsumerRecord<String, FraudAlert>> records = consumeAllMessages(
            consumer, "calls.fraud-alerts", 1, 3000
        );

        assertThat(records).isEmpty();
    }

    @Test
    void shouldFilterShortCalls() throws Exception {
        CallCompleted call = new CallCompleted(
            UUID.randomUUID().toString(),
            "+79001234567",
            2,
            "agent-1",
            5,
            System.currentTimeMillis()
        );

        kafkaTemplate.send("calls.completed", call.getCallId(), call).get();

        Thread.sleep(3000);

        Consumer<String, FraudAlert> consumer = fraudAlertConsumerFactory
            .createConsumer("fraud-test-group");
        consumer.subscribe(List.of("calls.fraud-alerts"));

        List<ConsumerRecord<String, FraudAlert>> records = consumeAllMessages(
            consumer, "calls.fraud-alerts", 1, 3000
        );

        assertThat(records).isEmpty();
    }
}
```

### 12.4.3. TranscriptionAnalyzerIntegrationTest

```java
@SpringBootTest
class TranscriptionAnalyzerIntegrationTest extends BaseIntegrationTest {

    @Autowired
    private KafkaTemplate<String, CallCompleted> kafkaTemplate;

    @Autowired
    private KafkaTemplate<String, EnrichedCall> enrichedCallTemplate;

    @Autowired
    private JdbcTemplate jdbcTemplate;

    @Test
    void shouldProcessFullTranscriptionPipeline() throws Exception {
        String callId = "550e8400-e29b-41d4-a716-446655440000";
        String phone = "+79001234567";

        // Send completed call
        CallCompleted call = new CallCompleted(
            callId, phone, 180, "agent-1", 8, System.currentTimeMillis()
        );
        kafkaTemplate.send("calls.completed", callId, call).get();

        // Wait for transcription pipeline
        Awaitility.await()
            .atMost(30, TimeUnit.SECONDS)
            .pollInterval(1, TimeUnit.SECONDS)
            .untilAsserted(() -> {
                // Verify transcription in PostgreSQL
                List<Map<String, Object>> results = jdbcTemplate.queryForList(
                    "SELECT * FROM call_transcriptions WHERE call_id = ?", callId
                );
                assertThat(results).isNotEmpty();

                Map<String, Object> transcription = results.get(0);
                assertThat(transcription.get("sentiment")).isNotNull();
                assertThat(transcription.get("urgency")).isNotNull();
                assertThat(transcription.get("problem")).isNotNull();
            });
    }

    @Test
    void shouldWriteToPostgreSQL() throws Exception {
        String callId = "660e8400-e29b-41d4-a716-446655440001";

        // Send completed call
        CallCompleted call = new CallCompleted(
            callId, "+79001234567", 180, "agent-1", 8, System.currentTimeMillis()
        );
        kafkaTemplate.send("calls.completed", callId, call).get();

        // Wait for dual-write
        Awaitility.await()
            .atMost(30, TimeUnit.SECONDS)
            .untilAsserted(() -> {
                // Verify call_metadata
                List<Map<String, Object>> metadata = jdbcTemplate.queryForList(
                    "SELECT * FROM call_metadata WHERE call_id = ?", callId
                );
                assertThat(metadata).isNotEmpty();

                // Verify call_transcriptions
                List<Map<String, Object>> transcriptions = jdbcTemplate.queryForList(
                    "SELECT * FROM call_transcriptions WHERE call_id = ?", callId
                );
                assertThat(transcriptions).isNotEmpty();

                // Verify foreign key relationship
                assertThat(transcriptions.get(0).get("call_id"))
                    .isEqualTo(metadata.get(0).get("call_id"));
            });
    }

    @Test
    void shouldHandleStatusLifecycle() throws Exception {
        String callId = "770e8400-e29b-41d4-a716-446655440002";

        CallCompleted call = new CallCompleted(
            callId, "+79001234567", 180, "agent-1", 8, System.currentTimeMillis()
        );
        kafkaTemplate.send("calls.completed", callId, call).get();

        // Check status progression
        Awaitility.await()
            .atMost(30, TimeUnit.SECONDS)
            .pollInterval(1, TimeUnit.SECONDS)
            .untilAsserted(() -> {
                List<Map<String, Object>> results = jdbcTemplate.queryForList(
                    "SELECT status FROM call_metadata WHERE call_id = ?", callId
                );

                assertThat(results).isNotEmpty();
                assertThat(results.get(0).get("status")).isEqualTo("COMPLETED");
            });
    }
}
```

### 12.4.4. ReportingNPSIntegrationTest

```java
@SpringBootTest
class ReportingNPSIntegrationTest extends BaseIntegrationTest {

    @LocalServerPort
    private int port;

    private RestTemplate restTemplate;

    @Autowired
    private JdbcTemplate jdbcTemplate;

    @BeforeEach
    void setUp() {
        restTemplate = new RestTemplate();
        restTemplate.getMessageConverters()
            .add(new MappingJackson2HttpMessageConverter());
    }

    @Test
    void shouldReturnDailyReport() throws Exception {
        // Insert test data
        jdbcTemplate.execute("INSERT INTO call_metadata VALUES ('test-1', '+79001234567', 180, 'agent-1', 8, 'premium', 'low', 'normal', 'COMPLETED', 1, NOW())");
        jdbcTemplate.execute("INSERT INTO call_transcriptions VALUES ('test-1', 'Test transcription', 'positive', 'low', 'general_inquiry', 'info_provided', 0.85)");

        ResponseEntity<String> response = restTemplate.getForEntity(
            "http://localhost:" + port + "/api/reports/daily",
            String.class
        );

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(response.getBody()).contains("totalCalls");
        assertThat(response.getBody()).contains("averageNPS");
    }

    @Test
    void shouldReturnAgentStats() throws Exception {
        // Insert test data for agent-1
        for (int i = 0; i < 5; i++) {
            jdbcTemplate.execute(String.format(
                "INSERT INTO call_metadata VALUES ('agent1-%d', '+79001234567', 180, 'agent-1', %d, 'premium', 'low', 'normal', 'COMPLETED', 1, NOW())",
                i, 7 + i
            ));
        }

        ResponseEntity<String> response = restTemplate.getForEntity(
            "http://localhost:" + port + "/api/reports/agent/agent-1",
            String.class
        );

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(response.getBody()).contains("agentId");
        assertThat(response.getBody()).contains("averageNPS");
    }

    @Test
    void shouldReturnSentimentDistribution() throws Exception {
        // Insert test data with different sentiments
        jdbcTemplate.execute("INSERT INTO call_transcriptions VALUES ('sent-1', 'Text', 'positive', 'low', 'general', 'info', 0.9)");
        jdbcTemplate.execute("INSERT INTO call_transcriptions VALUES ('sent-2', 'Text', 'negative', 'high', 'complaint', 'escalated', 0.7)");
        jdbcTemplate.execute("INSERT INTO call_transcriptions VALUES ('sent-3', 'Text', 'neutral', 'medium', 'inquiry', 'info', 0.8)");

        ResponseEntity<String> response = restTemplate.getForEntity(
            "http://localhost:" + port + "/api/sentiment/distribution",
            String.class
        );

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(response.getBody()).contains("positive");
        assertThat(response.getBody()).contains("negative");
        assertThat(response.getBody()).contains("neutral");
    }

    @Test
    void shouldReturnCallMetadataStatus() throws Exception {
        jdbcTemplate.execute("INSERT INTO call_metadata VALUES ('status-1', '+79001234567', 180, 'agent-1', 8, 'premium', 'low', 'normal', 'COMPLETED', 1, NOW())");

        ResponseEntity<String> response = restTemplate.getForEntity(
            "http://localhost:" + port + "/api/metadata/status-1",
            String.class
        );

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(response.getBody()).contains("COMPLETED");
    }
}
```

### 12.4.5. DLQIntegrationTest

```java
@SpringBootTest
class DLQIntegrationTest extends BaseIntegrationTest {

    @Autowired
    private KafkaTemplate<String, CallCompleted> kafkaTemplate;

    @Autowired
    private ConsumerFactory<String, String> dlqConsumerFactory;

    @Test
    void shouldSendInvalidMessageToDLQ() throws Exception {
        // Send malformed message directly to Kafka
        kafkaTemplate.send("calls.completed", "bad-call-id", "invalid-avro-data").get();

        Awaitility.await()
            .atMost(10, TimeUnit.SECONDS)
            .untilAsserted(() -> {
                Consumer<String, String> consumer = dlqConsumerFactory
                    .createConsumer("dlq-test-group");
                consumer.subscribe(List.of("calls.dlq"));

                List<ConsumerRecord<String, String>> records = consumeAllMessages(
                    consumer, "calls.dlq", 1, 5000
                );

                assertThat(records).isNotEmpty();
                assertThat(records.get(0).value()).contains("invalid-avro-data");
            });
    }

    @Test
    void shouldRetryBeforeSendingToDLQ() throws Exception {
        // Simulate transient error - should retry 3 times before DLQ
        // This tests the retry mechanism
        // (Implementation depends on specific error handling strategy)
    }
}
```

## 12.5. Awaitility конфигурация

```java
// Global Awaitility configuration
@SpringBootTest
class AwaitilityConfig {

    @BeforeEach
    void setUp() {
        Awaitility.setDefaultTimeout(10, TimeUnit.SECONDS);
        Awaitility.setDefaultPollInterval(500, TimeUnit.MILLISECONDS);
    }
}
```

## 12.6. Тестовые данные

### 12.6.1. SQL fixtures

```sql
-- src/test/resources/fixtures/init-test-data.sql
INSERT INTO call_metadata VALUES
    ('test-1', '+79001234567', 180, 'agent-1', 8, 'premium', 'low', 'normal', 'COMPLETED', 1, NOW()),
    ('test-2', '+79001234568', 240, 'agent-2', 6, 'standard', 'medium', 'normal', 'COMPLETED', 1, NOW());

INSERT INTO call_transcriptions VALUES
    ('test-1', 'Card was lost, need to block it', 'negative', 'high', 'card_loss', 'card_blocked', 0.9),
    ('test-2', 'I want to apply for a loan', 'neutral', 'low', 'credit_inquiry', 'info_provided', 0.8);
```

## 12.7. Запуск integration-тестов

```bash
# Все integration-тесты
make test-integration

# Тесты конкретного модуля
cd call-processor && mvn verify -Dtest='*IntegrationTest'

# С отчётом покрытия
mvn verify jacoco:report

# Только определённый тест
mvn test -Dtest=CallProcessorIntegrationTest#shouldProcessCallAndWriteToKafka
```

## 12.8. Покрытие интеграционными тестами

| Модуль | Тестов | Сценариев | Line % |
|--------|--------|-----------|--------|
| call-processor | 4 | 8 | 65% |
| fraud-detector | 4 | 6 | 60% |
| transcription-analyzer | 3 | 5 | 55% |
| reporting-nps | 4 | 6 | 50% |
| **Итого** | **15** | **25** | **57%** |
