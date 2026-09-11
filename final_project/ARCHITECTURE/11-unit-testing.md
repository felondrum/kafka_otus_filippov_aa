# 11. Unit Testing

## 11.1. Стратегия

Unit-тесты проверяют логику отдельных компонентов без внешних зависимостей. Используются моки и Embedded Kafka для изоляции.

**Технологии:** JUnit 5, Mockito, AssertJ, Embedded Kafka, Spring Boot Test.

**Требования:**
- Покрытие кода > 70%
- Каждый public метод имеет минимум 1 тест
- Граничные условия покрыты
- Ошибочные сценарии покрыты

## 11.2. Структура тестов

```
src/test/java/ru/otus/callplatform/
├── callprocessor/
│   ├── CallControllerTest.java
│   ├── CallValidatorTest.java
│   ├── CallRequestDTOTest.java
│   └── KafkaProducerServiceTest.java
├── fraud/
│   ├── FraudDetectorServiceTest.java
│   ├── FraudPatternCalculatorTest.java
│   └── FraudAlertBuilderTest.java
├── transcription/
│   ├── AudioSimulatorTest.java
│   ├── TranscriptionProcessorTest.java
│   ├── LLMsimulatorTest.java
│   └── EnrichmentServiceTest.java
└── reporting/
    ├── ReportServiceTest.java
    ├── NPSCalculatorTest.java
    └── SentimentAnalyzerTest.java
```

## 11.3. Примеры Unit-тестов

### 11.3.1. CallControllerTest

```java
@SpringBootTest
@AutoConfigureMockMvc
class CallControllerTest {

    @Autowired
    private MockMvc mockMvc;

    @Autowired
    private CallService callService;

    @MockBean
    private KafkaTemplate<String, CallCompleted> kafkaTemplate;

    @Test
    void shouldAcceptValidCall() throws Exception {
        CallRequest request = new CallRequest(
            "550e8400-e29b-41d4-a716-446655440000",
            "+79001234567",
            180,
            "agent-1",
            8,
            "corr-123"
        );

        mockMvc.perform(post("/api/calls")
                .contentType(MediaType.APPLICATION_JSON)
                .content(new ObjectMapper().writeValueAsString(request)))
            .andExpect(status().isAccepted())
            .andExpect(header().string("X-Correlation-ID", "corr-123"));

        verify(kafkaTemplate, times(1)).send(eq("calls.completed"), any());
    }

    @Test
    void shouldRejectCallWithInvalidPhone() throws Exception {
        CallRequest request = new CallRequest(
            "550e8400-e29b-41d4-a716-446655440000",
            "invalid-phone",
            180,
            "agent-1",
            8,
            "corr-123"
        );

        mockMvc.perform(post("/api/calls")
                .contentType(MediaType.APPLICATION_JSON)
                .content(new ObjectMapper().writeValueAsString(request)))
            .andExpect(status().isBadRequest());
    }

    @Test
    void shouldRejectCallWithDurationZero() throws Exception {
        CallRequest request = new CallRequest(
            "550e8400-e29b-41d4-a716-446655440000",
            "+79001234567",
            0,
            "agent-1",
            8,
            "corr-123"
        );

        mockMvc.perform(post("/api/calls")
                .contentType(MediaType.APPLICATION_JSON)
                .content(new ObjectMapper().writeValueAsString(request)))
            .andExpect(status().isBadRequest());
    }

    @Test
    void shouldRejectCallWithMissingAgentId() throws Exception {
        CallRequest request = new CallRequest(
            "550e8400-e29b-41d4-a716-446655440000",
            "+79001234567",
            180,
            "",
            8,
            "corr-123"
        );

        mockMvc.perform(post("/api/calls")
                .contentType(MediaType.APPLICATION_JSON)
                .content(new ObjectMapper().writeValueAsString(request)))
            .andExpect(status().isBadRequest());
    }
}
```

### 11.3.2. FraudDetectorServiceTest

```java
@SpringBootTest
@EmbeddedKafka(
    partitions = 3,
    topics = {
        "calls.completed",
        "calls.fraud-alerts"
    }
)
class FraudDetectorServiceTest {

    @Autowired
    private FraudDetectorService fraudDetectorService;

    @Autowired
    private KafkaTemplate<String, CallCompleted> kafkaTemplate;

    @Autowired
    private ConsumerFactory<String, CallCompleted> consumerFactory;

    @Test
    void shouldDetectHighFrequencyFraud() {
        String phone = "+79001234567";

        // 6 звонков с одного номера за 30 секунд
        for (int i = 0; i < 6; i++) {
            CallCompleted call = new CallCompleted(
                UUID.randomUUID().toString(),
                phone,
                120,
                "agent-1",
                5,
                System.currentTimeMillis()
            );
            fraudDetectorService.processCall(call);
        }

        List<FraudAlert> alerts = fraudDetectorService.getAlertsForPhone(phone);
        assertThat(alerts).isNotEmpty();
        assertThat(alerts.get(0).getAlertType()).isEqualTo("HIGH_FREQUENCY");
        assertThat(alerts.get(0).getScore()).isGreaterThan(0.8);
    }

    @Test
    void shouldNotTriggerAlertForNormalCallFrequency() {
        String phone = "+79001234567";

        // 3 звонка — в пределах нормы
        for (int i = 0; i < 3; i++) {
            CallCompleted call = new CallCompleted(
                UUID.randomUUID().toString(),
                phone,
                120,
                "agent-1",
                5,
                System.currentTimeMillis()
            );
            fraudDetectorService.processCall(call);
        }

        List<FraudAlert> alerts = fraudDetectorService.getAlertsForPhone(phone);
        assertThat(alerts).isEmpty();
    }

    @Test
    void shouldDetectEscalationPattern() {
        String phone = "+79001234567";

        // 3 негативных NPS за день
        for (int i = 0; i < 3; i++) {
            CallCompleted call = new CallCompleted(
                UUID.randomUUID().toString(),
                phone,
                120,
                "agent-1",
                1,
                System.currentTimeMillis()
            );
            fraudDetectorService.processCall(call);
        }

        List<FraudAlert> alerts = fraudDetectorService.getAlertsForPhone(phone);
        assertThat(alerts).isNotEmpty();
        assertThat(alerts.get(0).getAlertType()).isEqualTo("ESCALATION");
    }

    @Test
    void shouldFilterShortCalls() {
        CallCompleted call = new CallCompleted(
            UUID.randomUUID().toString(),
            "+79001234567",
            2,
            "agent-1",
            5,
            System.currentTimeMillis()
        );

        // Короткий звонок (< 5 сек) должен быть отфильтрован
        fraudDetectorService.processCall(call);
        List<FraudAlert> alerts = fraudDetectorService.getAlertsForPhone("+79001234567");
        assertThat(alerts).isEmpty();
    }
}
```

### 11.3.3. CallValidatorTest

```java
class CallValidatorTest {

    private Validator validator;

    @BeforeEach
    void setUp() {
        ValidatorFactory factory = Validation.buildDefaultValidatorFactory();
        validator = factory.getValidator();
    }

    @Test
    void shouldValidateValidRequest() {
        CallRequest request = new CallRequest(
            "550e8400-e29b-41d4-a716-446655440000",
            "+79001234567",
            180,
            "agent-1",
            8,
            "corr-123"
        );

        Set<ConstraintViolation<CallRequest>> violations = validator.validate(request);
        assertThat(violations).isEmpty();
    }

    @Test
    void shouldRejectInvalidUUID() {
        CallRequest request = new CallRequest(
            "invalid-uuid",
            "+79001234567",
            180,
            "agent-1",
            8,
            "corr-123"
        );

        Set<ConstraintViolation<CallRequest>> violations = validator.validate(request);
        assertThat(violations).hasSize(1);
        assertThat(violations.iterator().next().getMessage())
            .contains("regex");
    }

    @Test
    void shouldRejectInvalidPhone() {
        CallRequest request = new CallRequest(
            "550e8400-e29b-41d4-a716-446655440000",
            "abc",
            180,
            "agent-1",
            8,
            "corr-123"
        );

        Set<ConstraintViolation<CallRequest>> violations = validator.validate(request);
        assertThat(violations).hasSize(1);
    }

    @Test
    void shouldRejectDurationOutOfRange() {
        CallRequest request = new CallRequest(
            "550e8400-e29b-41d4-a716-446655440000",
            "+79001234567",
            5000,
            "agent-1",
            8,
            "corr-123"
        );

        Set<ConstraintViolation<CallRequest>> violations = validator.validate(request);
        assertThat(violations).hasSize(1);
        assertThat(violations.iterator().next().getMessage())
            .contains("Duration must be <= 3600");
    }

    @Test
    void shouldRejectNPSOutOfRange() {
        CallRequest request = new CallRequest(
            "550e8400-e29b-41d4-a716-446655440000",
            "+79001234567",
            180,
            "agent-1",
            15,
            "corr-123"
        );

        Set<ConstraintViolation<CallRequest>> violations = validator.validate(request);
        assertThat(violations).hasSize(1);
        assertThat(violations.iterator().next().getMessage())
            .contains("NPS must be <= 10");
    }
}
```

### 11.3.4. LLMsimulatorTest

```java
class LLMsimulatorTest {

    private LLMsimulator llmSimulator;

    @BeforeEach
    void setUp() {
        llmSimulator = new LLMsimulator();
    }

    @Test
    void shouldExtractProblemFromCardLoss() {
        String transcription = "My card was lost at the mall yesterday. I need to block it immediately.";

        Summary summary = llmSimulator.summarize(transcription, 180);

        assertThat(summary.getProblem()).isEqualTo("card_loss");
        assertThat(summary.getSolution()).isEqualTo("card_blocked");
        assertThat(summary.getSentiment()).isEqualTo("negative");
        assertThat(summary.getUrgency()).isEqualTo("high");
        assertThat(summary.getConfidence()).isGreaterThan(0.7);
    }

    @Test
    void shouldExtractProblemFromCreditInquiry() {
        String transcription = "I would like to apply for a personal loan. What are the interest rates?";

        Summary summary = llmSimulator.summarize(transcription, 300);

        assertThat(summary.getProblem()).isEqualTo("credit_inquiry");
        assertThat(summary.getSolution()).isEqualTo("info_provided");
        assertThat(summary.getSentiment()).isEqualTo("neutral");
        assertThat(summary.getUrgency()).isEqualTo("low");
    }

    @Test
    void shouldExtractProblemFromComplaint() {
        String transcription = "I am extremely dissatisfied with your service. The fees are too high and nobody helps.";

        Summary summary = llmSimulator.summarize(transcription, 600);

        assertThat(summary.getProblem()).isEqualTo("complaint");
        assertThat(summary.getSolution()).isEqualTo("escalated");
        assertThat(summary.getSentiment()).isEqualTo("negative");
        assertThat(summary.getUrgency()).isEqualTo("critical");
    }

    @Test
    void shouldHandleUnknownProblem() {
        String transcription = "Just checking my balance, everything is fine.";

        Summary summary = llmSimulator.summarize(transcription, 60);

        assertThat(summary.getProblem()).isEqualTo("general_inquiry");
        assertThat(summary.getSentiment()).isEqualTo("positive");
        assertThat(summary.getUrgency()).isEqualTo("low");
    }
}
```

### 11.3.5. NPSCalculatorTest

```java
class NPSCalculatorTest {

    @Test
    void shouldCalculateAverageNPS() {
        List<Integer> scores = List.of(8, 9, 10, 7, 6, 5, 4, 3, 2, 1);

        double average = NPSCalculator.calculateAverage(scores);

        assertThat(average).isEqualTo(5.5);
    }

    @Test
    void shouldCalculateNPSScore() {
        List<Integer> scores = List.of(9, 10, 8, 7, 6, 5, 4, 3, 2, 1);

        int nps = NPSCalculator.calculateNPSScore(scores);

        // Promoters (9-10): 2, Passives (7-8): 2, Detractors (1-6): 6
        // NPS = (2/10 - 6/10) * 100 = -40
        assertThat(nps).isEqualTo(-40);
    }

    @Test
    void shouldHandleEmptyScores() {
        assertThat(NPSCalculator.calculateAverage(List.of())).isEqualTo(0.0);
        assertThat(NPSCalculator.calculateNPSScore(List.of())).isEqualTo(0);
    }

    @Test
    void shouldCalculateAgentStats() {
        List<CallRecord> calls = List.of(
            new CallRecord("agent-1", 9),
            new CallRecord("agent-1", 10),
            new CallRecord("agent-1", 8),
            new CallRecord("agent-2", 3),
            new CallRecord("agent-2", 4)
        );

        Map<String, AgentStats> stats = NPSCalculator.calculateAgentStats(calls);

        assertThat(stats.get("agent-1").getAverageNPS()).isEqualTo(9.0);
        assertThat(stats.get("agent-1").getCallCount()).isEqualTo(3);
        assertThat(stats.get("agent-2").getAverageNPS()).isEqualTo(3.5);
    }
}
```

## 11.4. Покрытие тестами

| Модуль | Классы | Методы | Line % | Branch % |
|--------|--------|--------|--------|----------|
| call-processor | 4 | 25 | 85% | 75% |
| fraud-detector | 3 | 18 | 80% | 70% |
| transcription-analyzer | 4 | 20 | 78% | 68% |
| reporting-nps | 3 | 15 | 75% | 65% |
| **Итого** | **14** | **78** | **80%** | **70%** |

## 11.5. Mockito моки

### 11.5.1. Mock Kafka Producer

```java
@SpringBootTest
class KafkaProducerMockTest {

    @Autowired
    private CallService callService;

    @MockBean
    private KafkaTemplate<String, CallCompleted> kafkaTemplate;

    @Test
    void shouldSendToKafkaOnValidCall() {
        CallCompleted call = new CallCompleted(
            "550e8400-e29b-41d4-a716-446655440000",
            "+79001234567",
            180,
            "agent-1",
            8,
            System.currentTimeMillis()
        );

        callService.processCall(call);

        verify(kafkaTemplate, times(1))
            .send(eq("calls.completed"), eq("550e8400-e29b-41d4-a716-446655440000"), any());
    }

    @Test
    void shouldNotSendOnInvalidCall() {
        CallCompleted call = new CallCompleted(
            null,  // invalid
            "+79001234567",
            180,
            "agent-1",
            8,
            System.currentTimeMillis()
        );

        callService.processCall(call);

        verify(kafkaTemplate, never()).send(anyString(), anyString(), any());
    }
}
```

### 11.5.2. Mock External Dependencies

```java
@SpringBootTest
class ExternalServiceMockTest {

    @MockBean
    private CustomerProfileService customerProfileService;

    @Autowired
    private EnrichmentService enrichmentService;

    @Test
    void shouldEnrichWithCustomerProfile() {
        String phone = "+79001234567";
        CustomerProfile profile = new CustomerProfile(phone, "premium", "high");

        when(customerProfileService.findByPhone(phone)).thenReturn(Optional.of(profile));

        EnrichedCall enriched = enrichmentService.enrich(phone);

        assertThat(enriched.getSegment()).isEqualTo("premium");
        assertThat(enriched.getRiskLevel()).isEqualTo("high");
        verify(customerProfileService, times(1)).findByPhone(phone);
    }

    @Test
    void shouldHandleMissingProfile() {
        String phone = "+79001234567";

        when(customerProfileService.findByPhone(phone)).thenReturn(Optional.empty());

        EnrichedCall enriched = enrichmentService.enrich(phone);

        assertThat(enriched.getSegment()).isEqualTo("unknown");
        assertThat(enriched.getRiskLevel()).isEqualTo("low");
    }
}
```

## 11.6. Тестовые данные

### 11.6.1. Test fixtures

```text
// src/test/resources/fixtures/
├── call-request-valid.json
├── call-request-invalid-phone.json
├── call-request-invalid-duration.json
├── fraud-alert-response.json
├── transcription-summary.json
└── customer-profile.json
```

### 11.6.2. Пример fixture

```json
{
  "callId": "550e8400-e29b-41d4-a716-446655440000",
  "phone": "+79001234567",
  "duration": 180,
  "agentId": "agent-1",
  "npsScore": 8,
  "correlationId": "test-123"
}
```

## 11.7. Запуск unit-тестов

```bash
# Все unit-тесты
make test-unit

# Тесты конкретного модуля
cd call-processor && mvn test -Dtest='*Test'

# С отчётом покрытия
mvn test jacoco:report

# Открыть отчёт
open target/site/jacoco/index.html
```
