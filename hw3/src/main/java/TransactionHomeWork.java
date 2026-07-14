import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.DeleteTopicsOptions;
import org.apache.kafka.clients.admin.DeleteTopicsResult;
import org.apache.kafka.clients.admin.ListTopicsOptions;
import org.apache.kafka.clients.admin.ListTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.KafkaException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

/**
 * Класс для демонстрации транзакционной работы с Kafka.
 * 
 * <p>Выполняет следующие действия при запуске:
 * <ul>
 *   <li>Удаляет все существующие топики (topic1, topic2)</li>
 *   <li>Создаёт новые топики topic1 и topic2</li>
 *   <li>Выполняет транзакционную отправку сообщений</li>
 *   <li>Читает и отображает результаты</li>
 * </ul></p>
 * 
 * <p>Используемые топики: topic1, topic2</p>
 * 
 * <p>Пример использования:
 * <pre>{@code
 * TransactionHomeWork transactionApp = new TransactionHomeWork("localhost:9093");
 * transactionApp.run();
 * }</pre>
 * </p>
 */
public class TransactionHomeWork {

    private static final Logger logger = LoggerFactory.getLogger(TransactionHomeWork.class);
    private static final String TOPIC_1 = "topic1";
    private static final String TOPIC_2 = "topic2";
    private static final String GROUP_ID = "transaction-consumer-group";
    private static final int PARTITIONS = 3;
    private static final short REPLICATION_FACTOR = 1;
    private static final long ADMIN_CLIENT_TIMEOUT_MS = 30000;
    
    private final String bootstrapServers;

    /**
     * Конструктор с указанием серверов Kafka.
     * 
     * @param bootstrapServers адреса Kafka брокеров в формате host:port
     */
    public TransactionHomeWork(String bootstrapServers) {
        this.bootstrapServers = bootstrapServers;
    }

    /**
     * Основной метод для выполнения полного цикла работы с Kafka.
     * 
     * <p>Выполняет следующие действия:</p>
     * <ol>
     *   <li>Удаляет существующие топики topic1 и topic2 (если есть)</li>
     *   <li>Создаёт новые топики topic1 и topic2</li>
     *   <li>Выполняет транзакционную отправку сообщений</li>
     *   <li>Читает и отображает результаты из топиков</li>
     * </ol>
     */
    public void run() {
        try {
            cleanupAndInitializeTopics();
            produceTransactions();
            consumeMessages();
        } catch (Exception e) {
            logger.error("Ошибка при выполнении полного цикла работы с Kafka: ", e);
            throw new RuntimeException("Не удалось выполнить полный цикл работы", e);
        }
    }

    /**
     * Удаляет существующие топики и создаёт новые.
     * 
     * <p>Использует AdminClient для удаления и создания топиков с таймаутом.</p>
     */
    private void cleanupAndInitializeTopics() {
        logger.info("Начало очистки и инициализации топиков");
        
        try (AdminClient adminClient = createAdminClient()) {
            // Шаг 1: Удаление существующих топиков
            deleteTopicsIfExists(adminClient, Arrays.asList(TOPIC_1, TOPIC_2));
            
            // Шаг 2: Создание новых топиков
            List<NewTopic> newTopics = Arrays.asList(
                new NewTopic(TOPIC_1, PARTITIONS, REPLICATION_FACTOR),
                new NewTopic(TOPIC_2, PARTITIONS, REPLICATION_FACTOR)
            );
            createTopics(adminClient, newTopics);
            
        } catch (Exception e) {
            logger.error("Ошибка при настройке топиков: ", e);
            throw new RuntimeException("Не удалось настроить топики", e);
        }
        
        logger.info("Топики topic1 и topic2 успешно созданы/пересозданы");
    }

    /**
     * Удаляет топики, если они существуют.
     * 
     * @param adminClient экземпляр AdminClient
     * @param topicNames список имен топиков для удаления
     */
    private void deleteTopicsIfExists(AdminClient adminClient, List<String> topicNames) {
        try {
            ListTopicsOptions listOptions = new ListTopicsOptions();
            ListTopicsResult listResult = adminClient.listTopics(listOptions);
            
            Set<String> existingTopics = listResult.names().get(ADMIN_CLIENT_TIMEOUT_MS, TimeUnit.MILLISECONDS);
            
            for (String topicName : topicNames) {
                if (existingTopics.contains(topicName)) {
                    logger.info("Удаление топика: {}", topicName);
                    DeleteTopicsOptions deleteOptions = new DeleteTopicsOptions();
                    DeleteTopicsResult deleteResult = adminClient.deleteTopics(Collections.singletonList(topicName), deleteOptions);
                    deleteResult.all().get(ADMIN_CLIENT_TIMEOUT_MS, TimeUnit.MILLISECONDS);
                    logger.info("Топик {} успешно удален", topicName);
                } else {
                    logger.debug("Топик {} не существует, пропуск удаления", topicName);
                }
            }
        } catch (Exception e) {
            logger.error("Ошибка при удалении топиков: ", e);
            throw new RuntimeException("Не удалось удалить топики", e);
        }
    }

    /**
     * Создаёт новые топики.
     * 
     * @param adminClient экземпляр AdminClient
     * @param newTopics список создаваемых топиков
     */
    private void createTopics(AdminClient adminClient, List<NewTopic> newTopics) {
        try {
            adminClient.createTopics(newTopics).all().get(ADMIN_CLIENT_TIMEOUT_MS, TimeUnit.MILLISECONDS);
            logger.info("Созданы топики: {}", newTopics.stream().map(NewTopic::name).toList());
        } catch (Exception e) {
            logger.error("Ошибка при создании топиков: ", e);
            throw new RuntimeException("Не удалось создать топики", e);
        }
    }

    /**
     * Основной метод для выполнения транзакционной отправки сообщений.
     * 
     * <p>Выполняет следующие действия:</p>
     * <ol>
     *   <li>Открывает транзакцию</li>
     *   <li>Отправляет по 5 сообщений в каждый топик (topic1, topic2)</li>
     *   <li>Подтверждает транзакцию (commit)</li>
     *   <li>Открывает новую транзакцию</li>
     *   <li>Отправляет по 2 сообщения в каждый топик</li>
     *   <li>Отменяет транзакцию (abort)</li>
     * </ol>
     * 
     * <p>Важно: Сообщения из отменённой транзакции не будут доступны для чтения
     * потребителями, так как Kafka транзакции обеспечивают атомарность операций.</p>
     */
    public void produceTransactions() {
        Properties props = createProducerProperties();
        
        try (Producer<String, String> producer = new KafkaProducer<>(props)) {
            // Инициализация транзакции
            producer.initTransactions();

            // Первая транзакция - отправляем 5 сообщений в каждый топик
            logger.info("Начало первой транзакции (commit)");
            producer.beginTransaction();
            
            for (int i = 1; i <= 5; i++) {
                sendRecord(producer, TOPIC_1, "TX1-Message-" + i + "-Topic1");
                sendRecord(producer, TOPIC_2, "TX1-Message-" + i + "-Topic2");
            }
            
            producer.commitTransaction();
            logger.info("Первая транзакция подтверждена (commit)");

            // Вторая транзакция - отправляем 2 сообщения в каждый топик
            logger.info("Начало второй транзакции (abort)");
            producer.beginTransaction();
            
            for (int i = 1; i <= 2; i++) {
                sendRecord(producer, TOPIC_1, "TX2-Message-" + i + "-Topic1");
                sendRecord(producer, TOPIC_2, "TX2-Message-" + i + "-Topic2");
            }
            
            // Отмена транзакции - сообщения не будут доступны
            producer.abortTransaction();
            logger.info("Вторая транзакция отменена (abort)");

        } catch (KafkaException e) {
            logger.error("Ошибка при отправке транзакций: ", e);
            throw new RuntimeException("Не удалось выполнить транзакции", e);
        }
    }

    /**
     * Отправляет сообщение в указанный топик.
     * 
     * @param producer экземпляр KafkaProducer
     * @param topic имя топика
     * @param message сообщение для отправки
     * @param <K> тип ключа
     * @param <V> тип значения
     */
    private <K, V> void sendRecord(Producer<K, V> producer, String topic, V message) {
        ProducerRecord<K, V> record = new ProducerRecord<>(topic, message);
        Future<RecordMetadata> future = producer.send(record);
        
        try {
            RecordMetadata metadata = future.get();
            logger.info("Сообщение отправлено в топик {} [partition: {}, offset: {}]", 
                topic, metadata.partition(), metadata.offset());
        } catch (Exception e) {
            logger.error("Ошибка при отправке сообщения в топик {}: ", topic, e);
            throw new RuntimeException("Не удалось отправить сообщение", e);
        }
    }

    /**
     * Метод для чтения сообщений из топиков.
     * 
     * <p>Читает только подтверждённые транзакции. Сообщения из отменённых
     * транзакций не будут прочитаны благодаря механизму изоляции транзакций
     * в Kafka (isolation.level=read_committed).</p>
     * 
     * <p>Читает из топиков topic1 и topic2 с использованием группы потребителей.</p>
     */
    public void consumeMessages() {
        Properties props = createConsumerProperties();
        
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
            consumer.subscribe(Arrays.asList(TOPIC_1, TOPIC_2));
            
            logger.info("Начало чтения сообщений из топиков: {}, {}", TOPIC_1, TOPIC_2);
            
            boolean hasMessages = false;
            int messageCount = 0;
            
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(java.time.Duration.ofMillis(100));
                
                if (records.isEmpty()) {
                    if (hasMessages) {
                        logger.info("Все сообщения прочитаны. Всего прочитано: {} сообщений", messageCount);
                        break;
                    }
                    continue;
                }
                
                hasMessages = true;
                for (ConsumerRecord<String, String> record : records) {
                    messageCount++;
                    logger.info("Прочитано: топик={}, partition={}, offset={}, ключ={}, значение={}", 
                        record.topic(), record.partition(), record.offset(), 
                        record.key(), record.value());
                }
            }
            
        } catch (KafkaException e) {
            logger.error("Ошибка при чтении сообщений: ", e);
            throw new RuntimeException("Не удалось прочитать сообщения", e);
        }
    }

    /**
     * Создаёт и настраивает свойства для Producer.
     * 
     * <p>Включает транзакционные возможности:
     * <ul>
     *   <li>transactional.id для идентификации транзакции</li>
     *   <li>ack=all для обязательного подтверждения от всех реплик</li>
     * </ul></p>
     * 
     * @return Properties с настройками Producer
     */
    private Properties createProducerProperties() {
        Properties props = new Properties();
        props.put("bootstrap.servers", bootstrapServers);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        
        // Настройки для транзакций
        props.put("transactional.id", "transaction-home-work-" + System.currentTimeMillis());
        props.put("acks", "all");
        props.put("enable.idempotence", "true");
        
        return props;
    }

    /**
     * Создаёт и настраивает свойства для Consumer.
     * 
     * <p>Устанавливает уровень изоляции read_committed, чтобы читать только
     * подтверждённые транзакции и исключать сообщения из отменённых транзакций.</p>
     * 
     * @return Properties с настройками Consumer
     */
    private Properties createConsumerProperties() {
        Properties props = new Properties();
        props.put("bootstrap.servers", bootstrapServers);
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("group.id", GROUP_ID);
        props.put("auto.offset.reset", "earliest");
        
        // Важно: читаем только подтверждённые транзакции
        props.put("isolation.level", "read_committed");
        
        return props;
    }

    /**
     * Создаёт и настраивает AdminClient для управления топиками.
     * 
     * @return AdminClient экземпляр
     */
    private AdminClient createAdminClient() {
        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        return AdminClient.create(props);
    }
}
