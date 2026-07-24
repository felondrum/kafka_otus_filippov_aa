import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Главный класс для оркестрации выполнения приложения транзакционной отправки в Kafka.
 * 
 * <p>Выполняет следующие действия:</p>
 * <ol>
 *   <li>Инициализирует приложение транзакционной отправки</li>
 *   <li>Выполняет полный цикл работы: удаление/создание топиков, отправка, чтение</li>
 * </ol>
 * 
 * <p>Для корректной работы необходимо запустить Kafka через docker-compose:
 * <pre>{@code
 * cd hw3
 * docker-compose up -d
 * }</pre>
 * </p>
 *
 */
public class Main {

    private static final Logger logger = LoggerFactory.getLogger(Main.class);

    /**
     * Точка входа в приложение.
     * 
     * @param args аргументы командной строки (не используются)
     */
    public static void main(String[] args) {
        logger.info("========================================");
        logger.info("Запуск приложения транзакционной отправки в Kafka");
        logger.info("========================================");
        
        TransactionHomeWork transactionApp = new TransactionHomeWork("localhost:9093");
        
        // Выполнение полного цикла работы
        logger.info("Выполнение полного цикла работы с Kafka...");
        transactionApp.run();
        logger.info("Полный цикл работы завершён");
        
        logger.info("========================================");
        logger.info("Приложение завершено успешно");
        logger.info("========================================");
    }
}
