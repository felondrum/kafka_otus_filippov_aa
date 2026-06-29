#!/bin/bash

TOPIC="otus-test-topic"

# Удаление существующего топика
echo -e "\n=== Чистим старые топики ==="
docker exec -it kafka-new /bin/bash -c "/opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9093 --delete --topic ${TOPIC} --command-config /etc/kafka/client-alice.properties" 2>&1 || true
sleep 3

# Создание топика
echo -e "\n=== Создаем топик ==="
docker exec -it kafka-new /bin/bash -c "/opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9093 --create --topic ${TOPIC} --partitions 3 --replication-factor 1 --if-not-exists --command-config /etc/kafka/client-alice.properties"

# Удаление старых ACL
echo -e "\n=== Чистим старые ACL ==="
docker exec -it kafka-new /bin/bash -c "/opt/kafka/bin/kafka-acls.sh --bootstrap-server localhost:9093 --remove --force --topic ${TOPIC} --command-config /etc/kafka/client-alice.properties" 2>&1 || true
sleep 2

# Настройка ACL
echo -e "\n=== Настраиваем ACL ==="
echo "Добавляем WRITE для Алисы"
docker exec -it kafka-new /bin/bash -c "/opt/kafka/bin/kafka-acls.sh --bootstrap-server localhost:9093 --add --allow-principal User:alice --operation Write --topic ${TOPIC} --command-config /etc/kafka/client-alice.properties"

echo "Добавляем READ для Боба..."
docker exec -it kafka-new /bin/bash -c "/opt/kafka/bin/kafka-acls.sh --bootstrap-server localhost:9093 --add --allow-principal User:bob --operation Read --topic ${TOPIC} --command-config /etc/kafka/client-bob.properties"

# Просмотр ACL
echo -e "\n=== Проверяем ACL ==="
docker exec -it kafka-new /bin/bash -c "/opt/kafka/bin/kafka-acls.sh --bootstrap-server localhost:9093 --list --command-config /etc/kafka/client-alice.properties"

echo -e "\n========================================="
echo "Тесты"
echo "========================================="

# Создаем тестовое сообщение в контейнере
docker exec -it kafka-new /bin/bash -c "echo 'Привет от Алисы. Дата и время: $(date)' > /tmp/alice-message.txt"

# Тест 1: Alice - отправка сообщения (должно быть успешно)
echo -e "\n[Тест 1] Алиса: Write permission"
echo "----------------------------------------"
echo "1. Список топиков:"
docker exec -it kafka-new /bin/bash -c "/opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9093 --list --command-config /etc/kafka/client-alice.properties" 2>&1

echo -e "\n2. Алиса, отправка сообщения:"
docker exec -it kafka-new /bin/bash -c "cat /tmp/alice-message.txt | /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server localhost:9093 --topic ${TOPIC} --producer.config /etc/kafka/client-alice.properties" 2>&1

echo -e "\n3. Алиса пытается прочитать (FAIL: TimeoutException):"
docker exec -it kafka-new /bin/bash -c "/opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9093 --topic ${TOPIC} --consumer.config /etc/kafka/client-alice.properties --from-beginning --max-messages 1 --timeout-ms 3000" 2>&1

# Ждем, чтобы сообщение успело записаться
sleep 3

# Тест 2: Bob - чтение сообщений (должно быть успешно)
echo -e "\n[TEST 2] Боб: Read permission"
echo "----------------------------------------"
echo "1. Список топиков:"
docker exec -it kafka-new /bin/bash -c "/opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9093 --list --command-config /etc/kafka/client-bob.properties" 2>&1

echo -e "\n2. Боб пытается писать (FAIL: TopicAuthorizationException):"
docker exec -it kafka-new /bin/bash -c "echo 'Hello from Bob' | /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server localhost:9093 --topic ${TOPIC} --producer.config /etc/kafka/client-bob.properties" 2>&1

echo -e "\n3. Боб читает сообщения (должен увидеть сообщение от Алисы):"
docker exec -it kafka-new /bin/bash -c "/opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9093 --topic ${TOPIC} --consumer.config /etc/kafka/client-bob.properties --from-beginning --max-messages 1 --timeout-ms 5000" 2>&1

# Тест 3: Charlie - без прав (должны быть ошибки)
echo -e "\n[TEST 3] Чарли: No permissions"
echo "----------------------------------------"
echo "1. Список топиков:"
docker exec -it kafka-new /bin/bash -c "/opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9093 --list --command-config /etc/kafka/client-charlie.properties" 2>&1

echo -e "\n2. Чарли пытается писать (FAIL: TopicAuthorizationException):"
docker exec -it kafka-new /bin/bash -c "echo 'Hello from Charlie' | /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server localhost:9093 --topic ${TOPIC} --producer.config /etc/kafka/client-charlie.properties" 2>&1

echo -e "\n3. Чарли пытается читать (FAIL: TopicAuthorizationException):"
docker exec -it kafka-new /bin/bash -c "/opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9093 --topic ${TOPIC} --consumer.config /etc/kafka/client-charlie.properties --from-beginning --max-messages 1 --timeout-ms 3000" 2>&1

echo -e "\n========================================="
