# Создание директории для сертификатов
mkdir -p ssl-certificates
cd ssl-certificates

# 1. Генерация CA сертификата
openssl req -new -x509 -keyout otus-kafka-ca-key -out otus-kafka-ca-cert -days 365 -subj "/CN=Kafka-CA/OU=Otus/O=Kafka/L=Moscow/ST=Moscow/C=RU" -passout pass:otus_ca_password

# 2. Генерация сертификата для брокера
keytool -genkey -keyalg RSA -keystore otus-kafka-server.keystore.jks -keypass password -alias localhost -validity 365 -storetype pkcs12 -storepass password -dname "CN=localhost,OU=Otus,O=Kafka,L=Moscow,ST=Moscow,C=RU"

# 3. Экспорт сертификата брокера
keytool -certreq -keystore otus-kafka-server.keystore.jks -alias localhost -file otus-kafka-cert-file

# 4. Подпись сертификата брокера CA
openssl x509 -req -CA otus-kafka-ca-cert -CAkey otus-kafka-ca-key -in otus-kafka-cert-file -out otus-kafka-cert-signed -days 365 -CAcreateserial -passin pass:otus_ca_password

# 5. Импорт CA в keystore брокера
keytool -importcert -keystore otus-kafka-server.keystore.jks -alias CARoot -file otus-kafka-ca-cert -storepass password -noprompt

# 6. Импорт подписанного сертификата в keystore брокера
keytool -importcert -keystore otus-kafka-server.keystore.jks -alias localhost -file otus-kafka-cert-signed -storepass password -noprompt

# 7. Создание truststore для брокера
keytool -importcert -keystore otus-kafka-server.truststore.jks -alias CARoot -file otus-kafka-ca-cert -storepass password -noprompt

# 8. Создание truststore для клиента
keytool -importcert -keystore otus-kafka-client.truststore.jks -alias CARoot -file otus-kafka-ca-cert -storepass password -noprompt
