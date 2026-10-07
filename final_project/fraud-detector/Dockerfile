FROM eclipse-temurin:21-jre-alpine
WORKDIR /app

RUN apk add --no-cache libstdc++

RUN addgroup -S appgroup && adduser -S appuser -G appgroup
COPY build/libs/*.jar app.jar
COPY entrypoint.sh /app/entrypoint.sh
RUN chmod +x /app/entrypoint.sh && mkdir -p /tmp/kafka-streams && chown appuser:appgroup /tmp/kafka-streams

USER appuser

EXPOSE 8082

HEALTHCHECK --interval=30s --timeout=10s --start-period=60s --retries=3 \
  CMD wget --no-verbose --tries=1 --spider http://localhost:8082/actuator/health || exit 1

ENTRYPOINT ["/app/entrypoint.sh"]
