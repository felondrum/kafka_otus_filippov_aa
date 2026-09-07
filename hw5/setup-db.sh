#!/bin/bash
set -e

PG_CONTAINER="postgres-hw5"
PG_USER="root"
PG_PASSWORD="root"
PG_DB="otus"

echo "========================================="
echo "PostgreSQL Setup: create table and data"
echo "========================================="

# Wait for PostgreSQL readiness
echo -e "\n[1] Waiting for PostgreSQL..."
for i in {1..30}; do
  if docker exec "$PG_CONTAINER" pg_isready -U "$PG_USER" -d "$PG_DB" > /dev/null 2>&1; then
    echo "    PostgreSQL is ready (attempt $i)"
    break
  fi
  if [ "$i" -eq 30 ]; then
    echo "    ERROR: PostgreSQL not available after 30 attempts"
    exit 1
  fi
  echo -n "."
  sleep 1
done

# Create table
echo -e "\n[2] Creating table 'users'..."
docker exec -e PGPASSWORD="$PG_PASSWORD" "$PG_CONTAINER" psql -U "$PG_USER" -d "$PG_DB" -c "DROP TABLE IF EXISTS users;"
docker exec -e PGPASSWORD="$PG_PASSWORD" "$PG_CONTAINER" psql -U "$PG_USER" -d "$PG_DB" -c "
CREATE TABLE users (
    id SERIAL PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    email VARCHAR(100),
    age INTEGER,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
"
docker exec -e PGPASSWORD="$PG_PASSWORD" "$PG_CONTAINER" psql -U "$PG_USER" -d "$PG_DB" -c "ALTER TABLE users REPLICA IDENTITY FULL;"
docker exec -e PGPASSWORD="$PG_PASSWORD" "$PG_CONTAINER" psql -U "$PG_USER" -d "$PG_DB" -c "
INSERT INTO users (name, email, age) VALUES
    ('Ivan Ivanov', 'ivan@example.com', 25),
    ('Maria Petrova', 'maria@example.com', 30),
    ('Alexey Sidorov', 'alexey@example.com', 28),
    ('Elena Kozlova', 'elena@example.com', 22),
    ('Dmitry Volkov', 'dmitry@example.com', 35);
"

echo -e "\n========================================="
echo "PostgreSQL setup complete"
echo "========================================="
