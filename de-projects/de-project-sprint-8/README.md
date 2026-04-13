# Restaurant Subscribe Streaming Service

Проект 8-го спринта: сервис потоковой обработки подписок на рестораны для агрегатора доставки еды.

## Описание

Сервис обрабатывает рекламные кампании ресторанов в реальном времени и отправляет персонализированные push-уведомления подписчикам.

### Архитектура

```
                         ИСТОЧНИКИ ДАННЫХ
+-------------------+                      +-----------------------+
|    Сотрудник      |                      |      PostgreSQL       |
|    ресторана      |                      |subscribers_restaurants|
+--------+----------+                      +-----------+-----------+
         |                                             |
         v                                             |
+-------------------+                                  |
|      Kafka        |                                  |
|    (topic_in)     |                                  |
+--------+----------+                                  |
         |                                             |
         +------------------+  +------------------------+
                            |  |
                            v  v
              +-----------------------------+
              |       SPARK STREAMING       |
              |-----------------------------|
              | 1. Парсинг JSON             |
              | 2. Фильтрация по времени    |
              | 3. JOIN с подписчиками      |
              | 4. Добавление trigger_datetime|
              | 5. Persist DataFrame        |
              | 6. Запись в PostgreSQL      |
              | 7. Сериализация в JSON      |
              | 8. Отправка в Kafka         |
              | 9. Unpersist DataFrame      |
              +-------------+---------------+
                            |
            +---------------+---------------+
            |                               |
            v                               v
+-------------------+             +-------------------+
|    PostgreSQL     |             |       Kafka       |
| subscribers_feedback|           |    (topic_out)    |
+--------+----------+             +---------+---------+
         |                                  |
         v                                  v
+-------------------+             +-------------------+
|     Аналитика     |             | Сервис уведомлений|
|     фидбэка       |             +--------+----------+
+-------------------+                      |
                                           v
                                  +-------------------+
                                  | Push-уведомления  |
                                  |  пользователям    |
                                  +-------------------+
```

## Структура проекта

```
de-project-sprint-8/
├── README.md
├── .gitignore
└── src/
    ├── scripts/
    │   ├── streaming.py      # Основной Spark Streaming скрипт
    │   ├── env.sh            # Конфигурация (не в git)
    │   ├── env.sh.example    # Пример конфигурации
    │   ├── deploy.sh         # Скрипт развёртывания
    │   ├── run_spark.sh      # Скрипт запуска Spark job
    │   ├── kafka_producer.sh # Отправка тестовых сообщений
    │   └── kafka_consumer.sh # Чтение сообщений из Kafka
    └── sql/
        └── ddl.sql           # DDL скрипты для таблиц
```

## Потоки данных

### Входные данные (Kafka topic_in)

JSON сообщения с рекламными кампаниями:

```json
{
  "restaurant_id": "123e4567-e89b-12d3-a456-426614174000",
  "adv_campaign_id": "campaign-001",
  "adv_campaign_content": "Скидка 20% на все блюда!",
  "adv_campaign_owner": "Иванов Иван Иванович",
  "adv_campaign_owner_contact": "ivanov@restaurant.ru",
  "adv_campaign_datetime_start": 1768170000,
  "adv_campaign_datetime_end": 1768270000,
  "datetime_created": 1768170000
}
```

### Выходные данные

#### PostgreSQL (subscribers_feedback)

Данные с полем `feedback` для последующего сбора отзывов:

| Поле | Тип | Описание |
|------|-----|----------|
| restaurant_id | text | ID ресторана |
| adv_campaign_id | text | ID кампании |
| adv_campaign_content | text | Содержание акции |
| adv_campaign_owner | text | Владелец кампании |
| adv_campaign_owner_contact | text | Контакт владельца |
| adv_campaign_datetime_start | int8 | Начало кампании (Unix timestamp) |
| adv_campaign_datetime_end | int8 | Конец кампании (Unix timestamp) |
| datetime_created | int8 | Время создания (Unix timestamp) |
| client_id | text | ID клиента-подписчика |
| trigger_datetime_created | int4 | Время обработки (Unix timestamp) |
| feedback | varchar | Отзыв клиента (изначально NULL) |

#### Kafka (topic_out)

JSON сообщения **без поля feedback** для push-уведомлений:

```json
{
  "restaurant_id": "123e4567-e89b-12d3-a456-426614174000",
  "adv_campaign_id": "campaign-001",
  "adv_campaign_content": "Скидка 20% на все блюда!",
  "adv_campaign_owner": "Иванов Иван Иванович",
  "adv_campaign_owner_contact": "ivanov@restaurant.ru",
  "adv_campaign_datetime_start": 1768170000,
  "adv_campaign_datetime_end": 1768270000,
  "datetime_created": 1768170000,
  "client_id": "223e4567-e89b-12d3-a456-426614174000",
  "trigger_datetime_created": 1768170500
}
```

## Конфигурация

Создайте файл `src/scripts/env.sh` на основе `env.sh.example`:

```bash
# Kafka
export KAFKA_BOOTSTRAP_SERVER="your-kafka-server:9091"
export KAFKA_USERNAME="your-username"
export KAFKA_PASSWORD="your-password"
export KAFKA_TOPIC_IN="your_topic_in"
export KAFKA_TOPIC_OUT="your_topic_out"

# PostgreSQL источник (подписчики)
export PG_SOURCE_HOST="your-pg-source-host"
export PG_SOURCE_PORT="6432"
export PG_SOURCE_DB="de"
export PG_SOURCE_USER="your-user"
export PG_SOURCE_PASSWORD="your-password"

# PostgreSQL назначение (фидбэк)
export PG_DEST_HOST="localhost"
export PG_DEST_PORT="5432"
export PG_DEST_DB="de"
export PG_DEST_USER="your-user"
export PG_DEST_PASSWORD="your-password"

# Удалённый сервер
export REMOTE_HOST="your-server-ip"
export REMOTE_USER="yc-user"
export SSH_KEY="$HOME/.ssh/your_key"
export DOCKER_CONTAINER="your-container-name"
```

## Развёртывание и запуск

### 1. Развёртывание кода на сервер

```bash
cd src/scripts
./deploy.sh
```

### 2. Запуск Spark Streaming

```bash
./run_spark.sh
```

### 3. Тестирование

Отправка тестового сообщения:
```bash
./kafka_producer.sh '{"restaurant_id":"123e4567-e89b-12d3-a456-426614174000",...}'
```

Чтение сообщений из Kafka:
```bash
./kafka_consumer.sh          # входной топик
./kafka_consumer.sh --out    # выходной топик
```

## Технологии

- **Apache Spark 3.3.0** — Structured Streaming
- **Apache Kafka** — очередь сообщений с SASL_SSL (SCRAM-SHA-512)
- **PostgreSQL** — хранение данных подписчиков и фидбэка
- **Python 3.x** — PySpark

## Реализованные шаги

1. ✅ Чтение данных из Kafka с SASL_SSL авторизацией
2. ✅ Парсинг JSON по схеме в DataFrame
3. ✅ Фильтрация по времени актуальности акции
4. ✅ Чтение данных о подписчиках из PostgreSQL
5. ✅ JOIN потоковых и статичных данных по restaurant_id
6. ✅ Запись в PostgreSQL (с полем feedback) через foreachBatch
7. ✅ Отправка в Kafka (без поля feedback) в формате JSON
8. ✅ Персистентность DataFrame (persist/unpersist)

## Соответствие схеме

Маппинг блоков архитектуры на код в `streaming.py`:

| Блок схемы | Реализация | Строки |
|------------|------------|--------|
| Kafka → JSON в DataFrame | `from_json()` | 79-84 |
| Фильтрация по времени | `filter()` | 90-93 |
| Чтение подписчиков | `spark.read.jdbc()` | 95-103 |
| Объединение данных | `join()` | 157-161 |
| trigger_datetime_created | `unix_timestamp()` | 172 |
| Persist DataFrame | `df.persist()` | 118 |
| PostgreSQL (с feedback) | `df_with_feedback.write` | 122-131 |
| JSON → Kafka (без feedback) | `to_json() + write` | 135-145 |
| Unpersist | `df.unpersist()` | 153 |

## Исправления по результатам код-ревью

### 1. SSL-сертификаты для Kafka в Spark

**Проблема:** Kafka SASL_SSL truststore был настроен только для kafkacat, но не для Spark Java клиента.

**Решение:** Добавлена поддержка Java truststore через переменные окружения:
```python
KAFKA_SSL_TRUSTSTORE_LOCATION = os.getenv("KAFKA_SSL_TRUSTSTORE_LOCATION", "")
KAFKA_SSL_TRUSTSTORE_PASSWORD = os.getenv("KAFKA_SSL_TRUSTSTORE_PASSWORD", "")
```
Функция `get_kafka_options()` добавляет truststore в опции Kafka если переменные заданы.

### 2. Дубликаты при перезапуске (checkpointLocation)

**Проблема:** Отсутствие checkpointLocation приводило к повторному чтению всех сообщений после перезапуска.

**Решение:** Добавлен checkpointLocation в персистентную директорию:
```python
CHECKPOINT_LOCATION = os.getenv("CHECKPOINT_LOCATION", "/home/ajdaralijev/spark-checkpoints/restaurant-streaming")

query = result_df.writeStream \
    .option("checkpointLocation", CHECKPOINT_LOCATION) \
    .start()
```

### 3. Несоответствие типов (trigger_datetime_created)

**Проблема:** `unix_timestamp()` возвращает LongType, а PostgreSQL ожидает int4.

**Решение:** Добавлен явный cast к IntegerType:
```python
unix_timestamp(current_timestamp()).cast(IntegerType()).alias("trigger_datetime_created")
```

### 4. Идемпотентность foreachBatch

**Проблема:** При сбое между записью в PostgreSQL и Kafka возникали дубликаты при retry.

**Решение:** Добавлен unique index в DDL для защиты от дубликатов:
```sql
CREATE UNIQUE INDEX idx_feedback_unique
    ON public.subscribers_feedback(restaurant_id, adv_campaign_id, client_id, datetime_created);
```

Двухуровневая защита:
- **checkpointLocation** — предотвращает повторную обработку при нормальных перезапусках
- **Unique index** — отклоняет дубликаты при retry после сбоя

### 5. Производительность (show/count)

**Проблема:** Использование `show()` и `count()` создавало лишнюю нагрузку.

**Решение:**
- Удалены все вызовы `show()` и `count()`
- Проверка пустого батча через `df.rdd.isEmpty()`
- Добавлен `cache()` для таблицы подписчиков
- Использование `persist()`/`unpersist()` в foreachBatch

### Результаты тестирования

| Тест | Результат |
|------|-----------|
| Подключение к Kafka (SASL_SSL) | ✅ |
| Чтение подписчиков из PostgreSQL | ✅ |
| Запись в PostgreSQL (int4) | ✅ |
| Отправка в Kafka (без feedback) | ✅ |
| Checkpoint сохранение | ✅ |
| Отклонение дубликатов | ✅ |
