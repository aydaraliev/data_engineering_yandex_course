# Data Warehouse Pipeline: Тегирование гостей

Проект реализует 3-слойный Data Warehouse для системы тегирования гостей ресторанной сети. Данные о заказах обрабатываются в реальном времени через Kafka и сохраняются в PostgreSQL.

## Статус деплоя

**Kubernetes (Yandex Cloud):**
- STG Service: `cr.yandex/crp5jrq3v9u1b9npu54p/stg-service:2026.01.17` ✅ Running
- DDS Service: `cr.yandex/crp5jrq3v9u1b9npu54p/dds-service:2026.01.17` ✅ Running
- CDM Service: `cr.yandex/crp5jrq3v9u1b9npu54p/cdm-service:2026.01.17` ✅ Running

**Проверка:**
```bash
kubectl get pods
kubectl get deployments
```

## Архитектура

### Общая схема

```
+==============================================================================+
|                              YANDEX CLOUD                                    |
|                                                                              |
|    +-------------+       +-------------+       +-------------+               |
|    |    REDIS    |       |    KAFKA    |       | POSTGRESQL  |               |
|    | (port 6380) |       | (port 9091) |       | (port 6432) |               |
|    +------+------+       +------+------+       +------+------+               |
|           |                     |                     |                      |
+===========|=====================|=====================|======================+
            |                     |                     |
            v                     v                     v
+===========|=====================|=====================|======================+
|           |            DOCKER COMPOSE                 |                      |
|           |                     |                     |                      |
|  +--------+---------------------+---------------------+-------------------+  |
|  |                     STG SERVICE (port 5011)                            |  |
|  |                                                                        |  |
|  |   [Flask /health]  [APScheduler 25s]  [Kafka Consumer]  [Kafka Producer]  |
|  |                           |                  |                |        |  |
|  |                           v                  |                |        |  |
|  |                   +----------------+         |                |        |  |
|  |                   | StgMessage     |<--------+                |        |  |
|  |                   | Processor      |------------------------->+        |  |
|  |                   +-------+--------+                                   |  |
|  |                           |                                            |  |
|  |                           v                                            |  |
|  |                   +----------------+         +----------------+        |  |
|  |                   | Redis Client   |-------->| stg.order_     |        |  |
|  |                   | (enrich data)  |         | events (PG)    |        |  |
|  |                   +----------------+         +----------------+        |  |
|  +------------------------------------------------------------------------+  |
|           |                                                                  |
|           |  Kafka: order-service-orders --> stg-service-orders              |
|           v                                                                  |
|  +------------------------------------------------------------------------+  |
|  |                     DDS SERVICE (port 5012)                            |  |
|  |                                                                        |  |
|  |   [Flask /health]  [APScheduler 25s]  [Kafka Consumer]  [Kafka Producer]  |
|  |                           |                  |                |        |  |
|  |                           v                  |                |        |  |
|  |                   +----------------+         |                |        |  |
|  |                   | DdsMessage     |<--------+                |        |  |
|  |                   | Processor      |------------------------->+        |  |
|  |                   +-------+--------+                                   |  |
|  |                           |                                            |  |
|  |                           v                                            |  |
|  |                   +----------------+         +----------------+        |  |
|  |                   | DdsRepository  |-------->| dds.h_*, l_*,  |        |  |
|  |                   | (Data Vault)   |         | s_* (PG)       |        |  |
|  |                   +----------------+         +----------------+        |  |
|  +------------------------------------------------------------------------+  |
|           |                                                                  |
|           |  Kafka: stg-service-orders --> dds-service-orders                |
|           v                                                                  |
|  +------------------------------------------------------------------------+  |
|  |                     CDM SERVICE (port 5013)                            |  |
|  |                                                                        |  |
|  |   [Flask /health]  [APScheduler 25s]  [Kafka Consumer]                 |  |
|  |                           |                  |                         |  |
|  |                           v                  |                         |  |
|  |                   +----------------+         |                         |  |
|  |                   | CdmMessage     |<--------+                         |  |
|  |                   | Processor      |                                   |  |
|  |                   +-------+--------+                                   |  |
|  |                           |                                            |  |
|  |                           v                                            |  |
|  |                   +----------------+         +----------------+        |  |
|  |                   | CdmRepository  |-------->| cdm.user_*_    |        |  |
|  |                   | (data marts)   |         | counters (PG)  |        |  |
|  |                   +----------------+         +----------------+        |  |
|  +------------------------------------------------------------------------+  |
|                                                                              |
+==============================================================================+
```

### Поток данных

```
+--------------+    +--------------+    +--------------+    +--------------+
|   Внешняя    |    | STG Service  |    | DDS Service  |    | CDM Service  |
|   система    |    |              |    |              |    |              |
+------+-------+    +------+-------+    +------+-------+    +------+-------+
       |                   |                   |                   |
       | 1. Заказ          |                   |                   |
       +------------------>|                   |                   |
       |    (Kafka)        |                   |                   |
       |                   | 2. Обогащение     |                   |
       |                   |    из Redis       |                   |
       |                   |                   |                   |
       |                   | 3. Сохранение     |                   |
       |                   |    в PG (stg)     |                   |
       |                   |                   |                   |
       |                   | 4. Отправка       |                   |
       |                   +------------------>|                   |
       |                   |    (Kafka)        |                   |
       |                   |                   | 5. Парсинг        |
       |                   |                   |    Data Vault     |
       |                   |                   |                   |
       |                   |                   | 6. Сохранение     |
       |                   |                   |    в PG (dds)     |
       |                   |                   |                   |
       |                   |                   | 7. Отправка       |
       |                   |                   +------------------>|
       |                   |                   |    (Kafka)        |
       |                   |                   |                   | 8. Агрегация
       |                   |                   |                   |
       |                   |                   |                   | 9. Сохранение
       |                   |                   |                   |    в PG (cdm)
       v                   v                   v                   v
```

### Kafka топики

```
+---------------------+      +---------------------+      +---------------------+
| order-service-orders|----->| stg-service-orders  |----->| dds-service-orders  |
| (источник)          |      | (обогащенные)       |      | (для CDM)           |
+---------------------+      +---------------------+      +---------------------+
         |                            |                            |
         v                            v                            v
    STG читает               DDS читает                   CDM читает
    + пишет в PG             + пишет в PG                 + пишет в PG
    + пишет в Kafka          + пишет в Kafka
```

## Слои данных

### STG (Staging Layer)

Сырые данные заказов из источника. Обогащаются данными о пользователях и ресторанах из Redis.

**Таблицы:**
- `stg.order_events` — сырые JSON-события заказов

**Источник:** Kafka топик `order-service-orders`

**Выход:** Kafka топик `stg-service-orders`

### DDS (Detail Data Store)

Нормализованное хранилище по модели Data Vault 2.0.

**Хабы (бизнес-ключи):**
- `dds.h_user` — Пользователи
- `dds.h_product` — Продукты
- `dds.h_category` — Категории блюд
- `dds.h_restaurant` — Рестораны
- `dds.h_order` — Заказы

**Линки (связи):**
- `dds.l_order_user` — Заказ ↔ Пользователь
- `dds.l_order_product` — Заказ ↔ Продукт
- `dds.l_product_restaurant` — Продукт ↔ Ресторан
- `dds.l_product_category` — Продукт ↔ Категория

**Сателлиты (атрибуты):**
- `dds.s_user_names` — Имя пользователя
- `dds.s_product_names` — Название продукта
- `dds.s_restaurant_names` — Название ресторана
- `dds.s_order_cost` — Стоимость заказа
- `dds.s_order_status` — Статус заказа

**Источник:** Kafka топик `stg-service-orders`

**Выход:** Kafka топик `dds-service-orders`

### CDM (Common Data Marts)

Витрины данных для аналитики тегирования гостей.

**Таблицы:**
- `cdm.user_product_counters` — Счётчик заказов по блюдам для каждого пользователя
- `cdm.user_category_counters` — Счётчик заказов по категориям для каждого пользователя

**Источник:** Kafka топик `dds-service-orders`

## Структура проекта

```
solution/
├── docker-compose.yaml          # Оркестрация 3-х контейнеров
├── .env                         # Переменные окружения
├── README.md                    # Документация
│
├── service_stg/                 # ══════ STG-сервис ══════
│   ├── dockerfile               # Python 3.10 + зависимости
│   ├── requirements.txt         # flask, confluent-kafka, psycopg, redis, pydantic
│   └── src/
│       ├── app.py               # Flask + APScheduler (точка входа)
│       ├── app_config.py        # Конфигурация из env-переменных
│       ├── lib/
│       │   ├── kafka_connect/   # KafkaConsumer, KafkaProducer
│       │   ├── pg/              # PgConnect (PostgreSQL)
│       │   └── redis/           # RedisClient
│       └── stg_loader/
│           ├── stg_message_processor_job.py   # Логика обработки
│           └── repository/
│               └── stg_repository.py          # Запись в БД
│
├── service_dds/                 # ══════ DDS-сервис ══════
│   ├── dockerfile
│   ├── requirements.txt         # flask, confluent-kafka, psycopg, pydantic
│   └── src/
│       ├── app.py               # Flask + APScheduler (точка входа)
│       ├── app_config.py        # Конфигурация из env-переменных
│       ├── lib/
│       │   ├── kafka_connect/   # KafkaConsumer, KafkaProducer
│       │   └── pg/              # PgConnect (PostgreSQL)
│       └── dds_loader/
│           ├── dds_message_processor_job.py   # Логика обработки
│           └── repository/
│               └── dds_repository.py          # Data Vault операции
│
└── service_cdm/                 # ══════ CDM-сервис ══════
    ├── dockerfile
    ├── requirements.txt         # flask, confluent-kafka, psycopg, pydantic
    └── src/
        ├── app.py               # Flask + APScheduler (точка входа)
        ├── app_config.py        # Конфигурация из env-переменных
        ├── lib/
        │   ├── kafka_connect/   # KafkaConsumer
        │   └── pg/              # PgConnect (PostgreSQL)
        └── cdm_loader/
            ├── cdm_message_processor_job.py   # Логика обработки
            └── repository/
                └── cdm_repository.py          # Upsert счётчиков
```

## Как работает каждый сервис

Все 3 сервиса имеют одинаковую архитектуру:

```python
# app.py (точка входа)
from flask import Flask
from apscheduler.schedulers.background import BackgroundScheduler

app = Flask(__name__)

@app.get('/health')
def health():
    return 'healthy'

if __name__ == '__main__':
    # 1. Создаём зависимости
    consumer = config.kafka_consumer()
    producer = config.kafka_producer()  # только STG и DDS
    repository = Repository(config.pg_warehouse_db())

    # 2. Создаём процессор
    processor = MessageProcessor(consumer, producer, repository, logger)

    # 3. Запускаем фоновую задачу каждые 25 секунд
    scheduler = BackgroundScheduler()
    scheduler.add_job(func=processor.run, trigger="interval", seconds=25)
    scheduler.start()

    # 4. Запускаем Flask (для health-check)
    app.run(host='0.0.0.0', port=5000)
```

## Технологии

- **Python 3.10** — язык программирования
- **Flask** — HTTP-сервер для health-check эндпоинтов
- **APScheduler** — планировщик фоновых задач (каждые 25 сек)
- **confluent-kafka** — клиент Kafka с SASL/SSL
- **psycopg** — драйвер PostgreSQL
- **pydantic** — валидация данных
- **Redis** — кэш данных пользователей и ресторанов (только STG)
- **Docker** — контейнеризация

## Защита от сбоев и дублей

Pipeline реализует **at-least-once** семантику доставки с идемпотентной обработкой на всех слоях.

### Kafka offset commit

Каждый сервис использует ручной commit offset после успешной обработки сообщения:

```python
# После успешной обработки сообщения
self._consumer.commit()
```

Конфигурация consumer:
```python
'enable.auto.commit': False,  # Автоматический commit ВЫКЛЮЧЕН
'auto.offset.reset': 'earliest'  # При первом запуске читаем с начала
```

**Гарантия:** При падении сервиса необработанные сообщения будут прочитаны повторно.

### STG слой — идемпотентность

Таблица `stg.order_events` использует уникальный ключ по `object_id`:

```sql
INSERT INTO stg.order_events (...)
ON CONFLICT (object_id) DO NOTHING
```

**Гарантия:** Повторная вставка того же заказа будет проигнорирована.

### DDS слой — идемпотентность (Data Vault)

Все таблицы Data Vault используют детерминированные UUID ключи и `ON CONFLICT DO NOTHING`:

```python
# Генерация ключа из бизнес-идентификатора
h_user_pk = generate_uuid(user_id)  # MD5 хеш -> UUID
```

```sql
-- Хабы
INSERT INTO dds.h_user (h_user_pk, ...) ON CONFLICT (h_user_pk) DO NOTHING
INSERT INTO dds.h_order (h_order_pk, ...) ON CONFLICT (h_order_pk) DO NOTHING

-- Линки
INSERT INTO dds.l_order_user (hk_order_user_pk, ...) ON CONFLICT (hk_order_user_pk) DO NOTHING

-- Сателлиты
INSERT INTO dds.s_user_names (...) ON CONFLICT (h_user_pk, load_dt) DO NOTHING
```

**Гарантия:** Повторная обработка заказа не создаст дубликатов в DDS.

### CDM слой — идемпотентность (счётчики)

CDM использует служебную таблицу для отслеживания обработанных заказов:

```sql
CREATE TABLE cdm.srv_processed_orders (
    order_id INT PRIMARY KEY,
    processed_dttm TIMESTAMP NOT NULL DEFAULT NOW()
)
```

Логика обработки:

```python
# 1. Проверяем, был ли заказ уже обработан
if self._cdm_repository.is_order_processed(order_id):
    self._logger.info(f"Заказ {order_id} уже обработан, пропускаем")
    self._consumer.commit()
    continue

# 2. Обновляем счётчики
self._cdm_repository.user_product_counters_upsert(...)
self._cdm_repository.user_category_counters_upsert(...)

# 3. Помечаем заказ как обработанный
self._cdm_repository.mark_order_processed(order_id)

# 4. Фиксируем offset
self._consumer.commit()
```

**Гарантия:** Повторная обработка заказа не увеличит счётчики повторно.

### Сводная таблица защиты

```
+------------------+------------------------+--------------------------------+
| Слой             | Механизм защиты        | Что защищает                   |
+------------------+------------------------+--------------------------------+
| Kafka            | Manual commit после    | Сообщения не теряются при      |
|                  | успешной обработки     | падении сервиса                |
+------------------+------------------------+--------------------------------+
| STG              | ON CONFLICT DO NOTHING | Дубликаты событий заказов      |
|                  | по object_id           |                                |
+------------------+------------------------+--------------------------------+
| DDS (хабы)       | ON CONFLICT DO NOTHING | Дубликаты бизнес-сущностей     |
|                  | по h_*_pk (UUID)       | (users, products, orders...)   |
+------------------+------------------------+--------------------------------+
| DDS (линки)      | ON CONFLICT DO NOTHING | Дубликаты связей между         |
|                  | по hk_*_pk (UUID)      | сущностями                     |
+------------------+------------------------+--------------------------------+
| DDS (сателлиты)  | ON CONFLICT DO NOTHING | Дубликаты атрибутов            |
|                  | по (h_*_pk, load_dt)   | с одинаковым временем          |
+------------------+------------------------+--------------------------------+
| CDM              | srv_processed_orders   | Двойной подсчёт счётчиков      |
|                  | таблица                | при повторной обработке        |
+------------------+------------------------+--------------------------------+
```

### Сценарий восстановления после сбоя

```
1. Сервис падает во время обработки сообщения #123
2. Offset для #123 НЕ закоммичен (commit после обработки)
3. Сервис перезапускается
4. Consumer читает сообщение #123 повторно
5. Обработка идемпотентна:
   - STG: INSERT ... ON CONFLICT DO NOTHING (пропуск)
   - DDS: INSERT ... ON CONFLICT DO NOTHING (пропуск)
   - CDM: is_order_processed(123) = True (пропуск)
6. Offset коммитится
7. Продолжение с сообщения #124
```

## Запуск

### Предварительные требования

1. Docker и Docker Compose
2. Доступ к Kafka кластеру (Yandex Cloud)
3. Доступ к PostgreSQL (Yandex Cloud)
4. Доступ к Redis (Yandex Cloud)

### Конфигурация

Создайте файл `.env` в корне проекта:

```env
# Kafka settings
KAFKA_HOST=<kafka-host>
KAFKA_PORT=9091
KAFKA_CONSUMER_USERNAME=<username>
KAFKA_CONSUMER_PASSWORD=<password>

# Consumer groups
KAFKA_STG_CONSUMER_GROUP=stg-service-group
KAFKA_DDS_CONSUMER_GROUP=dds-service-group
KAFKA_CDM_CONSUMER_GROUP=cdm-service-group

# Kafka topics
KAFKA_ORDER_SERVICE_TOPIC=order-service-orders
KAFKA_STG_SERVICE_ORDERS_TOPIC=stg-service-orders
KAFKA_DDS_SERVICE_ORDERS_TOPIC=dds-service-orders

# Redis settings (только для STG)
REDIS_HOST=<redis-host>
REDIS_PORT=6380
REDIS_PASSWORD=<password>

# PostgreSQL settings
PG_WAREHOUSE_HOST=<pg-host>
PG_WAREHOUSE_PORT=6432
PG_WAREHOUSE_DBNAME=<dbname>
PG_WAREHOUSE_USER=<username>
PG_WAREHOUSE_PASSWORD=<password>
```

### Сборка и запуск

```bash
# Сборка и запуск всех сервисов
docker-compose up -d --build

# Проверка статуса
docker ps

# Просмотр логов
docker logs stg_service_container
docker logs dds_service_container
docker logs cdm_service_container
```

### Health-check

```bash
curl http://localhost:5011/health  # STG
curl http://localhost:5012/health  # DDS
curl http://localhost:5013/health  # CDM
```

## Формат сообщений

### Входное сообщение (order-service-orders)

```json
{
  "object_id": 9242179,
  "object_type": "order",
  "payload": {
    "id": 9242179,
    "date": "2026-01-08 18:56:09",
    "cost": 3660,
    "payment": 3660,
    "status": "CLOSED",
    "restaurant": { "id": "ef8c42c1..." },
    "user": { "id": "626a81ce..." },
    "products": [...]
  }
}
```

### STG → DDS (stg-service-orders)

```json
{
  "object_id": 9242179,
  "object_type": "order",
  "payload": {
    "id": 9242179,
    "date": "2026-01-08 18:56:09",
    "cost": 3660,
    "payment": 3660,
    "status": "CLOSED",
    "restaurant": {
      "id": "ef8c42c1...",
      "name": "Pizza House"
    },
    "user": {
      "id": "626a81ce...",
      "name": "Иван Петров"
    },
    "products": [
      {
        "id": "47b94729...",
        "name": "Баттер Наан",
        "price": 120,
        "quantity": 8,
        "category": "Хлеб"
      }
    ]
  }
}
```

### DDS → CDM (dds-service-orders)

```json
{
  "object_id": 9242179,
  "object_type": "order",
  "payload": {
    "user_id": "626a81ce...",
    "products": [
      {
        "id": "47b94729...",
        "name": "Баттер Наан",
        "category": "Хлеб"
      }
    ]
  }
}
```

## Генерация UUID ключей

Для детерминированной генерации UUID из бизнес-ключей используется MD5-хеш:

```python
import hashlib
import uuid

def generate_uuid(value: str) -> uuid.UUID:
    return uuid.UUID(hashlib.md5(value.encode()).hexdigest())

# Примеры:
h_user_pk = generate_uuid(user_id)           # UUID пользователя
h_order_pk = generate_uuid(str(order_id))    # UUID заказа
hk_order_user_pk = generate_uuid(f"{order_id}_{user_id}")  # UUID связи
```

## Проверка данных

```sql
-- Проверка STG
SELECT COUNT(*) FROM stg.order_events;

-- Проверка DDS хабов
SELECT COUNT(*) FROM dds.h_order;
SELECT COUNT(*) FROM dds.h_user;
SELECT COUNT(*) FROM dds.h_product;

-- Проверка CDM витрин
SELECT * FROM cdm.user_product_counters ORDER BY order_cnt DESC LIMIT 10;
SELECT * FROM cdm.user_category_counters ORDER BY order_cnt DESC LIMIT 10;
```

## Остановка сервисов (Docker Compose)

```bash
docker-compose down
```

## Kubernetes деплой (Yandex Cloud + Helm)

### Container Registry

Образы сервисов размещены в Yandex Container Registry:

- **STG Service:** `cr.yandex/crp5jrq3v9u1b9npu54p/stg-service:2026.01.17`
- **DDS Service:** `cr.yandex/crp5jrq3v9u1b9npu54p/dds-service:2026.01.17`
- **CDM Service:** `cr.yandex/crp5jrq3v9u1b9npu54p/cdm-service:2026.01.17`

### Сборка и публикация образов

```bash
# 1. Авторизация в Yandex Container Registry
yc container registry configure-docker

# 2. Сборка и публикация STG-сервиса
cd solution/service_stg
docker build -t cr.yandex/crp5jrq3v9u1b9npu54p/stg-service:2026.01.17 .
docker push cr.yandex/crp5jrq3v9u1b9npu54p/stg-service:2026.01.17

# 3. Сборка и публикация DDS-сервиса
cd ../service_dds
docker build -t cr.yandex/crp5jrq3v9u1b9npu54p/dds-service:2026.01.17 .
docker push cr.yandex/crp5jrq3v9u1b9npu54p/dds-service:2026.01.17

# 4. Сборка и публикация CDM-сервиса
cd ../service_cdm
docker build -t cr.yandex/crp5jrq3v9u1b9npu54p/cdm-service:2026.01.17 .
docker push cr.yandex/crp5jrq3v9u1b9npu54p/cdm-service:2026.01.17
```

### Создание секретов в Kubernetes

```bash
# Секреты для STG-сервиса (включает Redis)
kubectl create secret generic stg-service-secrets \
  --from-literal=KAFKA_HOST=<kafka-host> \
  --from-literal=KAFKA_CONSUMER_USERNAME=<username> \
  --from-literal=KAFKA_CONSUMER_PASSWORD=<password> \
  --from-literal=REDIS_HOST=<redis-host> \
  --from-literal=REDIS_PASSWORD=<redis-password> \
  --from-literal=PG_WAREHOUSE_HOST=<pg-host> \
  --from-literal=PG_WAREHOUSE_DBNAME=<dbname> \
  --from-literal=PG_WAREHOUSE_USER=<pg-user> \
  --from-literal=PG_WAREHOUSE_PASSWORD=<pg-password>

# Секреты для DDS-сервиса
kubectl create secret generic dds-service-secrets \
  --from-literal=KAFKA_HOST=<kafka-host> \
  --from-literal=KAFKA_CONSUMER_USERNAME=<username> \
  --from-literal=KAFKA_CONSUMER_PASSWORD=<password> \
  --from-literal=PG_WAREHOUSE_HOST=<pg-host> \
  --from-literal=PG_WAREHOUSE_DBNAME=<dbname> \
  --from-literal=PG_WAREHOUSE_USER=<pg-user> \
  --from-literal=PG_WAREHOUSE_PASSWORD=<pg-password>

# Секреты для CDM-сервиса
kubectl create secret generic cdm-service-secrets \
  --from-literal=KAFKA_HOST=<kafka-host> \
  --from-literal=KAFKA_CONSUMER_USERNAME=<username> \
  --from-literal=KAFKA_CONSUMER_PASSWORD=<password> \
  --from-literal=PG_WAREHOUSE_HOST=<pg-host> \
  --from-literal=PG_WAREHOUSE_DBNAME=<dbname> \
  --from-literal=PG_WAREHOUSE_USER=<pg-user> \
  --from-literal=PG_WAREHOUSE_PASSWORD=<pg-password>
```

### Деплой через Helm

```bash
# Деплой всех сервисов
helm upgrade --install stg-service solution/service_stg/helm/stg-service
helm upgrade --install dds-service solution/service_dds/helm/dds-service
helm upgrade --install cdm-service solution/service_cdm/helm/cdm-service

# Или с явным указанием тега:
helm upgrade --install stg-service solution/service_stg/helm/stg-service --set image.tag=2026.01.17
helm upgrade --install dds-service solution/service_dds/helm/dds-service --set image.tag=2026.01.17
helm upgrade --install cdm-service solution/service_cdm/helm/cdm-service --set image.tag=2026.01.17
```

### Обновление релизов

```bash
helm upgrade stg-service solution/service_stg/helm/stg-service
helm upgrade dds-service solution/service_dds/helm/dds-service
helm upgrade cdm-service solution/service_cdm/helm/cdm-service
```

### Проверка статуса

```bash
# Проверка Helm релизов
helm list

# Проверка подов
kubectl get pods -l app=stg-service
kubectl get pods -l app=dds-service
kubectl get pods -l app=cdm-service

# Проверка логов
kubectl logs -l app=stg-service --tail=50
kubectl logs -l app=dds-service --tail=50
kubectl logs -l app=cdm-service --tail=50

# Проверка deployment
kubectl get deployment stg-service
kubectl get deployment dds-service
kubectl get deployment cdm-service
```

### Удаление релизов

```bash
helm uninstall stg-service
helm uninstall dds-service
helm uninstall cdm-service
```

### Структура Helm чартов

```
solution/
├── service_stg/
│   └── helm/
│       └── stg-service/
│           ├── Chart.yaml           # Метаданные чарта
│           ├── values.yaml          # Значения по умолчанию
│           └── templates/
│               ├── deployment.yaml  # Шаблон Deployment
│               └── service.yaml     # Шаблон Service
├── service_dds/
│   └── helm/
│       └── dds-service/
│           ├── Chart.yaml
│           ├── values.yaml
│           └── templates/
│               ├── deployment.yaml
│               └── service.yaml
└── service_cdm/
    └── helm/
        └── cdm-service/
            ├── Chart.yaml
            ├── values.yaml
            └── templates/
                ├── deployment.yaml
                └── service.yaml
```

### Конфигурация через values.yaml

Основные параметры в `values.yaml`:

- `replicaCount` — количество реплик (по умолчанию: 1)
- `image.repository` — путь к образу в Container Registry
- `image.tag` — тег образа
- `resources.requests/limits` — лимиты CPU и памяти
- `env.*` — переменные окружения (не секретные)
- `secretName` — имя Kubernetes Secret с credentials

**Текущие ресурсные лимиты** (оптимизированы под квоту 1 CPU / 1Gi memory / 3 pods):
```yaml
resources:
  requests:
    memory: "128Mi"
    cpu: "50m"
  limits:
    memory: "256Mi"
    cpu: "200m"
```

## Troubleshooting

### Quota exceeded в Kubernetes

Если при деплое появляется ошибка `exceeded quota`, уменьшите ресурсные лимиты в `values.yaml`:
```bash
helm upgrade <service> <chart-path> --set resources.limits.cpu=100m --set resources.limits.memory=128Mi
```

### Топик не найден (Unknown topic or partition)

Kafka кластер может не поддерживать auto-creation топиков. Создайте топики вручную через Kafka UI или AdminClient.

### Нет новых сообщений

Проверьте offset consumer group:
- Если offset в конце топика — все сообщения уже обработаны
- Для повторной обработки измените `KAFKA_*_CONSUMER_GROUP` на новое значение

### Ошибка подключения к PostgreSQL

Убедитесь что:
1. Сертификат `/crt/YandexInternalRootCA.crt` загружен
2. IP-адрес добавлен в whitelist кластера
3. Порт 6432 (pgbouncer) доступен
