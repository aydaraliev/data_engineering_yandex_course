# Проект Data Lake: Гео-рекомендации

## Обзор проекта

Производственное решение Data Lake для гео-рекомендаций пользователей в социальной сети (регион Австралия). Реализует отслеживание местоположения, аналитику событий по городам и рекомендации друзей с использованием Apache Spark, HDFS и Apache Airflow.

## Структура проекта

```
de-project-sprint-7/
├── dags/
│   ├── geo_marts_dag.py          # Оркестрация Airflow
│   ├── validate_dag.py           # Валидация DAG
│   └── DEPLOYMENT.md             # Руководство по развертыванию
│
├── src/scripts/
│   ├── create_schema.py          # Создание структуры HDFS
│   ├── geo_utils.py              # Утилиты: расстояние Хаверсина, определение городов
│   ├── create_ods_layer.py       # Создание ODS слоя
│   ├── user_geo_mart.py          # Витрина геоаналитики пользователей
│   ├── zone_mart.py              # Витрина агрегации событий по городам
│   └── friend_recommendations.py # Витрина рекомендаций друзей
│
├── geo.csv                       # Справочник городов (24 города Австралии)
├── .gitignore                    # Паттерны игнорирования Git
└── README.md                     # Этот файл
```

**Всего**: 6 производственных Python-скриптов + 1 Airflow DAG + утилиты

## Архитектура Data Lake

### Трёхслойная архитектура

```
┌─────────────────────────────────────────────────────────────┐
│                   СЛОЙ RAW (Источник)                        │
│  /user/master/data/geo/events (Parquet, партиции по дате)   │
│  /user/ajdaral1ev/project/geo/raw/geo_csv/geo.csv           │
└────────────────┬────────────────────────────────────────────┘
                 │
                 │ ЧТЕНИЕ
                 ▼
┌─────────────────────────────────────────────────────────────┐
│                   СЛОЙ ODS (Операционный)                    │
│  /user/ajdaral1ev/project/geo/ods/events_with_cities        │
│                                                               │
│  ✓ События обогащены информацией о городах                  │
│  ✓ Однократное определение городов (Haversine)              │
│  ✓ Партиционирование по дате                                │
│  ✓ Переиспользование во всех витринах                       │
└────────────────┬────────────────────────────────────────────┘
                 │
                 │ ЧТЕНИЕ (все витрины)
                 │
        ┌────────┼────────┐
        │        │        │
        ▼        ▼        ▼
   ┌─────────┐ ┌─────────┐ ┌─────────┐
   │User Geo │ │Zone Mart│ │ Friend  │
   │  Mart   │ │         │ │Recommend│
   └────┬────┘ └────┬────┘ └────┬────┘
        │           │           │
        │ ЗАПИСЬ    │ ЗАПИСЬ    │ ЗАПИСЬ
        ▼           ▼           ▼
┌────────────────────────────────────────┐
│         СЛОЙ MART (Витрины)            │
│  /user/ajdaral1ev/project/geo/mart/    │
│  ├── user_geo_report                   │
│  ├── zone_mart (партиции по месяцам)   │
│  └── friend_recommendations            │
└────────────────────────────────────────┘
```

### Преимущества ODS слоя

**Проблема без ODS**:
- Каждая витрина независимо загружает полный набор событий из RAW
- Операция `find_nearest_city()` (cross-join N × 24) выполняется 3 раза
- Извлечение местоположений пользователей дублируется
- Определение городов (Haversine) вычисляется избыточно

**Решение с ODS слоем**:
1. **Производительность**: 3x ускорение (дорогие операции выполняются 1 раз)
2. **Переиспользование**: Обогащенные данные используются всеми витринами
3. **Качество данных**: Централизованная валидация и проверки
4. **Быстрая итерация**: Витрины пересобираются быстро из ODS без чтения RAW
5. **Отладка**: Проще изолировать проблемы на этапах конвейера

## Реализованные оптимизации

### 1. Broadcast Join для таблицы городов ✅

**Файл**: `geo_utils.py:114`

```python
# ОПТИМИЗАЦИЯ: Используем broadcast для маленькой таблицы городов (24 записи)
events_with_cities = events_df.crossJoin(F.broadcast(cities_renamed))
```

**Эффект**: Ускорение cross-join операции за счет репликации малой таблицы на всех исполнителях.

### 2. ODS слой для переиспользования ✅

**Файл**: `create_ods_layer.py`

```python
# Создание ODS слоя с обогащенными данными
events_with_cities.write \
    .mode("overwrite") \
    .partitionBy("date") \
    .parquet(output_path)
```

**Эффект**:
- Определение городов выполняется 1 раз вместо 3
- Витрины читают готовые обогащенные данные
- Общее ускорение конвейера в 2-3 раза

### 3. Адаптивное выполнение запросов ✅

**Все скрипты**:

```python
spark.sql.adaptive.enabled = true
```

**Эффект**: Динамическая оптимизация плана выполнения на основе статистики времени выполнения.

### 4. Настройка партиций shuffle ✅

```python
spark.sql.shuffle.partitions = 20  # Для датасета 1.5GB
```

**Эффект**: Баланс между параллелизмом и накладными расходами.

### 5. Партиционирование по дате ✅

**ODS слой**:
```python
.partitionBy("date")
```

**Zone Mart**:
```python
.partitionBy("month")
```

**Эффект**: Эффективная обрезка партиций для запросов по времени.

## Витрины данных

### 1. Витрина геоаналитики пользователей

**Скрипт**: `user_geo_mart.py`
**Выход**: `/user/ajdaral1ev/project/geo/mart/user_geo_report`

**Схема**:
| Поле | Тип | Описание |
|------|-----|----------|
| user_id | long | Идентификатор пользователя |
| act_city | string | Наиболее активный город (последние 30 дней) |
| home_city | string | Домашний город (27+ дней непрерывного пребывания) |
| travel_count | long | Количество смен городов |
| travel_array | array<string> | Упорядоченный список посещенных городов |
| local_time | timestamp | Время последнего события в местной временной зоне |

**Ключевые алгоритмы**:
- Формула Хаверсина для определения городов
- Оконная функция для последней активности
- Определение домашнего города на основе периодов (27+ дней подряд)
- Lag window функция для отслеживания путешествий

### 2. Витрина по зонам (городам)

**Скрипт**: `zone_mart.py`
**Выход**: `/user/ajdaral1ev/project/geo/mart/zone_mart` (партиции по месяцам)

**Схема**:
| Поле | Тип | Описание |
|------|-----|----------|
| month | date | Измерение месяца |
| week | date | Измерение недели (начало с понедельника) |
| zone_id | long | Идентификатор города |
| week_message | long | Количество сообщений за неделю |
| week_reaction | long | Количество реакций за неделю |
| week_subscription | long | Количество подписок за неделю |
| week_user | long | Количество регистраций за неделю |
| month_message | long | Количество сообщений за месяц |
| month_reaction | long | Количество реакций за месяц |
| month_subscription | long | Количество подписок за месяц |
| month_user | long | Количество регистраций за месяц |

**Ключевые функции**:
- Присвоение местоположения событиям без координат (использует последнюю известную позицию пользователя)
- Определение регистраций (первое сообщение = регистрация пользователя)
- Двойная агрегация (еженедельная + ежемесячная) за один проход
- Партиционирование по месяцам для эффективных запросов по диапазону времени

### 3. Витрина рекомендаций друзей

**Скрипт**: `friend_recommendations.py`
**Выход**: `/user/ajdaral1ev/project/geo/mart/friend_recommendations`

**Схема**:
| Поле | Тип | Описание |
|------|-----|----------|
| user_left | long | Первый пользователь (всегда < user_right) |
| user_right | long | Второй пользователь |
| processed_dttm | timestamp | Временная метка вычисления (UTC) |
| zone_id | long | Город, где оба пользователя находятся |
| local_time | timestamp | Обработанное время в местной временной зоне |

**Критерии рекомендации** (должны быть выполнены все):
- ✓ Оба пользователя подписаны на один канал
- ✓ Никогда ранее не общались (LEFT ANTI JOIN)
- ✓ Расстояние ≤ 1 км (формула Хаверсина)
- ✓ Находятся в одном городе
- ✓ Только уникальные пары (user_left < user_right)

## Оркестрация Airflow

### DAG: geo_marts_update

**Расписание**: Ежедневно в 00:00 UTC
**Выполнение**: Последовательное

```
start (лог)
  ↓
create_ods_layer (Spark) ← НОВЫЙ ШАГ
  ↓
update_user_geo_report (Spark)
  ↓
update_zone_mart (Spark)
  ↓
update_friend_recommendations (Spark)
  ↓
end (лог)
```

**Почему последовательно?**
- ODS должен быть создан перед витринами
- Каждая задача: 4 исполнителя × 4GB = 16GB памяти
- Параллельно: 48GB требуется (3 × 16GB)
- Последовательно: 16GB одновременно (предотвращает перегрузку кластера)

**Политика повторов**:
- 2 повтора на задачу
- 5-минутная задержка между повторами
- Предотвращает временные сбои

## Развертывание

### Предварительные требования

```bash
# Требуемое ПО
- Python 3.7+
- Apache Spark 3.3+
- Apache Airflow 2.0+
- Доступ к HDFS

# Пакеты Python
pip install pyspark apache-airflow-providers-apache-spark
```

### 1. Развертывание скриптов на HDFS/сервере

```bash
# Загрузка справочника городов
hdfs dfs -put geo.csv /user/ajdaral1ev/project/geo/raw/geo_csv/

# Копирование скриптов на сервер
scp -i ~/.ssh/ssh_private_key \
    src/scripts/*.py \
    yc-user@158.160.218.207:/lessons/scripts/
```

### 2. Создание структуры HDFS

```bash
# Запуск создания схемы
spark-submit \
  --master yarn \
  --deploy-mode client \
  /lessons/scripts/create_schema.py
```

### 3. Развертывание Airflow DAG

```bash
# Валидация синтаксиса DAG
cd dags/
python3 validate_dag.py

# Копирование в папку DAG Airflow
export AIRFLOW_DAGS_DIR="/opt/airflow/dags"
cp geo_marts_dag.py $AIRFLOW_DAGS_DIR/

# Проверка появления DAG
airflow dags list | grep geo_marts_update
```

### 4. Настройка подключения Airflow

```bash
airflow connections add yarn_spark \
    --conn-type spark \
    --conn-host yarn://master-host:8032 \
    --conn-extra '{"queue": "default"}'
```

### 5. Активация и запуск

```bash
# Снятие паузы с DAG
airflow dags unpause geo_marts_update

# Ручной запуск (опционально)
airflow dags trigger geo_marts_update

# Или ожидание запланированного запуска в 00:00 UTC
```

## Мониторинг

### Проверка статуса DAG

```bash
# Список статуса DAG
airflow dags list | grep geo_marts

# Проверка последнего запуска
airflow dags state geo_marts_update $(date +%Y-%m-%d)

# Просмотр логов задач
airflow tasks logs geo_marts_update create_ods_layer $(date +%Y-%m-%d)
airflow tasks logs geo_marts_update update_user_geo_report $(date +%Y-%m-%d)
```

### Проверка вывода HDFS

```bash
# Проверка существования директорий витрин
hdfs dfs -ls /user/ajdaral1ev/project/geo/ods/
hdfs dfs -ls /user/ajdaral1ev/project/geo/mart/

# Подсчет записей
hdfs dfs -count /user/ajdaral1ev/project/geo/ods/events_with_cities
hdfs dfs -count /user/ajdaral1ev/project/geo/mart/user_geo_report

# Просмотр примера данных
spark-shell
val ods = spark.read.parquet("/user/ajdaral1ev/project/geo/ods/events_with_cities")
ods.show(10, truncate=false)
ods.printSchema()

val ugr = spark.read.parquet("/user/ajdaral1ev/project/geo/mart/user_geo_report")
ugr.show(10, truncate=false)
```

### Проверки качества данных

```scala
// Валидация витрины геоаналитики пользователей
val ugr = spark.read.parquet("/user/ajdaral1ev/project/geo/mart/user_geo_report")
ugr.filter($"act_city".isNull).count()  // Должно быть 0
ugr.filter($"travel_count" < 1).count() // Должно быть 0

// Валидация витрины зон
val zm = spark.read.parquet("/user/ajdaral1ev/project/geo/mart/zone_mart")
zm.filter($"week_message" > $"month_message").count() // Должно быть 0

// Валидация рекомендаций друзей
val fr = spark.read.parquet("/user/ajdaral1ev/project/geo/mart/friend_recommendations")
fr.filter($"user_left" >= $"user_right").count() // Должно быть 0

// Проверка ODS слоя
val ods = spark.read.parquet("/user/ajdaral1ev/project/geo/ods/events_with_cities")
ods.filter($"city".isNull).count() // Должно быть 0
ods.groupBy("city").count().show(24)
```

## Конфигурация

### Пути HDFS

```
Исходные данные:
  /user/master/data/geo/events (Parquet, партиции по дате)
  /user/ajdaral1ev/project/geo/raw/geo_csv/geo.csv

Слой ODS:
  /user/ajdaral1ev/project/geo/ods/events_with_cities

Витрины:
  /user/ajdaral1ev/project/geo/mart/user_geo_report
  /user/ajdaral1ev/project/geo/mart/zone_mart
  /user/ajdaral1ev/project/geo/mart/friend_recommendations
```

### Конфигурация Spark

```python
# Распределение ресурсов
driver_memory = "4g"
executor_memory = "4g"
executor_cores = 2
num_executors = 4

# Настройка производительности
spark.sql.adaptive.enabled = true
spark.sql.shuffle.partitions = 20  # Настроить в зависимости от объема данных
```

### Для больших датасетов (10GB+)

```python
driver_memory = "8g"
executor_memory = "8g"
num_executors = 8
spark.sql.shuffle.partitions = 200
```

## Устранение неполадок

### DAG не появляется в Airflow

```bash
airflow dags list-import-errors
airflow config get-value core dags_folder
systemctl restart airflow-scheduler
```

### Сбой задачи Spark с OOM

```bash
# Увеличение памяти в dags/geo_marts_dag.py
driver_memory = "8g"
executor_memory = "8g"

# Или уменьшение обрабатываемых данных
spark.sql.shuffle.partitions = 10
```

### Медленная производительность определения городов

```bash
# Проверка малого размера таблицы городов
hdfs dfs -cat /user/ajdaral1ev/project/geo/raw/geo_csv/geo.csv | wc -l
# Должно быть 24 города

# Убедитесь, что broadcast hint применен (уже реализовано)
# geo_utils.py:114 - F.broadcast(cities_renamed)
```

### Ошибка прав доступа HDFS (Permission denied)

**Проблема**: `org.apache.hadoop.security.AccessControlException: Permission denied: user=ajdara1iev`

**Причина**: Пользователь Airflow (`ajdara1iev`) не может писать в директории HDFS, владельцем которых является `root:hadoop`.

**Решение**:

```bash
# 1. Изменение владельца всех директорий проекта
docker exec student-sp7-2-ajdara1iev-0-3691838488 \
  bash -c 'HADOOP_USER_NAME=hdfs hdfs dfs -chown -R ajdara1iev:hadoop /user/ajdaral1ev/project/geo'

# 2. Проверка прав доступа
docker exec student-sp7-2-ajdara1iev-0-3691838488 \
  hdfs dfs -ls /user/ajdaral1ev/project/geo

# Должно быть: drwxrwxr-x ajdara1iev hadoop
```

**Автоматическое исправление**: Скрипт `./setup_hdfs.sh` теперь автоматически устанавливает правильные права владения и доступа (775) для всех директорий.

## Технические детали

### Формула расстояния Хаверсина

```python
distance_km = 2 * R * arcsin(sqrt(
    sin²((lat2-lat1)/2) +
    cos(lat1) * cos(lat2) * sin²((lon2-lon1)/2)
))
```
Где R = 6371 км (радиус Земли)

### Алгоритм определения городов

1. Cross-join событий с 24 городами Австралии
2. Вычисление расстояния Хаверсина для каждой пары событие-город
3. Оконная функция: `row_number() OVER (PARTITION BY event_id ORDER BY distance)`
4. Фильтр `rank = 1` (ближайший город)

### Определение путешествий

```python
# Определение смен городов с использованием lag window
Window.partitionBy("user_id").orderBy("event_datetime")
  .withColumn("prev_city", lag("city", 1))
  .filter(col("city") != col("prev_city"))
```

## Развертывание и тестирование

### Автоматизация развертывания

Проект включает 5 shell-скриптов для автоматизации развертывания и мониторинга:

```bash
./deploy_scripts.sh      # 1. Развертывание Python скриптов на кластер
./setup_hdfs.sh          # 2. Создание структуры HDFS (идемпотентно)
./first_run.sh           # 3. Тестовый запуск с 10% выборкой
./monitor_pipeline.sh    # 4. Мониторинг и проверка качества данных
./deploy_dag.sh          # 5. Развертывание Airflow DAG
```

### Порядок развертывания

#### Шаг 1: Развертывание скриптов

```bash
./deploy_scripts.sh
```

**Что делает:**
- Проверяет SSH подключение к кластеру (158.160.159.232)
- Копирует 5 Python скриптов на сервер
- Размещает скрипты в Docker контейнере `/lessons/scripts/`
- Проверяет успешность развертывания

**Развертываемые скрипты:**
- `create_ods_layer.py` - Создание ODS слоя
- `geo_utils.py` - Утилиты геолокации
- `user_geo_mart.py` - Витрина пользователей
- `zone_mart.py` - Витрина по зонам
- `friend_recommendations.py` - Рекомендации друзей

#### Шаг 2: Настройка HDFS

```bash
./setup_hdfs.sh
```

**Что делает (идемпотентно):**
- Создает структуру директорий HDFS (RAW, ODS, MART)
- Загружает справочник городов `geo.csv`
- Проверяет существование перед созданием
- Безопасно для повторного запуска

**Созданная структура:**
```
/user/ajdaral1ev/project/geo/
├── raw/
│   └── geo_csv/geo.csv
├── ods/
│   └── events_with_cities/
└── mart/
    ├── user_geo_report/
    ├── zone_mart/
    └── friend_recommendations/
```

#### Шаг 3: Тестовый запуск

```bash
./first_run.sh
```

**Что делает:**
- Запускает весь pipeline с 10% выборкой данных
- Последовательно выполняет все 4 этапа:
  1. ODS Layer (create_ods_layer.py --sample 0.1)
  2. User Geo Mart (user_geo_mart.py --sample 0.1)
  3. Zone Mart (zone_mart.py --sample 0.1)
  4. Friend Recommendations (friend_recommendations.py --sample 0.1)
- Измеряет время выполнения каждого этапа
- Сохраняет лог в `first_run_YYYYMMDD_HHMMSS.log`

**Ожидаемое время:**
- 10% выборка: ~15-20 минут
- 100% данных: ~2-3 часа

**Изменение размера выборки:**
```bash
SAMPLE_FRACTION=0.05 ./first_run.sh  # 5% выборка
SAMPLE_FRACTION=1.0 ./first_run.sh   # Все данные
```

#### Шаг 4: Мониторинг

```bash
./monitor_pipeline.sh
```

**Что делает:**
- Проверяет статус Airflow DAG
- Анализирует структуру HDFS (размеры, файлы, партиции)
- Выполняет data quality checks:
  - Проверка NULL городов в ODS
  - Статистика по всем 3 витринам
  - Валидация корректности данных (user_left < user_right)
- Генерирует отчет `pipeline_status_YYYYMMDD_HHMMSS.txt`

**Пример вывода:**
```
✓ DAG найден в Airflow
  Статус: active

ODS слой (events_with_cities):
  ✓ Директория существует
  Размер: 450M
  Партиций (по дате): 28

Проверка качества данных:
  Всего событий: 1,234,567
  NULL городов: 0 (0.00%)
  Уникальных городов: 24
```

#### Шаг 5: Развертывание DAG

```bash
./deploy_dag.sh
```

**Что делает:**
- Валидирует Python синтаксис DAG локально
- Копирует DAG в Airflow: `/opt/airflow/dags/`
- Проверяет импорт в Airflow
- Выводит инструкции по активации

**После развертывания:**

1. **Активация DAG:**
```bash
# Через Airflow UI: включите toggle для geo_marts_update
# Или через CLI:
airflow dags unpause geo_marts_update
```

2. **Тестовый режим (10% выборка):**
```bash
airflow variables set geo_marts_sample_fraction 0.1
```

3. **Запуск вручную:**
```bash
airflow dags trigger geo_marts_update
```

4. **Возврат к 100% данных:**
```bash
airflow variables set geo_marts_sample_fraction 1.0
# Или удалить переменную:
airflow variables delete geo_marts_sample_fraction
```

### Режим выборки (Sampling)

Все скрипты поддерживают режим выборки для быстрого тестирования:

**Запуск вручную:**
```bash
spark-submit /lessons/scripts/create_ods_layer.py --sample 0.1
spark-submit /lessons/scripts/user_geo_mart.py --sample 0.1
```

**Через переменную окружения:**
```bash
export SAMPLE_FRACTION=0.1
spark-submit /lessons/scripts/create_ods_layer.py
```

**Через Airflow Variables:**
```bash
# Устанавливает 10% выборку для всех задач DAG
airflow variables set geo_marts_sample_fraction 0.1
```

**Детерминированность:**
- Используется `seed=42` для воспроизводимости
- Одинаковые результаты при повторных запусках

### Метрики производительности

Все скрипты логируют детальные метрики:

```
============================================
МЕТРИКИ ПРОИЗВОДИТЕЛЬНОСТИ:
  Загрузка данных:       12.45s
  Подготовка событий:    34.23s
  Обогащение городами:   156.78s
  Сохранение ODS:        45.12s
  ОБЩЕЕ ВРЕМЯ:           248.58s
============================================
```

**Ожидаемые улучшения от оптимизаций:**

| Компонент | До оптимизации | После оптимизации | Улучшение |
|-----------|----------------|-------------------|-----------|
| ODS Layer | 100% | ~70% | 30% быстрее |
| User Geo Mart | 100% | ~60% | 40% быстрее |
| Zone Mart | 100% | ~70% | 30% быстрее |
| Friend Recs | 100% | ~60% | 40% быстрее |

### Проверка корректности

**После первого запуска:**

```bash
./monitor_pipeline.sh
```

**Валидация данных:**

- [ ] ODS слой создан с events_with_cities
- [ ] Все 3 витрины существуют в HDFS
- [ ] Нет NULL городов в ODS (0%)
- [ ] User geo mart: act_city заполнен для всех пользователей
- [ ] Zone mart: корректное партиционирование по month
- [ ] Friend recommendations: user_left < user_right для всех пар
- [ ] Метрики производительности в логах
- [ ] Время выполнения ~10% от полного запуска (при sample=0.1)

## Вклад

### Стиль кода

- Python: PEP 8
- Docstrings: Google style
- Type hints: Рекомендуется для нового кода

### Git Workflow

```bash
git checkout -b feature/my-feature
# Внесение изменений
git commit -m "feat: описание функции"
git push origin feature/my-feature
```

## Лицензия

Внутренний проект для Data Engineering Sprint 7

## Контакты

- **Проект**: Data Lake Гео-рекомендации
- **Владелец**: ajdaral1ev
- **Инфраструктура**: Yandex Cloud (158.160.218.207)
- **Год**: 2024-2025

---

**Статус проекта**: ✅ Готово к продакшену с ODS слоем и оптимизациями

**Последнее обновление**: 2025-12-23
