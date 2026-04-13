import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, to_json, col, current_timestamp, unix_timestamp, lit, struct
from pyspark.sql.types import StructType, StructField, StringType, LongType, IntegerType

# Конфигурация Kafka (из переменных окружения)
KAFKA_BOOTSTRAP_SERVER = os.getenv("KAFKA_BOOTSTRAP_SERVER")
KAFKA_USERNAME = os.getenv("KAFKA_USERNAME")
KAFKA_PASSWORD = os.getenv("KAFKA_PASSWORD")
KAFKA_TOPIC_IN = os.getenv("KAFKA_TOPIC_IN")
KAFKA_TOPIC_OUT = os.getenv("KAFKA_TOPIC_OUT")

# SSL конфигурация для Kafka (Java truststore)
# Если не задано, используется системный truststore JVM
KAFKA_SSL_TRUSTSTORE_LOCATION = os.getenv("KAFKA_SSL_TRUSTSTORE_LOCATION", "")
KAFKA_SSL_TRUSTSTORE_PASSWORD = os.getenv("KAFKA_SSL_TRUSTSTORE_PASSWORD", "")

# Директория для checkpoint (для сохранения offset'ов между перезапусками)
CHECKPOINT_LOCATION = os.getenv("CHECKPOINT_LOCATION", "/home/ajdaralijev/spark-checkpoints/restaurant-streaming")

# Конфигурация PostgreSQL (источник данных о подписчиках)
PG_SOURCE_HOST = os.getenv("PG_SOURCE_HOST")
PG_SOURCE_PORT = os.getenv("PG_SOURCE_PORT", "6432")
PG_SOURCE_DB = os.getenv("PG_SOURCE_DB")
PG_SOURCE_USER = os.getenv("PG_SOURCE_USER")
PG_SOURCE_PASSWORD = os.getenv("PG_SOURCE_PASSWORD")

# Конфигурация PostgreSQL (назначение для записи фидбэка)
PG_DEST_HOST = os.getenv("PG_DEST_HOST")
PG_DEST_PORT = os.getenv("PG_DEST_PORT", "5432")
PG_DEST_DB = os.getenv("PG_DEST_DB")
PG_DEST_USER = os.getenv("PG_DEST_USER")
PG_DEST_PASSWORD = os.getenv("PG_DEST_PASSWORD")

# Проверка обязательных переменных
required_kafka = [KAFKA_BOOTSTRAP_SERVER, KAFKA_USERNAME, KAFKA_PASSWORD, KAFKA_TOPIC_IN, KAFKA_TOPIC_OUT]
required_pg_source = [PG_SOURCE_HOST, PG_SOURCE_DB, PG_SOURCE_USER, PG_SOURCE_PASSWORD]
required_pg_dest = [PG_DEST_HOST, PG_DEST_DB, PG_DEST_USER, PG_DEST_PASSWORD]

if not all(required_kafka):
    raise ValueError("Необходимо установить переменные окружения Kafka: KAFKA_BOOTSTRAP_SERVER, KAFKA_USERNAME, KAFKA_PASSWORD, KAFKA_TOPIC_IN, KAFKA_TOPIC_OUT")
if not all(required_pg_source):
    raise ValueError("Необходимо установить переменные окружения PostgreSQL источника: PG_SOURCE_HOST, PG_SOURCE_DB, PG_SOURCE_USER, PG_SOURCE_PASSWORD")
if not all(required_pg_dest):
    raise ValueError("Необходимо установить переменные окружения PostgreSQL назначения: PG_DEST_HOST, PG_DEST_DB, PG_DEST_USER, PG_DEST_PASSWORD")

# Необходимые библиотеки для интеграции Spark с Kafka и PostgreSQL
spark_jars_packages = ",".join([
    "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0",
    "org.postgresql:postgresql:42.4.0",
])


def get_kafka_options():
    """
    Возвращает словарь опций для подключения к Kafka.
    Включает SSL truststore если указан в переменных окружения.
    """
    options = {
        "kafka.bootstrap.servers": KAFKA_BOOTSTRAP_SERVER,
        "kafka.security.protocol": "SASL_SSL",
        "kafka.sasl.jaas.config": f'org.apache.kafka.common.security.scram.ScramLoginModule required username="{KAFKA_USERNAME}" password="{KAFKA_PASSWORD}";',
        "kafka.sasl.mechanism": "SCRAM-SHA-512",
    }

    # Добавляем truststore если указан (для окружений где CA не в JVM)
    if KAFKA_SSL_TRUSTSTORE_LOCATION and KAFKA_SSL_TRUSTSTORE_PASSWORD:
        options["kafka.ssl.truststore.location"] = KAFKA_SSL_TRUSTSTORE_LOCATION
        options["kafka.ssl.truststore.password"] = KAFKA_SSL_TRUSTSTORE_PASSWORD

    return options


# Создаём Spark сессию
spark = SparkSession.builder \
    .appName("RestaurantSubscribeStreamingService") \
    .config("spark.sql.session.timeZone", "UTC") \
    .config("spark.jars.packages", spark_jars_packages) \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# Получаем опции Kafka
kafka_options = get_kafka_options()

# Читаем из топика Kafka сообщения с акциями от ресторанов
restaurant_read_stream_df = spark.readStream \
    .format("kafka") \
    .options(**kafka_options) \
    .option("subscribe", KAFKA_TOPIC_IN) \
    .option("startingOffsets", "earliest") \
    .option("failOnDataLoss", "false") \
    .load()

# Определяем схему входного сообщения JSON
incoming_message_schema = StructType([
    StructField("restaurant_id", StringType(), True),
    StructField("adv_campaign_id", StringType(), True),
    StructField("adv_campaign_content", StringType(), True),
    StructField("adv_campaign_owner", StringType(), True),
    StructField("adv_campaign_owner_contact", StringType(), True),
    StructField("adv_campaign_datetime_start", LongType(), True),
    StructField("adv_campaign_datetime_end", LongType(), True),
    StructField("datetime_created", LongType(), True),
])

# Десериализуем JSON из поля value и преобразуем в DataFrame с колонками
parsed_df = restaurant_read_stream_df \
    .select(
        from_json(col("value").cast("string"), incoming_message_schema).alias("parsed_value")
    ) \
    .select("parsed_value.*")

# Фильтруем акции по времени: текущее время должно быть между началом и концом кампании
# Получаем текущее время в секундах (Unix timestamp)
current_timestamp_utc = unix_timestamp(current_timestamp())

filtered_df = parsed_df.filter(
    (col("adv_campaign_datetime_start") <= current_timestamp_utc) &
    (col("adv_campaign_datetime_end") > current_timestamp_utc)
)

# Читаем данные о подписчиках из PostgreSQL
# БЕЗ show() и count() - они создают лишнюю нагрузку
subscribers_restaurant_df = spark.read \
    .format("jdbc") \
    .option("url", f"jdbc:postgresql://{PG_SOURCE_HOST}:{PG_SOURCE_PORT}/{PG_SOURCE_DB}") \
    .option("driver", "org.postgresql.Driver") \
    .option("dbtable", "subscribers_restaurants") \
    .option("user", PG_SOURCE_USER) \
    .option("password", PG_SOURCE_PASSWORD) \
    .load()

# Кэшируем таблицу подписчиков - она используется в каждом микробатче
subscribers_restaurant_df.cache()

# Логируем только факт загрузки, без count()
print("=== Загружена таблица подписчиков из PostgreSQL ===")


def foreach_batch_function(df, epoch_id):
    """
    Функция для обработки каждого микробатча стрима.
    Записывает данные в PostgreSQL (с feedback) и Kafka (без feedback).

    checkpointLocation сохраняет offset'ы между перезапусками,
    предотвращая повторную обработку сообщений.
    """
    # Проверяем, есть ли данные в батче (без полного count)
    if df.rdd.isEmpty():
        print(f"=== Batch {epoch_id}: пустой батч, пропускаем ===")
        return

    # Кэшируем DataFrame для повторного использования
    df.persist()

    try:
        # 1. Записываем в PostgreSQL для аналитики (с полем feedback)
        df_with_feedback = df.withColumn("feedback", lit(None).cast(StringType()))

        pg_url = f"jdbc:postgresql://{PG_DEST_HOST}:{PG_DEST_PORT}/{PG_DEST_DB}"

        df_with_feedback.write \
            .format("jdbc") \
            .option("url", pg_url) \
            .option("driver", "org.postgresql.Driver") \
            .option("dbtable", "subscribers_feedback") \
            .option("user", PG_DEST_USER) \
            .option("password", PG_DEST_PASSWORD) \
            .mode("append") \
            .save()

        # 2. Отправляем в Kafka для push-уведомлений (без поля feedback)
        df.select(
            to_json(struct(col("*"))).alias("value")
        ).write \
            .format("kafka") \
            .options(**kafka_options) \
            .option("topic", KAFKA_TOPIC_OUT) \
            .save()

        # Логируем без count() - просто факт успешной записи
        print(f"=== Batch {epoch_id}: данные записаны в PostgreSQL и Kafka ===")

    except Exception as e:
        print(f"=== Batch {epoch_id}: ОШИБКА - {str(e)} ===")
        raise  # Пробрасываем исключение для retry механизма Spark

    finally:
        # Освобождаем кэш
        df.unpersist()


# Джойним данные из Kafka с подписчиками из PostgreSQL по restaurant_id
# Добавляем колонку trigger_datetime_created с текущим временем
result_df = filtered_df.join(
    subscribers_restaurant_df,
    filtered_df.restaurant_id == subscribers_restaurant_df.restaurant_id,
    "inner"
).select(
    # Выбираем колонки без дублирования restaurant_id
    filtered_df.restaurant_id,
    filtered_df.adv_campaign_id,
    filtered_df.adv_campaign_content,
    filtered_df.adv_campaign_owner,
    filtered_df.adv_campaign_owner_contact,
    filtered_df.adv_campaign_datetime_start,
    filtered_df.adv_campaign_datetime_end,
    filtered_df.datetime_created,
    subscribers_restaurant_df.client_id,
    # cast к IntegerType для соответствия int4 в PostgreSQL
    unix_timestamp(current_timestamp()).cast(IntegerType()).alias("trigger_datetime_created")
)

# Запускаем стрим с записью в PostgreSQL через foreachBatch
# ИСПРАВЛЕНО: добавлен checkpointLocation для сохранения offset'ов
query = result_df.writeStream \
    .outputMode("append") \
    .foreachBatch(foreach_batch_function) \
    .option("checkpointLocation", CHECKPOINT_LOCATION) \
    .start()

print(f"=== Стрим запущен, checkpoint: {CHECKPOINT_LOCATION} ===")

query.awaitTermination()