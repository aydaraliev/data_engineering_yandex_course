"""
Скрипт для создания витрины данных пользователей с геоаналитикой.

Витрина включает:
- user_id: идентификатор пользователя
- act_city: актуальный город (последнее сообщение)
- home_city: домашний город (город, где пользователь был 27+ дней)
- travel_count: количество посещенных городов
- travel_array: список городов в порядке посещения
- local_time: местное время последнего события
"""

import sys
import logging
import time
import argparse
import os
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class PerformanceMetrics:
    """Трекинг метрик производительности."""
    def __init__(self):
        self.checkpoints = {}

    def checkpoint(self, name: str):
        self.checkpoints[name] = time.time()
        logger.info(f"[CHECKPOINT] {name} at {datetime.now()}")

    def get_duration(self, start_name: str, end_name: str) -> float:
        if start_name in self.checkpoints and end_name in self.checkpoints:
            return self.checkpoints[end_name] - self.checkpoints[start_name]
        return 0.0


class UserGeoMart:
    """Класс для построения витрины геоданных пользователей."""

    def __init__(
        self,
        spark: SparkSession,
        ods_path: str,
        output_path: str,
        sample_fraction: float = 1.0
    ):
        """
        Инициализация.

        Args:
            spark: SparkSession
            ods_path: Путь к ODS слою (events_with_cities)
            output_path: Путь для сохранения витрины
            sample_fraction: Доля выборки (0.0-1.0), по умолчанию 1.0 (все данные)
        """
        self.spark = spark
        self.ods_path = ods_path
        self.output_path = output_path
        self.sample_fraction = sample_fraction
        self.metrics = PerformanceMetrics()

    def load_data(self):
        """Загружает данные из ODS слоя."""
        logger.info(f"Загрузка событий из ODS: {self.ods_path}")
        # ОПТИМИЗАЦИЯ: Читаем уже обогащенные данные из ODS вместо RAW
        events_raw = self.spark.read.parquet(self.ods_path)

        # Применение выборки если указано
        if self.sample_fraction < 1.0:
            logger.info(f"Применение выборки {self.sample_fraction} с seed=42")
            self.events_df = events_raw.sample(fraction=self.sample_fraction, seed=42)
        else:
            self.events_df = events_raw

        logger.info("События загружены из ODS (уже обогащены городами)")

    def filter_messages_with_coords(self):
        """Фильтрует только сообщения (уже с городами из ODS)."""
        logger.info("Фильтрация сообщений...")

        self.messages_df = self.events_df.filter(
            F.col("event_type") == "message"
        ).select(
            "user_id",
            "event_datetime",
            "city",
            "city_id",
            "timezone",
            "lat",
            "lon"
        )

        logger.info("Сообщения отфильтрованы")

    def enrich_with_cities(self):
        """Данные уже обогащены в ODS слое."""
        logger.info("Пропуск обогащения городами (уже выполнено в ODS)")
        # ОПТИМИЗАЦИЯ: Этот шаг больше не нужен, данные уже обогащены в ODS
        # ОПТИМИЗАЦИЯ: Кэшируем т.к. используется в 3 методах (act_city, home_city, travel_stats)
        self.enriched_df = self.messages_df.cache()

        # Материализация кэша
        msg_count = self.enriched_df.count()
        logger.info(f"Данные закэшированы: {msg_count} сообщений")

    def calculate_act_city(self):
        """
        Определяет актуальный город (act_city) для каждого пользователя.

        act_city - город, из которого было отправлено последнее сообщение.
        """
        logger.info("Расчет актуального города (act_city)...")

        # Окно для определения последнего события пользователя
        window_last = Window.partitionBy("user_id").orderBy(F.col("event_datetime").desc())

        self.act_city_df = self.enriched_df.withColumn(
            "rank",
            F.row_number().over(window_last)
        ).filter(
            F.col("rank") == 1
        ).select(
            "user_id",
            F.col("city").alias("act_city"),
            F.col("timezone").alias("act_timezone"),
            F.col("event_datetime").alias("last_event_time")
        )

        # logger.info(f"Актуальный город определен для {self.act_city_df.count()} пользователей")  # Commented for performance

    def calculate_home_city(self):
        """
        Определяет домашний город (home_city) для каждого пользователя.

        home_city - последний город, где пользователь был дольше 27 дней.

        Подход: Ищем непрерывные последовательности дней в одном городе.
        Город считается домашним, если:
        - Пользователь отправлял сообщения из этого города
        - Период активности в городе составляет 27+ дней (от первого до последнего сообщения)
        """
        logger.info("Расчет домашнего города (home_city)...")

        # Добавляем дату события (без времени)
        messages_with_date = self.enriched_df.withColumn(
            "event_date",
            F.to_date(F.col("event_datetime"))
        )

        # Определяем смену города для выявления непрерывных последовательностей
        window_city_change = Window.partitionBy("user_id").orderBy("event_datetime")

        city_sequences = messages_with_date.withColumn(
            "prev_city",
            F.lag("city").over(window_city_change)
        ).withColumn(
            "city_changed",
            F.when(F.col("city") != F.col("prev_city"), 1).otherwise(0)
        ).withColumn(
            "city_sequence_id",
            F.sum("city_changed").over(window_city_change)
        )

        # Для каждой последовательности считаем период пребывания
        city_periods = city_sequences.groupBy(
            "user_id",
            "city",
            "city_sequence_id"
        ).agg(
            F.min("event_date").alias("period_start"),
            F.max("event_date").alias("period_end"),
            F.max("event_datetime").alias("last_event_in_period")
        ).withColumn(
            "days_in_city",
            F.datediff(F.col("period_end"), F.col("period_start")) + 1
        )

        # Фильтруем периоды >= 27 дней
        home_cities = city_periods.filter(
            F.col("days_in_city") >= 27
        )

        # Берем последний период (по времени последнего события)
        window_last_home = Window.partitionBy("user_id").orderBy(F.col("last_event_in_period").desc())

        self.home_city_df = home_cities.withColumn(
            "rank",
            F.row_number().over(window_last_home)
        ).filter(
            F.col("rank") == 1
        ).select(
            "user_id",
            F.col("city").alias("home_city")
        )

        # logger.info(f"Домашний город определен для {self.home_city_df.count()} пользователей")  # Commented for performance

    def calculate_travel_stats(self):
        """
        Рассчитывает статистику путешествий.

        - travel_count: количество посещенных городов (с повторами)
        - travel_array: список городов в порядке посещения
        """
        logger.info("Расчет статистики путешествий...")

        # Сортируем события по времени
        window_travel = Window.partitionBy("user_id").orderBy("event_datetime")

        # Определяем смену города
        travel_events = self.enriched_df.withColumn(
            "prev_city",
            F.lag("city").over(window_travel)
        ).withColumn(
            "is_new_city",
            F.when(
                (F.col("prev_city").isNull()) | (F.col("city") != F.col("prev_city")),
                1
            ).otherwise(0)
        ).filter(
            F.col("is_new_city") == 1
        )

        # Агрегируем путешествия
        self.travel_df = travel_events.groupBy("user_id").agg(
            F.count("city").alias("travel_count"),
            F.collect_list("city").alias("travel_array")
        )

        # logger.info(f"Статистика путешествий рассчитана для {self.travel_df.count()} пользователей")  # Commented for performance

    def calculate_local_time(self):
        """
        Рассчитывает местное время последнего события.

        local_time - время последнего события с учетом timezone геопозиции.
        """
        logger.info("Расчет местного времени...")

        # Используем данные из act_city_df, где уже есть последнее событие и timezone
        self.local_time_df = self.act_city_df.withColumn(
            "local_time",
            F.from_utc_timestamp(F.col("last_event_time"), F.col("act_timezone"))
        ).select(
            "user_id",
            "local_time"
        )

        logger.info("Местное время рассчитано")

    def build_mart(self):
        """Собирает финальную витрину."""
        logger.info("Сборка финальной витрины...")

        # Объединяем все компоненты
        mart = self.act_city_df.select("user_id", "act_city")

        # Добавляем home_city
        mart = mart.join(
            self.home_city_df,
            on="user_id",
            how="left"
        )

        # Добавляем travel_count и travel_array
        mart = mart.join(
            self.travel_df,
            on="user_id",
            how="left"
        )

        # Добавляем local_time
        mart = mart.join(
            self.local_time_df,
            on="user_id",
            how="left"
        )

        # Финальная витрина
        self.mart_df = mart.select(
            "user_id",
            "act_city",
            "home_city",
            "travel_count",
            "travel_array",
            "local_time"
        )

        # logger.info(f"Витрина построена: {self.mart_df.count()} пользователей")  # Commented for performance

    def save_mart(self):
        """Сохраняет витрину в HDFS."""
        logger.info(f"Сохранение витрины в: {self.output_path}")

        # ОПТИМИЗАЦИЯ: Coalesce для уменьшения количества мелких файлов
        self.mart_df.coalesce(2).write \
            .mode("overwrite") \
            .parquet(self.output_path)

        logger.info("Витрина успешно сохранена")

    def show_sample(self, n=10):
        """Показывает примеры записей из витрины."""
        logger.info(f"Примеры записей витрины (первые {n}):")
        self.mart_df.show(n, truncate=False)

    def run(self):
        """Выполняет полный процесс построения витрины."""
        logger.info("=" * 70)
        logger.info("НАЧАЛО ПОСТРОЕНИЯ ВИТРИНЫ USER_GEO_REPORT")
        if self.sample_fraction < 1.0:
            logger.info(f"РЕЖИМ ВЫБОРКИ: {self.sample_fraction * 100}%")
        logger.info("=" * 70)

        try:
            self.metrics.checkpoint("start")

            self.load_data()
            self.metrics.checkpoint("load_data")

            self.filter_messages_with_coords()
            self.enrich_with_cities()
            self.metrics.checkpoint("enrich_with_cities")

            self.calculate_act_city()
            self.metrics.checkpoint("calculate_act_city")

            self.calculate_home_city()
            self.metrics.checkpoint("calculate_home_city")

            self.calculate_travel_stats()
            self.metrics.checkpoint("calculate_travel_stats")

            self.calculate_local_time()
            self.build_mart()
            self.metrics.checkpoint("build_mart")

            self.save_mart()
            self.metrics.checkpoint("save_mart")

            self.show_sample()

            # Вывод метрик производительности
            logger.info("=" * 70)
            logger.info("МЕТРИКИ ПРОИЗВОДИТЕЛЬНОСТИ:")
            logger.info(f"  Загрузка данных:       {self.metrics.get_duration('start', 'load_data'):.2f}s")
            logger.info(f"  Обогащение городами:   {self.metrics.get_duration('load_data', 'enrich_with_cities'):.2f}s")
            logger.info(f"  Расчет act_city:       {self.metrics.get_duration('enrich_with_cities', 'calculate_act_city'):.2f}s")
            logger.info(f"  Расчет home_city:      {self.metrics.get_duration('calculate_act_city', 'calculate_home_city'):.2f}s")
            logger.info(f"  Расчет путешествий:    {self.metrics.get_duration('calculate_home_city', 'calculate_travel_stats'):.2f}s")
            logger.info(f"  Сборка витрины:        {self.metrics.get_duration('calculate_travel_stats', 'build_mart'):.2f}s")
            logger.info(f"  Сохранение:            {self.metrics.get_duration('build_mart', 'save_mart'):.2f}s")
            logger.info(f"  ОБЩЕЕ ВРЕМЯ:           {self.metrics.get_duration('start', 'save_mart'):.2f}s")
            logger.info("=" * 70)
            logger.info("ВИТРИНА USER_GEO_REPORT УСПЕШНО ПОСТРОЕНА")
            logger.info("=" * 70)

            return 0

        except Exception as e:
            logger.error(f"Ошибка при построении витрины: {e}", exc_info=True)
            return 1


def main():
    """Основная функция."""
    # Парсинг аргументов командной строки
    parser = argparse.ArgumentParser(description='Построение витрины геоданных пользователей')
    parser.add_argument('--sample', type=float, default=None,
                       help='Доля выборки (0.0-1.0), например 0.1 для 10%%')
    args = parser.parse_args()

    # Определяем sample_fraction из аргументов или переменной окружения
    sample_fraction = args.sample if args.sample is not None else float(os.getenv('SAMPLE_FRACTION', '1.0'))

    # Создаем Spark сессию
    spark = SparkSession.builder \
        .appName("UserGeoMart") \
        .config("spark.sql.adaptive.enabled", "true") \
        .getOrCreate()

    # ОПТИМИЗАЦИЯ: Читаем из ODS вместо RAW
    ods_path = "/user/ajdaral1ev/project/geo/ods/events_with_cities"
    output_path = "/user/ajdaral1ev/project/geo/mart/user_geo_report"

    # Создаем и запускаем процесс
    mart_builder = UserGeoMart(
        spark=spark,
        ods_path=ods_path,
        output_path=output_path,
        sample_fraction=sample_fraction
    )

    result = mart_builder.run()

    spark.stop()
    return result


if __name__ == "__main__":
    sys.exit(main())
