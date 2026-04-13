"""
Скрипт для создания витрины в разрезе зон (городов).

Витрина содержит агрегированную статистику событий по городам:
- Количество сообщений, реакций, подписок, регистраций
- Агрегация по неделям и месяцам
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


class ZoneMart:
    """Класс для построения витрины в разрезе зон (городов)."""

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

    def get_user_last_locations(self):
        """
        Пропускается - города уже определены в ODS слое.
        """
        logger.info("Пропуск определения позиций (уже выполнено в ODS)")
        # ОПТИМИЗАЦИЯ: Этот шаг больше не нужен

    def enrich_events_with_locations(self):
        """
        Пропускается - события уже обогащены в ODS слое.
        """
        logger.info("Пропуск обогащения координатами (уже выполнено в ODS)")
        # ОПТИМИЗАЦИЯ: Данные уже обогащены в ODS
        self.events_enriched = self.events_df

    def map_events_to_cities(self):
        """
        Пропускается - города уже определены в ODS слое.
        """
        logger.info("Пропуск определения городов (уже выполнено в ODS)")
        # ОПТИМИЗАЦИЯ: Города уже определены в ODS
        # ОПТИМИЗАЦИЯ: Кэшируем т.к. используется в нескольких методах
        self.events_with_cities = self.events_df.cache()

        # Материализация кэша
        events_count = self.events_with_cities.count()
        logger.info(f"Данные закэшированы: {events_count} событий")

    def add_time_dimensions(self):
        """Добавляет временные измерения (месяц, неделя)."""
        logger.info("Добавление временных измерений...")

        self.events_with_time = self.events_with_cities.withColumn(
            "month",
            F.trunc(F.col("event_datetime"), "month")
        ).withColumn(
            "week",
            F.date_trunc("week", F.col("event_datetime"))  # Неделя начинается с понедельника
        )

        logger.info("Временные измерения добавлены")

    def calculate_registrations(self):
        """
        Определяет регистрации пользователей.

        Регистрация = первое сообщение пользователя.
        Используем город первого сообщения.
        """
        logger.info("Расчет регистраций...")

        # Находим первое сообщение каждого пользователя
        messages_only = self.events_with_time.filter(F.col("event_type") == "message")

        window_first = Window.partitionBy("user_id").orderBy("event_datetime")

        registrations = messages_only.withColumn(
            "rank",
            F.row_number().over(window_first)
        ).filter(
            F.col("rank") == 1
        ).withColumn(
            "event_type",
            F.lit("user")  # Помечаем как регистрацию
        ).select(
            "event_type",
            "user_id",
            "event_datetime",
            "city",
            "city_id",
            "month",
            "week"
        )

        logger.info(f"Найдено регистраций: {registrations.count()}")

        # Объединяем с остальными событиями
        other_events = self.events_with_time.filter(F.col("event_type") != "message").select(
            "event_type",
            "user_id",
            "event_datetime",
            "city",
            "city_id",
            "month",
            "week"
        )

        messages = messages_only.select(
            "event_type",
            "user_id",
            "event_datetime",
            "city",
            "city_id",
            "month",
            "week"
        )

        self.events_final = messages.union(other_events).union(registrations)

        logger.info(f"Всего событий для агрегации: {self.events_final.count()}")

    def build_zone_mart(self):
        """Строит финальную витрину в разрезе зон."""
        logger.info("Построение витрины в разрезе зон...")

        # Агрегация по неделям
        weekly_agg = self.events_final.groupBy("week", "city_id", "city").agg(
            F.sum(F.when(F.col("event_type") == "message", 1).otherwise(0)).alias("week_message"),
            F.sum(F.when(F.col("event_type") == "reaction", 1).otherwise(0)).alias("week_reaction"),
            F.sum(F.when(F.col("event_type") == "subscription", 1).otherwise(0)).alias("week_subscription"),
            F.sum(F.when(F.col("event_type") == "user", 1).otherwise(0)).alias("week_user")
        )

        # Агрегация по месяцам
        monthly_agg = self.events_final.groupBy("month", "city_id", "city").agg(
            F.sum(F.when(F.col("event_type") == "message", 1).otherwise(0)).alias("month_message"),
            F.sum(F.when(F.col("event_type") == "reaction", 1).otherwise(0)).alias("month_reaction"),
            F.sum(F.when(F.col("event_type") == "subscription", 1).otherwise(0)).alias("month_subscription"),
            F.sum(F.when(F.col("event_type") == "user", 1).otherwise(0)).alias("month_user")
        )

        # Объединяем недельные и месячные агрегации
        # Для каждой недели находим соответствующий месяц
        self.zone_mart_df = weekly_agg.withColumn(
            "month",
            F.trunc(F.col("week"), "month")
        ).join(
            monthly_agg,
            on=["month", "city_id", "city"],
            how="left"
        ).select(
            "month",
            "week",
            F.col("city_id").alias("zone_id"),
            "week_message",
            "week_reaction",
            "week_subscription",
            "week_user",
            "month_message",
            "month_reaction",
            "month_subscription",
            "month_user"
        ).orderBy("month", "week", "zone_id")

        logger.info(f"Витрина построена: {self.zone_mart_df.count()} записей")

    def save_mart(self):
        """Сохраняет витрину в HDFS."""
        logger.info(f"Сохранение витрины в: {self.output_path}")

        # ОПТИМИЗАЦИЯ: Coalesce для оптимального количества файлов на партицию
        self.zone_mart_df.coalesce(4).write \
            .mode("overwrite") \
            .partitionBy("month") \
            .parquet(self.output_path)

        logger.info("Витрина успешно сохранена")

    def show_sample(self, n=20):
        """Показывает примеры записей из витрины."""
        logger.info(f"\nПримеры записей витрины (первые {n}):")
        self.zone_mart_df.show(n, truncate=False)

        logger.info("\nСтатистика витрины:")
        self.zone_mart_df.select(
            F.count("*").alias("total_records"),
            F.countDistinct("zone_id").alias("unique_zones"),
            F.countDistinct("week").alias("unique_weeks"),
            F.countDistinct("month").alias("unique_months"),
            F.sum("week_message").alias("total_week_messages"),
            F.sum("month_message").alias("total_month_messages")
        ).show()

    def run(self):
        """Выполняет полный процесс построения витрины."""
        logger.info("=" * 70)
        logger.info("НАЧАЛО ПОСТРОЕНИЯ ВИТРИНЫ В РАЗРЕЗЕ ЗОН")
        if self.sample_fraction < 1.0:
            logger.info(f"РЕЖИМ ВЫБОРКИ: {self.sample_fraction * 100}%")
        logger.info("=" * 70)

        try:
            self.metrics.checkpoint("start")

            self.load_data()
            self.metrics.checkpoint("load_data")

            self.get_user_last_locations()
            self.enrich_events_with_locations()
            self.map_events_to_cities()
            self.metrics.checkpoint("map_events_to_cities")

            self.add_time_dimensions()
            self.metrics.checkpoint("add_time_dimensions")

            self.calculate_registrations()
            self.metrics.checkpoint("calculate_registrations")

            self.build_zone_mart()
            self.metrics.checkpoint("build_zone_mart")

            self.save_mart()
            self.metrics.checkpoint("save_mart")

            self.show_sample()

            # Вывод метрик производительности
            logger.info("=" * 70)
            logger.info("МЕТРИКИ ПРОИЗВОДИТЕЛЬНОСТИ:")
            logger.info(f"  Загрузка данных:       {self.metrics.get_duration('start', 'load_data'):.2f}s")
            logger.info(f"  Определение городов:   {self.metrics.get_duration('load_data', 'map_events_to_cities'):.2f}s")
            logger.info(f"  Временные измерения:   {self.metrics.get_duration('map_events_to_cities', 'add_time_dimensions'):.2f}s")
            logger.info(f"  Расчет регистраций:    {self.metrics.get_duration('add_time_dimensions', 'calculate_registrations'):.2f}s")
            logger.info(f"  Построение витрины:    {self.metrics.get_duration('calculate_registrations', 'build_zone_mart'):.2f}s")
            logger.info(f"  Сохранение:            {self.metrics.get_duration('build_zone_mart', 'save_mart'):.2f}s")
            logger.info(f"  ОБЩЕЕ ВРЕМЯ:           {self.metrics.get_duration('start', 'save_mart'):.2f}s")
            logger.info("=" * 70)
            logger.info("ВИТРИНА В РАЗРЕЗЕ ЗОН УСПЕШНО ПОСТРОЕНА")
            logger.info("=" * 70)

            return 0

        except Exception as e:
            logger.error(f"Ошибка при построении витрины: {e}", exc_info=True)
            return 1


def main():
    """Основная функция."""
    # Парсинг аргументов командной строки
    parser = argparse.ArgumentParser(description='Построение витрины в разрезе зон (городов)')
    parser.add_argument('--sample', type=float, default=None,
                       help='Доля выборки (0.0-1.0), например 0.1 для 10%%')
    args = parser.parse_args()

    # Определяем sample_fraction из аргументов или переменной окружения
    sample_fraction = args.sample if args.sample is not None else float(os.getenv('SAMPLE_FRACTION', '1.0'))

    # Создаем Spark сессию
    spark = SparkSession.builder \
        .appName("ZoneMart") \
        .config("spark.sql.adaptive.enabled", "true") \
        .getOrCreate()

    # ОПТИМИЗАЦИЯ: Читаем из ODS вместо RAW
    ods_path = "/user/ajdaral1ev/project/geo/ods/events_with_cities"
    output_path = "/user/ajdaral1ev/project/geo/mart/zone_mart"

    # Создаем и запускаем процесс
    mart_builder = ZoneMart(
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
