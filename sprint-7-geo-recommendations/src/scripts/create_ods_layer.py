"""
Скрипт для создания слоя ODS (Operational Data Store).

Создает очищенный и обогащенный слой данных для повторного использования:
- events_with_cities: события с определенными городами (используется всеми витринами)

Преимущества ODS слоя:
- Однократное выполнение дорогой операции find_nearest_city
- Переиспользование обогащенных данных во всех витринах
- Ускорение разработки и отладки витрин
- Централизованная валидация качества данных
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
from geo_utils import find_nearest_city

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


class ODSLayer:
    """Класс для построения слоя ODS."""

    def __init__(
        self,
        spark: SparkSession,
        events_path: str,
        cities_path: str,
        output_path: str,
        sample_fraction: float = 1.0
    ):
        """
        Инициализация.

        Args:
            spark: SparkSession
            events_path: Путь к данным событий (RAW)
            cities_path: Путь к справочнику городов
            output_path: Путь для сохранения ODS
            sample_fraction: Доля выборки (0.0-1.0), по умолчанию 1.0 (все данные)
        """
        self.spark = spark
        self.events_path = events_path
        self.cities_path = cities_path
        self.output_path = output_path
        self.sample_fraction = sample_fraction
        self.metrics = PerformanceMetrics()

    def load_data(self):
        """Загружает данные из RAW слоя."""
        logger.info(f"Загрузка событий из RAW: {self.events_path}")
        events_raw = self.spark.read.parquet(self.events_path)

        # Применение выборки если указано
        if self.sample_fraction < 1.0:
            logger.info(f"Применение выборки {self.sample_fraction} с seed=42")
            self.events_df = events_raw.sample(fraction=self.sample_fraction, seed=42)
        else:
            self.events_df = events_raw

        logger.info(f"Загрузка городов из: {self.cities_path}")
        self.cities_df = self.spark.read.csv(
            self.cities_path,
            header=True,
            inferSchema=True,
            sep=";"
        )

        logger.info("Данные загружены из RAW слоя")

    def prepare_events(self):
        """
        Подготавливает события для обогащения.

        Извлекает координаты из сообщений и назначает последние
        известные координаты для событий без геопозиции.
        """
        logger.info("Подготовка событий для обогащения...")

        # Извлекаем сообщения с координатами
        messages_with_coords = self.events_df.filter(
            (F.col("event_type") == "message") &
            (F.col("lat").isNotNull()) &
            (F.col("lon").isNotNull())
        ).select(
            F.col("event.message_from").alias("user_id"),
            F.col("event.datetime").alias("datetime_str"),
            "lat",
            "lon"
        ).withColumn(
            "event_datetime",
            F.coalesce(
                F.to_timestamp(F.col("datetime_str"), "yyyy-MM-dd HH:mm:ss"),
                F.to_timestamp(F.col("datetime_str")),
                F.col("datetime_str").cast("timestamp")
            )
        )

        # Получаем последнее местоположение для каждого пользователя
        window_last = Window.partitionBy("user_id").orderBy(F.col("event_datetime").desc())

        user_locations = messages_with_coords.withColumn(
            "rank",
            F.row_number().over(window_last)
        ).filter(
            F.col("rank") == 1
        ).select(
            "user_id",
            F.col("lat").alias("last_lat"),
            F.col("lon").alias("last_lon")
        )

        # Подготавливаем все события
        events_prepared = self.events_df.select(
            "event_type",
            "event",
            F.col("event.message_from").alias("user_id"),
            F.col("event.datetime").alias("datetime_str"),
            "lat",
            "lon",
            "date"
        ).withColumn(
            "event_datetime",
            F.coalesce(
                F.to_timestamp(F.col("datetime_str"), "yyyy-MM-dd HH:mm:ss"),
                F.to_timestamp(F.col("datetime_str")),
                F.col("datetime_str").cast("timestamp")
            )
        )

        # Присоединяем последние координаты пользователей
        # ОПТИМИЗАЦИЯ: broadcast для маленькой таблицы user_locations
        events_with_locations = events_prepared.join(
            F.broadcast(user_locations),
            on="user_id",
            how="left"
        )

        # Используем координаты события, если есть, иначе - последние координаты пользователя
        self.events_enriched = events_with_locations.withColumn(
            "final_lat",
            F.coalesce(F.col("lat"), F.col("last_lat"))
        ).withColumn(
            "final_lon",
            F.coalesce(F.col("lon"), F.col("last_lon"))
        ).filter(
            F.col("final_lat").isNotNull() & F.col("final_lon").isNotNull()
        ).cache()  # ОПТИМИЗАЦИЯ: Кэшируем для повторного использования

        # Материализация кэша
        events_count = self.events_enriched.count()
        logger.info(f"События подготовлены для обогащения и закэшированы: {events_count} записей")

    def enrich_with_cities(self):
        """
        Обогащает события информацией о городах.

        Это самая дорогая операция (cross-join N × 24).
        Выполняется один раз в ODS слое вместо 3 раз в каждой витрине.
        """
        logger.info("Обогащение событий городами (ОДНА операция для всех витрин)...")

        # Подготавливаем данные для find_nearest_city
        events_for_mapping = self.events_enriched.select(
            "event_type",
            "event",
            "user_id",
            "event_datetime",
            "date",
            F.col("final_lat").alias("lat"),
            F.col("final_lon").alias("lon")
        )

        # Определяем ближайший город
        # ОПТИМИЗАЦИЯ: broadcast применяется внутри find_nearest_city
        self.events_with_cities = find_nearest_city(
            events_for_mapping,
            self.cities_df,
            event_lat_col="lat",
            event_lon_col="lon",
            city_lat_col="lat",
            city_lon_col="lng",
            city_name_col="city"
        )

        logger.info("События обогащены информацией о городах")

    def save_ods(self):
        """
        Сохраняет ODS слой в HDFS.

        Партиционирование по date для эффективного инкрементального обновления.
        """
        logger.info(f"Сохранение ODS слоя в: {self.output_path}")

        # ОПТИМИЗАЦИЯ: Партиционирование по date для инкрементальных обновлений
        # ОПТИМИЗАЦИЯ: Coalesce для оптимального количества файлов на партицию
        self.events_with_cities.coalesce(4).write \
            .mode("overwrite") \
            .partitionBy("date") \
            .parquet(self.output_path)

        logger.info("ODS слой успешно сохранен")

    def show_statistics(self):
        """Показывает статистику ODS слоя."""
        logger.info("\n=== Статистика ODS слоя ===")

        # Общая статистика
        logger.info("Распределение событий по типам:")
        self.events_with_cities.groupBy("event_type").count().orderBy(F.desc("count")).show()

        logger.info("Распределение событий по городам:")
        self.events_with_cities.groupBy("city").count().orderBy(F.desc("count")).show(10)

        logger.info("Распределение по датам:")
        self.events_with_cities.groupBy("date").count().orderBy("date").show(10)

    def run(self):
        """Выполняет полный процесс построения ODS слоя."""
        logger.info("=" * 70)
        logger.info("НАЧАЛО ПОСТРОЕНИЯ ODS СЛОЯ")
        if self.sample_fraction < 1.0:
            logger.info(f"РЕЖИМ ВЫБОРКИ: {self.sample_fraction * 100}%")
        logger.info("=" * 70)

        try:
            self.metrics.checkpoint("start")

            self.load_data()
            self.metrics.checkpoint("load_data")

            self.prepare_events()
            self.metrics.checkpoint("prepare_events")

            self.enrich_with_cities()
            self.metrics.checkpoint("enrich_with_cities")

            self.save_ods()
            self.metrics.checkpoint("save_ods")

            self.show_statistics()
            self.metrics.checkpoint("show_statistics")

            # Вывод метрик производительности
            logger.info("=" * 70)
            logger.info("МЕТРИКИ ПРОИЗВОДИТЕЛЬНОСТИ:")
            logger.info(f"  Загрузка данных:       {self.metrics.get_duration('start', 'load_data'):.2f}s")
            logger.info(f"  Подготовка событий:    {self.metrics.get_duration('load_data', 'prepare_events'):.2f}s")
            logger.info(f"  Обогащение городами:   {self.metrics.get_duration('prepare_events', 'enrich_with_cities'):.2f}s")
            logger.info(f"  Сохранение ODS:        {self.metrics.get_duration('enrich_with_cities', 'save_ods'):.2f}s")
            logger.info(f"  ОБЩЕЕ ВРЕМЯ:           {self.metrics.get_duration('start', 'show_statistics'):.2f}s")
            logger.info("=" * 70)
            logger.info("ODS СЛОЙ УСПЕШНО ПОСТРОЕН")
            logger.info("Все витрины теперь могут читать из ODS вместо RAW")
            logger.info("=" * 70)

            return 0

        except Exception as e:
            logger.error(f"Ошибка при построении ODS: {e}", exc_info=True)
            return 1


def main():
    """Основная функция."""
    # Парсинг аргументов командной строки
    parser = argparse.ArgumentParser(description='Создание ODS слоя с событиями и городами')
    parser.add_argument('--sample', type=float, default=None,
                       help='Доля выборки (0.0-1.0), например 0.1 для 10%%')
    args = parser.parse_args()

    # Определяем sample_fraction из аргументов или переменной окружения
    sample_fraction = args.sample if args.sample is not None else float(os.getenv('SAMPLE_FRACTION', '1.0'))

    spark = SparkSession.builder \
        .appName("CreateODSLayer") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.shuffle.partitions", "20") \
        .getOrCreate()

    # Параметры
    events_path = "/user/master/data/geo/events"
    cities_path = "/user/ajdaral1ev/project/geo/raw/geo_csv/geo.csv"
    output_path = "/user/ajdaral1ev/project/geo/ods/events_with_cities"

    # Создаем ODS слой
    ods_builder = ODSLayer(
        spark=spark,
        events_path=events_path,
        cities_path=cities_path,
        output_path=output_path,
        sample_fraction=sample_fraction
    )

    result = ods_builder.run()

    spark.stop()
    return result


if __name__ == "__main__":
    sys.exit(main())
