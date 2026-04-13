"""
Витрина для рекомендации друзей.

Логика:
- Пользователи подписаны на один канал
- Ранее никогда не переписывались
- Расстояние между ними <= 1 км
- Пары должны быть уникальными (user_left < user_right)
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
from geo_utils import calculate_haversine_distance

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


class FriendRecommendations:
    """Класс для построения витрины рекомендаций друзей."""

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
        self.processed_dttm = F.current_timestamp()
        self.metrics = PerformanceMetrics()

    def load_data(self):
        """Загружает данные из ODS слоя."""
        logger.info(f"Загрузка событий из ODS: {self.ods_path}")
        # ОПТИМИЗАЦИЯ: Читаем уже обогащенные данные из ODS вместо RAW
        events_raw = self.spark.read.parquet(self.ods_path)

        # Применение выборки если указано
        if self.sample_fraction < 1.0:
            logger.info(f"Применение выборки {self.sample_fraction} с seed=42")
            events_sampled = events_raw.sample(fraction=self.sample_fraction, seed=42)
        else:
            events_sampled = events_raw

        # ОПТИМИЗАЦИЯ: Кэшируем т.к. используется в 3 методах (subscriptions, locations, communications)
        self.events_df = events_sampled.cache()

        # Материализация кэша
        events_count = self.events_df.count()
        logger.info(f"События загружены из ODS и закэшированы: {events_count} записей")

    def get_user_subscriptions(self):
        """Получает подписки пользователей на каналы."""
        logger.info("Извлечение подписок пользователей...")

        subscriptions = self.events_df.filter(
            F.col("event_type") == "subscription"
        ).select(
            F.col("event.user").alias("user_id"),
            F.col("event.subscription_channel").alias("channel_id")
        ).distinct()

        # logger.info(f"Найдено уникальных подписок: {subscriptions.count()}")  # Commented for performance

        self.subscriptions_df = subscriptions

    def get_user_last_locations(self):
        """Получает последнюю геопозицию для каждого пользователя из ODS."""
        logger.info("Получение последних позиций пользователей из ODS...")

        # ОПТИМИЗАЦИЯ: Города уже определены в ODS, просто берем последнюю позицию
        messages = self.events_df.filter(
            F.col("event_type") == "message"
        )

        window_last = Window.partitionBy("user_id").orderBy(F.col("event_datetime").desc())

        self.user_locations_df = messages.withColumn(
            "rank",
            F.row_number().over(window_last)
        ).filter(
            F.col("rank") == 1
        ).select(
            "user_id",
            "lat",
            "lon",
            "city",
            "city_id",
            "timezone"
        )

        logger.info("Последние позиции пользователей получены из ODS")

    def map_users_to_cities(self):
        """Пропускается - города уже определены в ODS слое."""
        logger.info("Пропуск определения городов (уже выполнено в ODS)")
        # ОПТИМИЗАЦИЯ: Города уже определены в ODS
        self.users_with_cities = self.user_locations_df
        logger.info(f"Распределение по городам:")
        self.users_with_cities.groupBy("city").count().orderBy(F.desc("count")).show(10)

    def get_user_communications(self):
        """Получает пары пользователей, которые переписывались."""
        logger.info("Извлечение пар пользователей, которые переписывались...")

        messages = self.events_df.filter(
            F.col("event_type") == "message"
        ).select(
            F.col("event.message_from").alias("user_from"),
            F.col("event.message_to").alias("user_to")
        ).filter(
            F.col("user_to").isNotNull()
        )

        # logger.info(f"Найдено сообщений между пользователями: {messages.count()}")  # Commented for performance

        # Создаем уникальные пары (порядок не важен)
        self.communicated_pairs = messages.withColumn(
            "user_left",
            F.when(F.col("user_from") < F.col("user_to"), F.col("user_from"))
             .otherwise(F.col("user_to"))
        ).withColumn(
            "user_right",
            F.when(F.col("user_from") < F.col("user_to"), F.col("user_to"))
             .otherwise(F.col("user_from"))
        ).select(
            "user_left",
            "user_right"
        ).distinct()

        # logger.info(f"Уникальных пар, которые переписывались: {self.communicated_pairs.count()}")  # Commented for performance

    def find_channel_peers(self):
        """Находит пары пользователей, подписанных на один канал."""
        logger.info("Поиск пар пользователей с общими подписками...")

        # Self-join: находим пары пользователей в одном канале
        channel_pairs = self.subscriptions_df.alias("s1").join(
            self.subscriptions_df.alias("s2"),
            on=F.col("s1.channel_id") == F.col("s2.channel_id"),
            how="inner"
        ).filter(
            F.col("s1.user_id") < F.col("s2.user_id")  # Избегаем дублей и self-pairs
        ).select(
            F.col("s1.user_id").alias("user_left"),
            F.col("s2.user_id").alias("user_right"),
            F.col("s1.channel_id").alias("channel_id")
        ).distinct()

        # logger.info(f"Пар с общими подписками: {channel_pairs.count()}")  # Commented for performance

        self.channel_pairs_df = channel_pairs

    def filter_non_communicated(self):
        """Фильтрует пары, которые никогда не переписывались."""
        logger.info("Фильтрация пар, которые никогда не переписывались...")

        # LEFT ANTI JOIN: оставляем только те пары, которых НЕТ в communicated_pairs
        self.non_communicated_pairs = self.channel_pairs_df.join(
            self.communicated_pairs,
            on=["user_left", "user_right"],
            how="left_anti"
        )

        # logger.info(f"Пар, которые не переписывались: {self.non_communicated_pairs.count()}")  # Commented for performance

    def enrich_with_locations(self):
        """Обогащает пары координатами обоих пользователей."""
        logger.info("Обогащение пар координатами пользователей...")

        # Присоединяем координаты для user_left
        pairs_with_left = self.non_communicated_pairs.join(
            self.users_with_cities.select(
                F.col("user_id").alias("user_left"),
                F.col("lat").alias("lat_left"),
                F.col("lon").alias("lon_left"),
                F.col("city_id").alias("zone_id"),
                F.col("city").alias("city_name"),
                F.col("timezone").alias("timezone")
            ),
            on="user_left",
            how="inner"
        )

        # logger.info(f"Пар с координатами user_left: {pairs_with_left.count()}")  # Commented for performance

        # Присоединяем координаты для user_right
        pairs_with_both = pairs_with_left.join(
            self.users_with_cities.select(
                F.col("user_id").alias("user_right"),
                F.col("lat").alias("lat_right"),
                F.col("lon").alias("lon_right"),
                F.col("city_id").alias("city_id_right")
            ),
            on="user_right",
            how="inner"
        )

        # logger.info(f"Пар с координатами обоих: {pairs_with_both.count()}")  # Commented for performance

        # Фильтруем пары из одного города
        self.pairs_with_coords = pairs_with_both.filter(
            F.col("zone_id") == F.col("city_id_right")
        ).drop("city_id_right")

        # logger.info(f"Пар из одного города: {self.pairs_with_coords.count()}")  # Commented for performance

    def calculate_distances(self):
        """Вычисляет расстояние между пользователями."""
        logger.info("Расчет расстояний между пользователями...")

        # Используем Haversine формулу
        distance_km = calculate_haversine_distance(
            "lat_left", "lon_left",
            "lat_right", "lon_right"
        )

        self.pairs_with_distance = self.pairs_with_coords.withColumn(
            "distance_km",
            distance_km
        )

        logger.info("Статистика расстояний:")
        self.pairs_with_distance.select(
            F.min("distance_km").alias("min_km"),
            F.avg("distance_km").alias("avg_km"),
            F.max("distance_km").alias("max_km"),
            F.count("*").alias("total_pairs")
        ).show()

    def filter_by_distance(self, max_distance_km: float = 1.0):
        """Фильтрует пары по расстоянию."""
        logger.info(f"Фильтрация пар с расстоянием <= {max_distance_km} км...")

        self.close_pairs = self.pairs_with_distance.filter(
            F.col("distance_km") <= max_distance_km
        )

        # logger.info(f"Пар в радиусе {max_distance_km} км: {self.close_pairs.count()}")  # Commented for performance

    def calculate_local_time(self):
        """Вычисляет локальное время для каждой пары."""
        logger.info("Расчет локального времени...")

        self.recommendations_with_time = self.close_pairs.withColumn(
            "local_time",
            F.from_utc_timestamp(self.processed_dttm, F.col("timezone"))
        )

        logger.info("Локальное время добавлено")

    def build_recommendations(self):
        """Строит финальную витрину рекомендаций."""
        logger.info("Построение витрины рекомендаций...")

        self.recommendations_df = self.recommendations_with_time.select(
            "user_left",
            "user_right",
            self.processed_dttm.alias("processed_dttm"),
            "zone_id",
            "local_time"
        ).distinct()

        # logger.info(f"Построена витрина: {self.recommendations_df.count()} рекомендаций")  # Commented for performance

    def save_recommendations(self):
        """Сохраняет витрину в HDFS."""
        logger.info(f"Сохранение витрины в: {self.output_path}")

        # ОПТИМИЗАЦИЯ: Coalesce для уменьшения количества мелких файлов
        self.recommendations_df.coalesce(2).write \
            .mode("overwrite") \
            .parquet(self.output_path)

        logger.info("Витрина успешно сохранена")

    def show_sample(self, n=20):
        """Показывает примеры рекомендаций."""
        logger.info(f"\nПримеры рекомендаций (первые {n}):")
        self.recommendations_df.show(n, truncate=False)

        logger.info("\nСтатистика по зонам:")
        self.recommendations_df.groupBy("zone_id").agg(
            F.count("*").alias("recommendations_count")
        ).orderBy(F.desc("recommendations_count")).show(10)

        logger.info("\nОбщая статистика:")
        self.recommendations_df.select(
            F.count("*").alias("total_recommendations"),
            F.countDistinct("user_left").alias("unique_users_left"),
            F.countDistinct("user_right").alias("unique_users_right"),
            F.countDistinct("zone_id").alias("unique_zones")
        ).show(truncate=False)

    def run(self):
        """Выполняет полный процесс построения витрины."""
        logger.info("=" * 70)
        logger.info("НАЧАЛО ПОСТРОЕНИЯ ВИТРИНЫ РЕКОМЕНДАЦИЙ ДРУЗЕЙ")
        if self.sample_fraction < 1.0:
            logger.info(f"РЕЖИМ ВЫБОРКИ: {self.sample_fraction * 100}%")
        logger.info("=" * 70)

        try:
            self.metrics.checkpoint("start")

            self.load_data()
            self.metrics.checkpoint("load_data")

            self.get_user_subscriptions()
            self.metrics.checkpoint("get_user_subscriptions")

            self.get_user_last_locations()
            self.map_users_to_cities()
            self.metrics.checkpoint("map_users_to_cities")

            self.get_user_communications()
            self.metrics.checkpoint("get_user_communications")

            self.find_channel_peers()
            self.metrics.checkpoint("find_channel_peers")

            self.filter_non_communicated()
            self.enrich_with_locations()
            self.metrics.checkpoint("enrich_with_locations")

            self.calculate_distances()
            self.metrics.checkpoint("calculate_distances")

            self.filter_by_distance(max_distance_km=1.0)
            self.calculate_local_time()
            self.build_recommendations()
            self.metrics.checkpoint("build_recommendations")

            self.save_recommendations()
            self.metrics.checkpoint("save_recommendations")

            self.show_sample()

            # Вывод метрик производительности
            logger.info("=" * 70)
            logger.info("МЕТРИКИ ПРОИЗВОДИТЕЛЬНОСТИ:")
            logger.info(f"  Загрузка данных:       {self.metrics.get_duration('start', 'load_data'):.2f}s")
            logger.info(f"  Подписки:              {self.metrics.get_duration('load_data', 'get_user_subscriptions'):.2f}s")
            logger.info(f"  Позиции пользователей: {self.metrics.get_duration('get_user_subscriptions', 'map_users_to_cities'):.2f}s")
            logger.info(f"  История переписки:     {self.metrics.get_duration('map_users_to_cities', 'get_user_communications'):.2f}s")
            logger.info(f"  Поиск пар (каналы):    {self.metrics.get_duration('get_user_communications', 'find_channel_peers'):.2f}s")
            logger.info(f"  Обогащение координатами:{self.metrics.get_duration('find_channel_peers', 'enrich_with_locations'):.2f}s")
            logger.info(f"  Расчет расстояний:     {self.metrics.get_duration('enrich_with_locations', 'calculate_distances'):.2f}s")
            logger.info(f"  Построение рекомендаций:{self.metrics.get_duration('calculate_distances', 'build_recommendations'):.2f}s")
            logger.info(f"  Сохранение:            {self.metrics.get_duration('build_recommendations', 'save_recommendations'):.2f}s")
            logger.info(f"  ОБЩЕЕ ВРЕМЯ:           {self.metrics.get_duration('start', 'save_recommendations'):.2f}s")
            logger.info("=" * 70)
            logger.info("ВИТРИНА РЕКОМЕНДАЦИЙ ДРУЗЕЙ УСПЕШНО ПОСТРОЕНА")
            logger.info("=" * 70)

            return 0

        except Exception as e:
            logger.error(f"Ошибка при построении витрины: {e}", exc_info=True)
            return 1


def main():
    """Основная функция."""
    # Парсинг аргументов командной строки
    parser = argparse.ArgumentParser(description='Построение витрины рекомендаций друзей')
    parser.add_argument('--sample', type=float, default=None,
                       help='Доля выборки (0.0-1.0), например 0.1 для 10%%')
    args = parser.parse_args()

    # Определяем sample_fraction из аргументов или переменной окружения
    sample_fraction = args.sample if args.sample is not None else float(os.getenv('SAMPLE_FRACTION', '1.0'))

    spark = SparkSession.builder \
        .appName("FriendRecommendations") \
        .config("spark.sql.adaptive.enabled", "true") \
        .getOrCreate()

    # ОПТИМИЗАЦИЯ: Читаем из ODS вместо RAW
    ods_path = "/user/ajdaral1ev/project/geo/ods/events_with_cities"
    output_path = "/user/ajdaral1ev/project/geo/mart/friend_recommendations"

    builder = FriendRecommendations(
        spark=spark,
        ods_path=ods_path,
        output_path=output_path,
        sample_fraction=sample_fraction
    )

    result = builder.run()

    spark.stop()
    return result


if __name__ == "__main__":
    sys.exit(main())
