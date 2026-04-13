"""
Утилиты для работы с географическими данными.
Содержит функции для расчета расстояний и определения городов по координатам.
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from typing import Tuple


def calculate_haversine_distance(
    lat1_col: str,
    lon1_col: str,
    lat2_col: str,
    lon2_col: str,
    radius_km: float = 6371.0
) -> F.Column:
    """
    Вычисляет расстояние между двумя точками на сфере по формуле гаверсинуса.

    Формула:
    d = 2r * arcsin(sqrt(sin²((φ2-φ1)/2) + cos(φ1) * cos(φ2) * sin²((λ2-λ1)/2)))

    где:
    - φ1, φ2 — широты точек в радианах
    - λ1, λ2 — долготы точек в радианах
    - r — радиус сферы (Земли), примерно 6371 км

    Args:
        lat1_col: Название колонки с широтой первой точки (в градусах)
        lon1_col: Название колонки с долготой первой точки (в градусах)
        lat2_col: Название колонки с широтой второй точки (в градусах)
        lon2_col: Название колонки с долготой второй точки (в градусах)
        radius_km: Радиус Земли в километрах

    Returns:
        Column: Расстояние между точками в километрах
    """
    # Конвертация градусов в радианы
    lat1_rad = F.radians(F.col(lat1_col))
    lon1_rad = F.radians(F.col(lon1_col))
    lat2_rad = F.radians(F.col(lat2_col))
    lon2_rad = F.radians(F.col(lon2_col))

    # Разница координат
    delta_lat = lat2_rad - lat1_rad
    delta_lon = lon2_rad - lon1_rad

    # Формула гаверсинуса
    a = (
        F.pow(F.sin(delta_lat / F.lit(2)), 2) +
        F.cos(lat1_rad) * F.cos(lat2_rad) * F.pow(F.sin(delta_lon / F.lit(2)), 2)
    )

    c = F.lit(2) * F.asin(F.sqrt(a))

    distance_km = F.lit(radius_km) * c

    return distance_km


def find_nearest_city(
    events_df: DataFrame,
    cities_df: DataFrame,
    event_lat_col: str = "lat",
    event_lon_col: str = "lon",
    city_lat_col: str = "lat",
    city_lon_col: str = "lng",
    city_name_col: str = "city"
) -> DataFrame:
    """
    Определяет ближайший город для каждого события на основе координат.

    Алгоритм:
    1. Кросс-джойн событий и городов
    2. Расчет расстояния от события до каждого города
    3. Выбор города с минимальным расстоянием

    Args:
        events_df: DataFrame с событиями и координатами
        cities_df: DataFrame со справочником городов и их координатами
        event_lat_col: Название колонки с широтой события
        event_lon_col: Название колонки с долготой события
        city_lat_col: Название колонки с широтой города
        city_lon_col: Название колонки с долготой города
        city_name_col: Название колонки с названием города

    Returns:
        DataFrame: События с добавленной колонкой city и distance_km
    """
    # Переименовываем колонки городов для избежания конфликтов
    # Добавляем timezone на основе города (все города в Австралии)
    cities_renamed = cities_df.select(
        F.col(city_name_col).alias("city"),
        F.col(city_lat_col).alias("city_lat"),
        F.col(city_lon_col).alias("city_lon"),
        F.col("id").alias("city_id") if "id" in cities_df.columns else F.lit(None).alias("city_id")
    ).withColumn(
        "timezone",
        # Определяем timezone на основе города или координат
        F.when(F.col("city").isin(["Sydney", "Melbourne", "Brisbane", "Canberra", "Gold Coast", "Newcastle", "Wollongong", "Cranbourne"]),
               "Australia/Sydney")
        .when(F.col("city").isin(["Perth"]),
              "Australia/Perth")
        .when(F.col("city").isin(["Adelaide"]),
              "Australia/Adelaide")
        .when(F.col("city").isin(["Darwin"]),
              "Australia/Darwin")
        .otherwise("Australia/Sydney")  # По умолчанию Sydney для всех остальных
    )

    # Кросс-джойн событий с городами
    # ОПТИМИЗАЦИЯ: Используем broadcast для маленькой таблицы городов (24 записи)
    events_with_cities = events_df.crossJoin(F.broadcast(cities_renamed))

    # Расчет расстояния до каждого города
    events_with_distances = events_with_cities.withColumn(
        "distance_km",
        calculate_haversine_distance(
            event_lat_col,
            event_lon_col,
            "city_lat",
            "city_lon"
        )
    )

    # Для каждого события находим город с минимальным расстоянием
    # Используем window функцию для ранжирования
    from pyspark.sql.window import Window

    # Предполагаем, что у событий есть уникальный идентификатор или комбинация полей
    # Создаем временный id для группировки, если его нет
    if "event_id" not in events_with_distances.columns:
        events_with_distances = events_with_distances.withColumn(
            "event_id",
            F.monotonically_increasing_id()
        )

    # Окно для ранжирования по расстоянию для каждого события
    window_spec = Window.partitionBy("event_id").orderBy(F.col("distance_km").asc())

    # Добавляем ранг
    events_ranked = events_with_distances.withColumn(
        "rank",
        F.row_number().over(window_spec)
    )

    # Оставляем только ближайший город (rank = 1)
    nearest_cities = events_ranked.filter(F.col("rank") == 1).drop("rank", "city_lat", "city_lon")

    return nearest_cities


def get_city_with_timezone(cities_df: DataFrame) -> DataFrame:
    """
    Подготавливает справочник городов с информацией о timezone.

    Args:
        cities_df: DataFrame с городами

    Returns:
        DataFrame: Обработанный справочник городов
    """
    return cities_df.select(
        F.col("id").alias("city_id"),
        F.col("city"),
        F.col("lat"),
        F.col("lng"),
        F.col("timezone") if "timezone" in cities_df.columns else F.lit("Australia/Sydney").alias("timezone")
    )
