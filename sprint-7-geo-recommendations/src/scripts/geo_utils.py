"""
Utilities for working with geographical data.
Contains functions for distance calculation and resolving cities by coordinates.
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
    Computes the distance between two points on a sphere using the haversine formula.

    Formula:
    d = 2r * arcsin(sqrt(sin^2((phi2-phi1)/2) + cos(phi1) * cos(phi2) * sin^2((lambda2-lambda1)/2)))

    where:
    - phi1, phi2 - latitudes of the points in radians
    - lambda1, lambda2 - longitudes of the points in radians
    - r - radius of the sphere (Earth), approximately 6371 km

    Args:
        lat1_col: Column name with latitude of the first point (in degrees)
        lon1_col: Column name with longitude of the first point (in degrees)
        lat2_col: Column name with latitude of the second point (in degrees)
        lon2_col: Column name with longitude of the second point (in degrees)
        radius_km: Earth radius in kilometres

    Returns:
        Column: Distance between the points in kilometres
    """
    # Convert degrees to radians
    lat1_rad = F.radians(F.col(lat1_col))
    lon1_rad = F.radians(F.col(lon1_col))
    lat2_rad = F.radians(F.col(lat2_col))
    lon2_rad = F.radians(F.col(lon2_col))

    # Coordinate differences
    delta_lat = lat2_rad - lat1_rad
    delta_lon = lon2_rad - lon1_rad

    # Haversine formula
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
    Determines the nearest city for each event based on coordinates.

    Algorithm:
    1. Cross-join events and cities
    2. Compute the distance from the event to each city
    3. Pick the city with the minimum distance

    Args:
        events_df: DataFrame with events and coordinates
        cities_df: DataFrame with the city dictionary and their coordinates
        event_lat_col: Column name with the event latitude
        event_lon_col: Column name with the event longitude
        city_lat_col: Column name with the city latitude
        city_lon_col: Column name with the city longitude
        city_name_col: Column name with the city name

    Returns:
        DataFrame: Events with added city and distance_km columns
    """
    # Rename city columns to avoid conflicts
    # Add timezone based on the city (all cities are in Australia)
    cities_renamed = cities_df.select(
        F.col(city_name_col).alias("city"),
        F.col(city_lat_col).alias("city_lat"),
        F.col(city_lon_col).alias("city_lon"),
        F.col("id").alias("city_id") if "id" in cities_df.columns else F.lit(None).alias("city_id")
    ).withColumn(
        "timezone",
        # Resolve timezone from city or coordinates
        F.when(F.col("city").isin(["Sydney", "Melbourne", "Brisbane", "Canberra", "Gold Coast", "Newcastle", "Wollongong", "Cranbourne"]),
               "Australia/Sydney")
        .when(F.col("city").isin(["Perth"]),
              "Australia/Perth")
        .when(F.col("city").isin(["Adelaide"]),
              "Australia/Adelaide")
        .when(F.col("city").isin(["Darwin"]),
              "Australia/Darwin")
        .otherwise("Australia/Sydney")  # Default to Sydney for everyone else
    )

    # Cross-join events with cities
    # OPTIMIZATION: use broadcast for the small cities table (24 rows)
    events_with_cities = events_df.crossJoin(F.broadcast(cities_renamed))

    # Compute distance to each city
    events_with_distances = events_with_cities.withColumn(
        "distance_km",
        calculate_haversine_distance(
            event_lat_col,
            event_lon_col,
            "city_lat",
            "city_lon"
        )
    )

    # For each event find the city with the minimum distance
    # Use a window function for ranking
    from pyspark.sql.window import Window

    # Assume the events have a unique identifier or combination of fields
    # Create a temporary id for grouping if missing
    if "event_id" not in events_with_distances.columns:
        events_with_distances = events_with_distances.withColumn(
            "event_id",
            F.monotonically_increasing_id()
        )

    # Window for ranking by distance per event
    window_spec = Window.partitionBy("event_id").orderBy(F.col("distance_km").asc())

    # Add rank
    events_ranked = events_with_distances.withColumn(
        "rank",
        F.row_number().over(window_spec)
    )

    # Keep only the nearest city (rank = 1)
    nearest_cities = events_ranked.filter(F.col("rank") == 1).drop("rank", "city_lat", "city_lon")

    return nearest_cities


def get_city_with_timezone(cities_df: DataFrame) -> DataFrame:
    """
    Prepares the cities dictionary with timezone information.

    Args:
        cities_df: DataFrame with cities

    Returns:
        DataFrame: Processed cities dictionary
    """
    return cities_df.select(
        F.col("id").alias("city_id"),
        F.col("city"),
        F.col("lat"),
        F.col("lng"),
        F.col("timezone") if "timezone" in cities_df.columns else F.lit("Australia/Sydney").alias("timezone")
    )
