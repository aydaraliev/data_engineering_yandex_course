"""
Script for building the user geo analytics data mart.

The mart includes:
- user_id: user identifier
- act_city: active city (last message)
- home_city: home city (city where the user stayed 27+ days)
- travel_count: number of visited cities
- travel_array: list of cities in visiting order
- local_time: local time of the last event
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

# Logging configuration
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class PerformanceMetrics:
    """Performance metrics tracking."""
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
    """Class for building the user geo data mart."""

    def __init__(
        self,
        spark: SparkSession,
        ods_path: str,
        output_path: str,
        sample_fraction: float = 1.0
    ):
        """
        Initialization.

        Args:
            spark: SparkSession
            ods_path: Path to the ODS layer (events_with_cities)
            output_path: Path for saving the mart
            sample_fraction: Sample fraction (0.0-1.0); default 1.0 (all data)
        """
        self.spark = spark
        self.ods_path = ods_path
        self.output_path = output_path
        self.sample_fraction = sample_fraction
        self.metrics = PerformanceMetrics()

    def load_data(self):
        """Loads data from the ODS layer."""
        logger.info(f"Loading events from ODS: {self.ods_path}")
        # OPTIMIZATION: read already enriched data from ODS instead of RAW
        events_raw = self.spark.read.parquet(self.ods_path)

        # Apply sampling if requested
        if self.sample_fraction < 1.0:
            logger.info(f"Applying sample fraction {self.sample_fraction} with seed=42")
            self.events_df = events_raw.sample(fraction=self.sample_fraction, seed=42)
        else:
            self.events_df = events_raw

        logger.info("Events loaded from ODS (already enriched with cities)")

    def filter_messages_with_coords(self):
        """Filters messages only (already with cities from ODS)."""
        logger.info("Filtering messages...")

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

        logger.info("Messages filtered")

    def enrich_with_cities(self):
        """Data is already enriched in the ODS layer."""
        logger.info("Skipping city enrichment (already performed in ODS)")
        # OPTIMIZATION: this step is no longer needed, data is already enriched in ODS
        # OPTIMIZATION: cache since used in 3 methods (act_city, home_city, travel_stats)
        self.enriched_df = self.messages_df.cache()

        # Materialize the cache
        msg_count = self.enriched_df.count()
        logger.info(f"Data cached: {msg_count} messages")

    def calculate_act_city(self):
        """
        Determines the active city (act_city) for each user.

        act_city - city from which the last message was sent.
        """
        logger.info("Calculating the active city (act_city)...")

        # Window for identifying the user's last event
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

        # logger.info(f"Active city determined for {self.act_city_df.count()} users")  # Commented for performance

    def calculate_home_city(self):
        """
        Determines the home city (home_city) for each user.

        home_city - the last city where the user stayed longer than 27 days.

        Approach: look for uninterrupted sequences of days in the same city.
        A city is considered home if:
        - The user sent messages from this city
        - The period of activity in the city is 27+ days (from first to last message)
        """
        logger.info("Calculating the home city (home_city)...")

        # Add event date (without time)
        messages_with_date = self.enriched_df.withColumn(
            "event_date",
            F.to_date(F.col("event_datetime"))
        )

        # Detect city change to identify uninterrupted sequences
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

        # For each sequence compute the stay duration
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

        # Filter periods >= 27 days
        home_cities = city_periods.filter(
            F.col("days_in_city") >= 27
        )

        # Take the last period (by last event time)
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

        # logger.info(f"Home city determined for {self.home_city_df.count()} users")  # Commented for performance

    def calculate_travel_stats(self):
        """
        Computes travel statistics.

        - travel_count: number of visited cities (with repeats)
        - travel_array: list of cities in visiting order
        """
        logger.info("Computing travel statistics...")

        # Sort events by time
        window_travel = Window.partitionBy("user_id").orderBy("event_datetime")

        # Detect city change
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

        # Aggregate travels
        self.travel_df = travel_events.groupBy("user_id").agg(
            F.count("city").alias("travel_count"),
            F.collect_list("city").alias("travel_array")
        )

        # logger.info(f"Travel statistics computed for {self.travel_df.count()} users")  # Commented for performance

    def calculate_local_time(self):
        """
        Computes the local time of the last event.

        local_time - time of the last event taking the geolocation timezone into account.
        """
        logger.info("Computing local time...")

        # Use data from act_city_df where the last event time and timezone are already available
        self.local_time_df = self.act_city_df.withColumn(
            "local_time",
            F.from_utc_timestamp(F.col("last_event_time"), F.col("act_timezone"))
        ).select(
            "user_id",
            "local_time"
        )

        logger.info("Local time computed")

    def build_mart(self):
        """Assembles the final mart."""
        logger.info("Assembling the final mart...")

        # Combine all components
        mart = self.act_city_df.select("user_id", "act_city")

        # Add home_city
        mart = mart.join(
            self.home_city_df,
            on="user_id",
            how="left"
        )

        # Add travel_count and travel_array
        mart = mart.join(
            self.travel_df,
            on="user_id",
            how="left"
        )

        # Add local_time
        mart = mart.join(
            self.local_time_df,
            on="user_id",
            how="left"
        )

        # Final mart
        self.mart_df = mart.select(
            "user_id",
            "act_city",
            "home_city",
            "travel_count",
            "travel_array",
            "local_time"
        )

        # logger.info(f"Mart built: {self.mart_df.count()} users")  # Commented for performance

    def save_mart(self):
        """Saves the mart to HDFS."""
        logger.info(f"Saving mart to: {self.output_path}")

        # OPTIMIZATION: coalesce to reduce the number of small files
        self.mart_df.coalesce(2).write \
            .mode("overwrite") \
            .parquet(self.output_path)

        logger.info("Mart saved successfully")

    def show_sample(self, n=10):
        """Shows sample records from the mart."""
        logger.info(f"Sample mart records (first {n}):")
        self.mart_df.show(n, truncate=False)

    def run(self):
        """Executes the full mart build process."""
        logger.info("=" * 70)
        logger.info("STARTING USER_GEO_REPORT MART BUILD")
        if self.sample_fraction < 1.0:
            logger.info(f"SAMPLE MODE: {self.sample_fraction * 100}%")
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

            # Print performance metrics
            logger.info("=" * 70)
            logger.info("PERFORMANCE METRICS:")
            logger.info(f"  Data load:              {self.metrics.get_duration('start', 'load_data'):.2f}s")
            logger.info(f"  City enrichment:        {self.metrics.get_duration('load_data', 'enrich_with_cities'):.2f}s")
            logger.info(f"  act_city calculation:   {self.metrics.get_duration('enrich_with_cities', 'calculate_act_city'):.2f}s")
            logger.info(f"  home_city calculation:  {self.metrics.get_duration('calculate_act_city', 'calculate_home_city'):.2f}s")
            logger.info(f"  Travel calculation:     {self.metrics.get_duration('calculate_home_city', 'calculate_travel_stats'):.2f}s")
            logger.info(f"  Mart assembly:          {self.metrics.get_duration('calculate_travel_stats', 'build_mart'):.2f}s")
            logger.info(f"  Save:                   {self.metrics.get_duration('build_mart', 'save_mart'):.2f}s")
            logger.info(f"  TOTAL:                  {self.metrics.get_duration('start', 'save_mart'):.2f}s")
            logger.info("=" * 70)
            logger.info("USER_GEO_REPORT MART BUILT SUCCESSFULLY")
            logger.info("=" * 70)

            return 0

        except Exception as e:
            logger.error(f"Error while building the mart: {e}", exc_info=True)
            return 1


def main():
    """Main entry point."""
    # Parse command-line arguments
    parser = argparse.ArgumentParser(description='Build the user geo data mart')
    parser.add_argument('--sample', type=float, default=None,
                       help='Sample fraction (0.0-1.0), e.g. 0.1 for 10%%')
    args = parser.parse_args()

    # Derive sample_fraction from argument or environment variable
    sample_fraction = args.sample if args.sample is not None else float(os.getenv('SAMPLE_FRACTION', '1.0'))

    # Create Spark session
    spark = SparkSession.builder \
        .appName("UserGeoMart") \
        .config("spark.sql.adaptive.enabled", "true") \
        .getOrCreate()

    # OPTIMIZATION: read from ODS instead of RAW
    ods_path = "/user/student/project/geo/ods/events_with_cities"
    output_path = "/user/student/project/geo/mart/user_geo_report"

    # Create and launch the process
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
