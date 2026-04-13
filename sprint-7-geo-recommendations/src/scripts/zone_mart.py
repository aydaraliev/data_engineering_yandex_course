"""
Script for building the mart aggregated by zones (cities).

The mart contains aggregated event statistics per city:
- Counts of messages, reactions, subscriptions, registrations
- Aggregation by week and month
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


class ZoneMart:
    """Class for building the mart aggregated by zones (cities)."""

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

    def get_user_last_locations(self):
        """
        Skipped - cities are already resolved in the ODS layer.
        """
        logger.info("Skipping position resolution (already performed in ODS)")
        # OPTIMIZATION: this step is no longer needed

    def enrich_events_with_locations(self):
        """
        Skipped - events are already enriched in the ODS layer.
        """
        logger.info("Skipping coordinate enrichment (already performed in ODS)")
        # OPTIMIZATION: data is already enriched in ODS
        self.events_enriched = self.events_df

    def map_events_to_cities(self):
        """
        Skipped - cities are already resolved in the ODS layer.
        """
        logger.info("Skipping city resolution (already performed in ODS)")
        # OPTIMIZATION: cities are already resolved in ODS
        # OPTIMIZATION: cache since used in several methods
        self.events_with_cities = self.events_df.cache()

        # Materialize the cache
        events_count = self.events_with_cities.count()
        logger.info(f"Data cached: {events_count} events")

    def add_time_dimensions(self):
        """Adds time dimensions (month, week)."""
        logger.info("Adding time dimensions...")

        self.events_with_time = self.events_with_cities.withColumn(
            "month",
            F.trunc(F.col("event_datetime"), "month")
        ).withColumn(
            "week",
            F.date_trunc("week", F.col("event_datetime"))  # Week starts on Monday
        )

        logger.info("Time dimensions added")

    def calculate_registrations(self):
        """
        Detects user registrations.

        Registration = the user's first message.
        The city of the first message is used.
        """
        logger.info("Calculating registrations...")

        # Find each user's first message
        messages_only = self.events_with_time.filter(F.col("event_type") == "message")

        window_first = Window.partitionBy("user_id").orderBy("event_datetime")

        registrations = messages_only.withColumn(
            "rank",
            F.row_number().over(window_first)
        ).filter(
            F.col("rank") == 1
        ).withColumn(
            "event_type",
            F.lit("user")  # Mark as a registration
        ).select(
            "event_type",
            "user_id",
            "event_datetime",
            "city",
            "city_id",
            "month",
            "week"
        )

        logger.info(f"Registrations found: {registrations.count()}")

        # Combine with the remaining events
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

        logger.info(f"Total events for aggregation: {self.events_final.count()}")

    def build_zone_mart(self):
        """Builds the final mart aggregated by zones."""
        logger.info("Building the zone mart...")

        # Weekly aggregation
        weekly_agg = self.events_final.groupBy("week", "city_id", "city").agg(
            F.sum(F.when(F.col("event_type") == "message", 1).otherwise(0)).alias("week_message"),
            F.sum(F.when(F.col("event_type") == "reaction", 1).otherwise(0)).alias("week_reaction"),
            F.sum(F.when(F.col("event_type") == "subscription", 1).otherwise(0)).alias("week_subscription"),
            F.sum(F.when(F.col("event_type") == "user", 1).otherwise(0)).alias("week_user")
        )

        # Monthly aggregation
        monthly_agg = self.events_final.groupBy("month", "city_id", "city").agg(
            F.sum(F.when(F.col("event_type") == "message", 1).otherwise(0)).alias("month_message"),
            F.sum(F.when(F.col("event_type") == "reaction", 1).otherwise(0)).alias("month_reaction"),
            F.sum(F.when(F.col("event_type") == "subscription", 1).otherwise(0)).alias("month_subscription"),
            F.sum(F.when(F.col("event_type") == "user", 1).otherwise(0)).alias("month_user")
        )

        # Combine weekly and monthly aggregations
        # For each week find the corresponding month
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

        logger.info(f"Mart built: {self.zone_mart_df.count()} records")

    def save_mart(self):
        """Saves the mart to HDFS."""
        logger.info(f"Saving mart to: {self.output_path}")

        # OPTIMIZATION: coalesce for the optimal number of files per partition
        self.zone_mart_df.coalesce(4).write \
            .mode("overwrite") \
            .partitionBy("month") \
            .parquet(self.output_path)

        logger.info("Mart saved successfully")

    def show_sample(self, n=20):
        """Shows sample records from the mart."""
        logger.info(f"\nSample mart records (first {n}):")
        self.zone_mart_df.show(n, truncate=False)

        logger.info("\nMart statistics:")
        self.zone_mart_df.select(
            F.count("*").alias("total_records"),
            F.countDistinct("zone_id").alias("unique_zones"),
            F.countDistinct("week").alias("unique_weeks"),
            F.countDistinct("month").alias("unique_months"),
            F.sum("week_message").alias("total_week_messages"),
            F.sum("month_message").alias("total_month_messages")
        ).show()

    def run(self):
        """Executes the full mart build process."""
        logger.info("=" * 70)
        logger.info("STARTING ZONE MART BUILD")
        if self.sample_fraction < 1.0:
            logger.info(f"SAMPLE MODE: {self.sample_fraction * 100}%")
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

            # Print performance metrics
            logger.info("=" * 70)
            logger.info("PERFORMANCE METRICS:")
            logger.info(f"  Data load:              {self.metrics.get_duration('start', 'load_data'):.2f}s")
            logger.info(f"  City resolution:        {self.metrics.get_duration('load_data', 'map_events_to_cities'):.2f}s")
            logger.info(f"  Time dimensions:        {self.metrics.get_duration('map_events_to_cities', 'add_time_dimensions'):.2f}s")
            logger.info(f"  Registrations:          {self.metrics.get_duration('add_time_dimensions', 'calculate_registrations'):.2f}s")
            logger.info(f"  Mart build:             {self.metrics.get_duration('calculate_registrations', 'build_zone_mart'):.2f}s")
            logger.info(f"  Save:                   {self.metrics.get_duration('build_zone_mart', 'save_mart'):.2f}s")
            logger.info(f"  TOTAL:                  {self.metrics.get_duration('start', 'save_mart'):.2f}s")
            logger.info("=" * 70)
            logger.info("ZONE MART BUILT SUCCESSFULLY")
            logger.info("=" * 70)

            return 0

        except Exception as e:
            logger.error(f"Error while building the mart: {e}", exc_info=True)
            return 1


def main():
    """Main entry point."""
    # Parse command-line arguments
    parser = argparse.ArgumentParser(description='Build the zone (city) mart')
    parser.add_argument('--sample', type=float, default=None,
                       help='Sample fraction (0.0-1.0), e.g. 0.1 for 10%%')
    args = parser.parse_args()

    # Derive sample_fraction from argument or environment variable
    sample_fraction = args.sample if args.sample is not None else float(os.getenv('SAMPLE_FRACTION', '1.0'))

    # Create Spark session
    spark = SparkSession.builder \
        .appName("ZoneMart") \
        .config("spark.sql.adaptive.enabled", "true") \
        .getOrCreate()

    # OPTIMIZATION: read from ODS instead of RAW
    ods_path = "/user/student/project/geo/ods/events_with_cities"
    output_path = "/user/student/project/geo/mart/zone_mart"

    # Create and launch the process
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
