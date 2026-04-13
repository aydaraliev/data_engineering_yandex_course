"""
Script for building the ODS (Operational Data Store) layer.

Produces a cleaned and enriched data layer for reuse:
- events_with_cities: events with resolved cities (used by every mart)

Benefits of the ODS layer:
- The expensive find_nearest_city operation is executed once
- Enriched data is reused by every mart
- Accelerates mart development and debugging
- Centralizes data quality validation
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


class ODSLayer:
    """Class for building the ODS layer."""

    def __init__(
        self,
        spark: SparkSession,
        events_path: str,
        cities_path: str,
        output_path: str,
        sample_fraction: float = 1.0
    ):
        """
        Initialization.

        Args:
            spark: SparkSession
            events_path: Path to the events data (RAW)
            cities_path: Path to the city dictionary
            output_path: Path for saving the ODS
            sample_fraction: Sample fraction (0.0-1.0); default 1.0 (all data)
        """
        self.spark = spark
        self.events_path = events_path
        self.cities_path = cities_path
        self.output_path = output_path
        self.sample_fraction = sample_fraction
        self.metrics = PerformanceMetrics()

    def load_data(self):
        """Loads data from the RAW layer."""
        logger.info(f"Loading events from RAW: {self.events_path}")
        events_raw = self.spark.read.parquet(self.events_path)

        # Apply sampling if requested
        if self.sample_fraction < 1.0:
            logger.info(f"Applying sample fraction {self.sample_fraction} with seed=42")
            self.events_df = events_raw.sample(fraction=self.sample_fraction, seed=42)
        else:
            self.events_df = events_raw

        logger.info(f"Loading cities from: {self.cities_path}")
        self.cities_df = self.spark.read.csv(
            self.cities_path,
            header=True,
            inferSchema=True,
            sep=";"
        )

        logger.info("Data loaded from the RAW layer")

    def prepare_events(self):
        """
        Prepares events for enrichment.

        Extracts coordinates from messages and assigns the last known
        coordinates to events without a geo-position.
        """
        logger.info("Preparing events for enrichment...")

        # Extract messages with coordinates
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

        # Get the last location for every user
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

        # Prepare all events
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

        # Attach the user's last known coordinates
        # OPTIMIZATION: broadcast for the small user_locations table
        events_with_locations = events_prepared.join(
            F.broadcast(user_locations),
            on="user_id",
            how="left"
        )

        # Use event coordinates if present, otherwise fall back to the user's last coordinates
        self.events_enriched = events_with_locations.withColumn(
            "final_lat",
            F.coalesce(F.col("lat"), F.col("last_lat"))
        ).withColumn(
            "final_lon",
            F.coalesce(F.col("lon"), F.col("last_lon"))
        ).filter(
            F.col("final_lat").isNotNull() & F.col("final_lon").isNotNull()
        ).cache()  # OPTIMIZATION: cache for reuse

        # Materialize the cache
        events_count = self.events_enriched.count()
        logger.info(f"Events prepared for enrichment and cached: {events_count} records")

    def enrich_with_cities(self):
        """
        Enriches events with city information.

        This is the most expensive operation (cross-join N x 24).
        Performed once in the ODS layer instead of three times per mart.
        """
        logger.info("Enriching events with cities (ONE operation for all marts)...")

        # Prepare data for find_nearest_city
        events_for_mapping = self.events_enriched.select(
            "event_type",
            "event",
            "user_id",
            "event_datetime",
            "date",
            F.col("final_lat").alias("lat"),
            F.col("final_lon").alias("lon")
        )

        # Determine the nearest city
        # OPTIMIZATION: broadcast is applied inside find_nearest_city
        self.events_with_cities = find_nearest_city(
            events_for_mapping,
            self.cities_df,
            event_lat_col="lat",
            event_lon_col="lon",
            city_lat_col="lat",
            city_lon_col="lng",
            city_name_col="city"
        )

        logger.info("Events enriched with city information")

    def save_ods(self):
        """
        Saves the ODS layer to HDFS.

        Partitioning by date for efficient incremental updates.
        """
        logger.info(f"Saving ODS layer to: {self.output_path}")

        # OPTIMIZATION: partitioning by date for incremental updates
        # OPTIMIZATION: coalesce for the optimal number of files per partition
        self.events_with_cities.coalesce(4).write \
            .mode("overwrite") \
            .partitionBy("date") \
            .parquet(self.output_path)

        logger.info("ODS layer saved successfully")

    def show_statistics(self):
        """Shows ODS layer statistics."""
        logger.info("\n=== ODS layer statistics ===")

        # Overall statistics
        logger.info("Event distribution by type:")
        self.events_with_cities.groupBy("event_type").count().orderBy(F.desc("count")).show()

        logger.info("Event distribution by city:")
        self.events_with_cities.groupBy("city").count().orderBy(F.desc("count")).show(10)

        logger.info("Distribution by date:")
        self.events_with_cities.groupBy("date").count().orderBy("date").show(10)

    def run(self):
        """Executes the full ODS layer build process."""
        logger.info("=" * 70)
        logger.info("STARTING ODS LAYER BUILD")
        if self.sample_fraction < 1.0:
            logger.info(f"SAMPLE MODE: {self.sample_fraction * 100}%")
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

            # Print performance metrics
            logger.info("=" * 70)
            logger.info("PERFORMANCE METRICS:")
            logger.info(f"  Data load:              {self.metrics.get_duration('start', 'load_data'):.2f}s")
            logger.info(f"  Event preparation:      {self.metrics.get_duration('load_data', 'prepare_events'):.2f}s")
            logger.info(f"  City enrichment:        {self.metrics.get_duration('prepare_events', 'enrich_with_cities'):.2f}s")
            logger.info(f"  ODS save:               {self.metrics.get_duration('enrich_with_cities', 'save_ods'):.2f}s")
            logger.info(f"  TOTAL:                  {self.metrics.get_duration('start', 'show_statistics'):.2f}s")
            logger.info("=" * 70)
            logger.info("ODS LAYER BUILT SUCCESSFULLY")
            logger.info("All marts can now read from ODS instead of RAW")
            logger.info("=" * 70)

            return 0

        except Exception as e:
            logger.error(f"Error while building the ODS: {e}", exc_info=True)
            return 1


def main():
    """Main entry point."""
    # Parse command-line arguments
    parser = argparse.ArgumentParser(description='Create the ODS layer with events and cities')
    parser.add_argument('--sample', type=float, default=None,
                       help='Sample fraction (0.0-1.0), e.g. 0.1 for 10%%')
    args = parser.parse_args()

    # Derive sample_fraction from argument or environment variable
    sample_fraction = args.sample if args.sample is not None else float(os.getenv('SAMPLE_FRACTION', '1.0'))

    spark = SparkSession.builder \
        .appName("CreateODSLayer") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.shuffle.partitions", "20") \
        .getOrCreate()

    # Parameters
    events_path = "/user/master/data/geo/events"
    cities_path = "/user/student/project/geo/raw/geo_csv/geo.csv"
    output_path = "/user/student/project/geo/ods/events_with_cities"

    # Build the ODS layer
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
