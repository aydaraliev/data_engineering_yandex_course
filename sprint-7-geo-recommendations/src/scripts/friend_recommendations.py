"""
Friend recommendations mart.

Logic:
- Users subscribed to the same channel
- They have never exchanged messages before
- Distance between them <= 1 km
- Pairs must be unique (user_left < user_right)
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


class FriendRecommendations:
    """Class for building the friend recommendations mart."""

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
        self.processed_dttm = F.current_timestamp()
        self.metrics = PerformanceMetrics()

    def load_data(self):
        """Loads data from the ODS layer."""
        logger.info(f"Loading events from ODS: {self.ods_path}")
        # OPTIMIZATION: read already enriched data from ODS instead of RAW
        events_raw = self.spark.read.parquet(self.ods_path)

        # Apply sampling if requested
        if self.sample_fraction < 1.0:
            logger.info(f"Applying sample fraction {self.sample_fraction} with seed=42")
            events_sampled = events_raw.sample(fraction=self.sample_fraction, seed=42)
        else:
            events_sampled = events_raw

        # OPTIMIZATION: cache since it is used in 3 methods (subscriptions, locations, communications)
        self.events_df = events_sampled.cache()

        # Materialize the cache
        events_count = self.events_df.count()
        logger.info(f"Events loaded from ODS and cached: {events_count} records")

    def get_user_subscriptions(self):
        """Retrieves user channel subscriptions."""
        logger.info("Extracting user subscriptions...")

        subscriptions = self.events_df.filter(
            F.col("event_type") == "subscription"
        ).select(
            F.col("event.user").alias("user_id"),
            F.col("event.subscription_channel").alias("channel_id")
        ).distinct()

        # logger.info(f"Unique subscriptions found: {subscriptions.count()}")  # Commented for performance

        self.subscriptions_df = subscriptions

    def get_user_last_locations(self):
        """Retrieves the last geolocation for each user from ODS."""
        logger.info("Fetching last user positions from ODS...")

        # OPTIMIZATION: cities are already resolved in ODS, just pick the last position
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

        logger.info("Last user positions retrieved from ODS")

    def map_users_to_cities(self):
        """Skipped - cities are already resolved in the ODS layer."""
        logger.info("Skipping city resolution (already performed in ODS)")
        # OPTIMIZATION: cities already resolved in ODS
        self.users_with_cities = self.user_locations_df
        logger.info(f"City distribution:")
        self.users_with_cities.groupBy("city").count().orderBy(F.desc("count")).show(10)

    def get_user_communications(self):
        """Retrieves pairs of users who have exchanged messages."""
        logger.info("Extracting pairs of users who have communicated...")

        messages = self.events_df.filter(
            F.col("event_type") == "message"
        ).select(
            F.col("event.message_from").alias("user_from"),
            F.col("event.message_to").alias("user_to")
        ).filter(
            F.col("user_to").isNotNull()
        )

        # logger.info(f"Messages between users found: {messages.count()}")  # Commented for performance

        # Build unique pairs (order does not matter)
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

        # logger.info(f"Unique pairs that have communicated: {self.communicated_pairs.count()}")  # Commented for performance

    def find_channel_peers(self):
        """Finds pairs of users subscribed to the same channel."""
        logger.info("Looking for pairs of users with shared subscriptions...")

        # Self-join: find user pairs in the same channel
        channel_pairs = self.subscriptions_df.alias("s1").join(
            self.subscriptions_df.alias("s2"),
            on=F.col("s1.channel_id") == F.col("s2.channel_id"),
            how="inner"
        ).filter(
            F.col("s1.user_id") < F.col("s2.user_id")  # Avoid duplicates and self-pairs
        ).select(
            F.col("s1.user_id").alias("user_left"),
            F.col("s2.user_id").alias("user_right"),
            F.col("s1.channel_id").alias("channel_id")
        ).distinct()

        # logger.info(f"Pairs with shared subscriptions: {channel_pairs.count()}")  # Commented for performance

        self.channel_pairs_df = channel_pairs

    def filter_non_communicated(self):
        """Filters pairs that have never communicated."""
        logger.info("Filtering pairs that have never communicated...")

        # LEFT ANTI JOIN: keep only pairs that are NOT in communicated_pairs
        self.non_communicated_pairs = self.channel_pairs_df.join(
            self.communicated_pairs,
            on=["user_left", "user_right"],
            how="left_anti"
        )

        # logger.info(f"Pairs that have not communicated: {self.non_communicated_pairs.count()}")  # Commented for performance

    def enrich_with_locations(self):
        """Enriches pairs with coordinates of both users."""
        logger.info("Enriching pairs with user coordinates...")

        # Attach coordinates for user_left
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

        # logger.info(f"Pairs with user_left coordinates: {pairs_with_left.count()}")  # Commented for performance

        # Attach coordinates for user_right
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

        # logger.info(f"Pairs with coordinates for both users: {pairs_with_both.count()}")  # Commented for performance

        # Filter pairs in the same city
        self.pairs_with_coords = pairs_with_both.filter(
            F.col("zone_id") == F.col("city_id_right")
        ).drop("city_id_right")

        # logger.info(f"Pairs in the same city: {self.pairs_with_coords.count()}")  # Commented for performance

    def calculate_distances(self):
        """Computes the distance between users."""
        logger.info("Computing distances between users...")

        # Use the Haversine formula
        distance_km = calculate_haversine_distance(
            "lat_left", "lon_left",
            "lat_right", "lon_right"
        )

        self.pairs_with_distance = self.pairs_with_coords.withColumn(
            "distance_km",
            distance_km
        )

        logger.info("Distance statistics:")
        self.pairs_with_distance.select(
            F.min("distance_km").alias("min_km"),
            F.avg("distance_km").alias("avg_km"),
            F.max("distance_km").alias("max_km"),
            F.count("*").alias("total_pairs")
        ).show()

    def filter_by_distance(self, max_distance_km: float = 1.0):
        """Filters pairs by distance."""
        logger.info(f"Filtering pairs with distance <= {max_distance_km} km...")

        self.close_pairs = self.pairs_with_distance.filter(
            F.col("distance_km") <= max_distance_km
        )

        # logger.info(f"Pairs within {max_distance_km} km: {self.close_pairs.count()}")  # Commented for performance

    def calculate_local_time(self):
        """Computes the local time for each pair."""
        logger.info("Computing local time...")

        self.recommendations_with_time = self.close_pairs.withColumn(
            "local_time",
            F.from_utc_timestamp(self.processed_dttm, F.col("timezone"))
        )

        logger.info("Local time added")

    def build_recommendations(self):
        """Builds the final recommendations mart."""
        logger.info("Building the recommendations mart...")

        self.recommendations_df = self.recommendations_with_time.select(
            "user_left",
            "user_right",
            self.processed_dttm.alias("processed_dttm"),
            "zone_id",
            "local_time"
        ).distinct()

        # logger.info(f"Mart built: {self.recommendations_df.count()} recommendations")  # Commented for performance

    def save_recommendations(self):
        """Saves the mart to HDFS."""
        logger.info(f"Saving mart to: {self.output_path}")

        # OPTIMIZATION: coalesce to reduce the number of small files
        self.recommendations_df.coalesce(2).write \
            .mode("overwrite") \
            .parquet(self.output_path)

        logger.info("Mart saved successfully")

    def show_sample(self, n=20):
        """Shows sample recommendations."""
        logger.info(f"\nSample recommendations (first {n}):")
        self.recommendations_df.show(n, truncate=False)

        logger.info("\nStatistics by zone:")
        self.recommendations_df.groupBy("zone_id").agg(
            F.count("*").alias("recommendations_count")
        ).orderBy(F.desc("recommendations_count")).show(10)

        logger.info("\nOverall statistics:")
        self.recommendations_df.select(
            F.count("*").alias("total_recommendations"),
            F.countDistinct("user_left").alias("unique_users_left"),
            F.countDistinct("user_right").alias("unique_users_right"),
            F.countDistinct("zone_id").alias("unique_zones")
        ).show(truncate=False)

    def run(self):
        """Executes the full mart build process."""
        logger.info("=" * 70)
        logger.info("STARTING FRIEND RECOMMENDATIONS MART BUILD")
        if self.sample_fraction < 1.0:
            logger.info(f"SAMPLE MODE: {self.sample_fraction * 100}%")
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

            # Print performance metrics
            logger.info("=" * 70)
            logger.info("PERFORMANCE METRICS:")
            logger.info(f"  Data load:              {self.metrics.get_duration('start', 'load_data'):.2f}s")
            logger.info(f"  Subscriptions:          {self.metrics.get_duration('load_data', 'get_user_subscriptions'):.2f}s")
            logger.info(f"  User positions:         {self.metrics.get_duration('get_user_subscriptions', 'map_users_to_cities'):.2f}s")
            logger.info(f"  Communication history:  {self.metrics.get_duration('map_users_to_cities', 'get_user_communications'):.2f}s")
            logger.info(f"  Channel pair search:    {self.metrics.get_duration('get_user_communications', 'find_channel_peers'):.2f}s")
            logger.info(f"  Coordinate enrichment:  {self.metrics.get_duration('find_channel_peers', 'enrich_with_locations'):.2f}s")
            logger.info(f"  Distance calculation:   {self.metrics.get_duration('enrich_with_locations', 'calculate_distances'):.2f}s")
            logger.info(f"  Build recommendations:  {self.metrics.get_duration('calculate_distances', 'build_recommendations'):.2f}s")
            logger.info(f"  Save:                   {self.metrics.get_duration('build_recommendations', 'save_recommendations'):.2f}s")
            logger.info(f"  TOTAL:                  {self.metrics.get_duration('start', 'save_recommendations'):.2f}s")
            logger.info("=" * 70)
            logger.info("FRIEND RECOMMENDATIONS MART BUILT SUCCESSFULLY")
            logger.info("=" * 70)

            return 0

        except Exception as e:
            logger.error(f"Error while building the mart: {e}", exc_info=True)
            return 1


def main():
    """Main entry point."""
    # Parse command-line arguments
    parser = argparse.ArgumentParser(description='Build the friend recommendations mart')
    parser.add_argument('--sample', type=float, default=None,
                       help='Sample fraction (0.0-1.0), e.g. 0.1 for 10%%')
    args = parser.parse_args()

    # Derive sample_fraction from argument or environment variable
    sample_fraction = args.sample if args.sample is not None else float(os.getenv('SAMPLE_FRACTION', '1.0'))

    spark = SparkSession.builder \
        .appName("FriendRecommendations") \
        .config("spark.sql.adaptive.enabled", "true") \
        .getOrCreate()

    # OPTIMIZATION: read from ODS instead of RAW
    ods_path = "/user/student/project/geo/ods/events_with_cities"
    output_path = "/user/student/project/geo/mart/friend_recommendations"

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
