from pyspark.sql import functions as F
from src.paths import SILVER_CLEAN_DIR, GOLD_DIR, s
from src.spark import build_spark

silver_inputFile = SILVER_CLEAN_DIR / "clean_silver_yellow_tripdata"
gold_outputFile = GOLD_DIR / "gold_metrics"

# SILVER_IN = SILVER_DIR / "trips_clean" / "clean_silver_yellow_tripdata"
# GOLD_OUT  = GOLD_DIR / "daily_metrics" / "yellow_tripdata_2016-01"

def main():
    spark = build_spark("2016-03-gold-metrics")

    clean = spark.read.parquet(s(silver_inputFile))

    gold = (
        clean
        .groupBy("pickup_date")
        .agg(
            F.count("*").alias("trip_count"),
            F.round(F.sum("total_amount"), 2).alias("total_revenue"),
            F.round(F.avg("total_amount"), 2).alias("avg_total_amount"),
            F.round(F.avg("trip_distance"), 3).alias("avg_trip_distance"),
            F.round(F.avg("trip_duration_min"),3).alias("avg_trip_duration")
        )
        .orderBy("pickup_date")
    )
    (
        gold
        .coalesce(1)  # small, daily output
        .write
        .mode("overwrite")
        .partitionBy("pickup_date")
        .parquet(s(gold_outputFile))
    )

    spark.stop()


if __name__ == "__main__":
    main()

