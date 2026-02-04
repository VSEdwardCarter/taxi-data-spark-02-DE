from pyspark.sql import functions as F

from src.spark import build_spark
from src.paths import SILVER_DIR, GOLD_DIR, s


SILVER_IN = SILVER_DIR / "yellow_tripdata_2016-01"
GOLD_OUT  = GOLD_DIR / "daily_metrics" / "yellow_tripdata_2016-01"

def main():
    spark = build_spark("nyc-taxi-gold-daily-metrics")
    df = spark.read.parquet(s(SILVER_IN))

    gold = (
        df.groupBy("pickup_date")
        .agg(
            F.count("*").alias("trip_count"),
            F.round(F.sum("total_amount"), 2).alias("total_revenue"),
            F.round(F.avg("total_amount"), 2).alias("avg_total_amount"),
            F.round(F.avg("trip_distance"), 3).alias("avg_trip_distance"),
            F.round(F.avg("trip_duration_min"), 2).alias("avg_trip_duration_min"),
        )
        .orderBy("pickup_date")
    )

    (
        gold.coalesce(1)  # small output; keep it readable
        .write.mode("overwrite")
        .partitionBy("pickup_date")
        .parquet(s(GOLD_OUT))
    )

    spark.stop()


if __name__ == "__main__":
    main()
