from pyspark.sql import functions as F
from pyspark.sql import types as T

from src.spark import build_spark
from src.paths import BRONZE_DIR, SILVER_DIR, s

BRONZE_IN = BRONZE_DIR / "yellow_tripdata_2016-01"
SILVER_OUT = SILVER_DIR / "yellow_tripdata_2016-01"


def to_ts(colname: str):
    # TLC files are usually "YYYY-MM-DD HH:MM:SS"
    return F.to_timestamp(F.col(colname), "yyyy-MM-dd HH:mm:ss")


def main():
    spark = build_spark("nyc-taxi-silver-clean")

    df = spark.read.parquet(s(BRONZE_IN))

    # ---- 1) Standardize types (cast strings from CSV to real numeric types) ----
    # NOTE: Some TLC versions use these exact names; if any column is missing,
    # Spark will throw an AnalysisException (which is good; we’ll adjust names).
    df = (
        df
        .withColumn("VendorID", F.col("VendorID").cast("int"))
        .withColumn("RatecodeID", F.col("RatecodeID").cast("int"))
        .withColumn("passenger_count", F.col("passenger_count").cast("int"))
        .withColumn("trip_distance", F.col("trip_distance").cast("double"))
        .withColumn("payment_type", F.col("payment_type").cast("int"))
        .withColumn("fare_amount", F.col("fare_amount").cast("double"))
        .withColumn("extra", F.col("extra").cast("double"))
        .withColumn("mta_tax", F.col("mta_tax").cast("double"))
        .withColumn("tip_amount", F.col("tip_amount").cast("double"))
        .withColumn("tolls_amount", F.col("tolls_amount").cast("double"))
        .withColumn("improvement_surcharge", F.col("improvement_surcharge").cast("double"))
        .withColumn("total_amount", F.col("total_amount").cast("double"))
    )

    # ---- 2) Parse timestamps + derive pickup_date + duration ----
    # Common TLC names:
    #   tpep_pickup_datetime, tpep_dropoff_datetime
    df = (
        df
        .withColumn("pickup_ts", to_ts("tpep_pickup_datetime"))
        .withColumn("dropoff_ts", to_ts("tpep_dropoff_datetime"))
        .withColumn("pickup_date", F.to_date(F.col("pickup_ts")))
        .withColumn(
            "trip_duration_min",
            (F.col("dropoff_ts").cast("long") - F.col("pickup_ts").cast("long")) / 60.0
        )
    )

    # ---- 3) Basic data quality filters (keep it conservative) ----
    # Keep rows that look like real trips
    df = df.filter(F.col("pickup_ts").isNotNull() & F.col("dropoff_ts").isNotNull())
    df = df.filter(F.col("trip_duration_min") > 0)
    df = df.filter(F.col("trip_duration_min") <= 6 * 60)  # drop >6hr outliers
    df = df.filter(F.col("trip_distance") > 0)
    df = df.filter(F.col("total_amount") >= 0)

    # Optional: passenger_count sanity (keep 0? depends; TLC sometimes has 0)
    df = df.filter(F.col("passenger_count").isNull() | (F.col("passenger_count") >= 0))

    # ---- 4) Write Silver partitioned by pickup_date (core lake pattern) ----
    (
        df
        .repartition("pickup_date")
        .write
        .mode("overwrite")
        .partitionBy("pickup_date")
        .parquet(s(SILVER_OUT))
    )

    spark.stop()


if __name__ == "__main__":
    main()
