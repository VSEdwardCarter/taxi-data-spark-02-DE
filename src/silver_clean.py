from pyspark.sql import functions as F

import spark
from src.spark import build_spark
from src.paths import BRONZE_DIR, SILVER_INVEST_DIR, SILVER_CLEAN_DIR, s

input_bronze = BRONZE_DIR / "bronze_yellow_tripdata"
clean_output_silver = SILVER_CLEAN_DIR / "clean_silver_yellow_tripdata"
invest_output_silver = SILVER_INVEST_DIR / "invest_silver_yellow_tripdata"

# ========
# Deal with time
# ========

def string_to_ts(columnName : str):
    return F.to_timestamp(F.col(columnName), "yyyy-MM-dd HH:mm:ss")


def main():
    spark = build_spark("2016-3-silver-clean")  # <- start spark session

    df =  spark.read.parquet(s(input_bronze)) # <- create a DF from the bronze data

    # =======
    # Standardize Types (cast strings from CSV to real numeric types)
        ###  ___INT___
        # |-- VendorID: string (nullable = true)
        # |-- passenger_count: string (nullable = true)
        # |-- trip_distance: string (nullable = true)
        # |-- RatecodeID: string (nullable = true)
        # |-- payment_type: string (nullable = true)
        # |-- fare_amount: string (nullable = true)
        # |-- extra: string (nullable = true)
        # |-- mta_tax: string (nullable = true)
        # |-- tip_amount: string (nullable = true)
        # |-- tolls_amount: string (nullable = true)
        # |-- improvement_surcharge: string (nullable = true)
        # |-- total_amount: string (nullable = true)

        ### ___TIMESTAM___
        # |-- tpep_pickup_datetime: string (nullable = true)  <- TimeStamp
        # |-- tpep_dropoff_datetime: string (nullable = true)  <- TimeStamp

        ### ___STRING___
        # |-- _source_file: string (nullable = true)
        # |-- store_and_fwd_flag: string (nullable = true)

        ### ___WKT___
        # |-- dropoff_longitude: string (nullable = true)
        # |-- dropoff_latitude: string (nullable = true)
        # |-- pickup_longitude: string (nullable = true)
        # |-- pickup_latitude: string (nullable = true)
    # =======

    df = (
        df
        .withColumn("VendorID", F.col("VendorId").cast("int"))
        .withColumn("passenger_count", F.col("passenger_count").cast("int"))
        .withColumn("trip_distance", F.col("trip_distance").cast("int"))
        .withColumn("RatecodeID", F.col("RatecodeID").cast("int"))
        .withColumn("payment_type", F.col("payment_type").cast("int"))
        .withColumn("fare_amount", F.col("fare_amount").cast("int"))
        .withColumn("extra", F.col("extra").cast("int"))
        .withColumn("mta_tax", F.col("mta_tax").cast("int"))
        .withColumn("tip_amount", F.col("tip_amount").cast("int"))
        .withColumn("tolls_amount", F.col("tolls_amount").cast("int"))
        .withColumn("improvement_surcharge", F.col("improvement_surcharge").cast("int"))
        .withColumn("total_amount", F.col("total_amount").cast("int"))
    )

    df = (
        df
        .withColumn("pickup_ts", string_to_ts("tpep_pickup_datetime"))
        .withColumn("dropoff_ts", string_to_ts("tpep_dropoff_datetime"))
        .withColumn("pickup_date", F.to_date(F.col("pickup_ts")))
        .withColumn(
            "trip_duration_min",
            (F.col("dropoff_ts").cast("long") - F.col("pickup_ts").cast("long"))/60.0
        )
    )

    investigation_trips = (
        df
        .withColumn("has_valid_timestamps", F.col("pickup_ts").isNotNull() & F.col("dropoff_ts").isNotNull())
        .withColumn("has_positive_duration", F.col("trip_duration_min") > 0)
        .withColumn("duration_within_6h", F.col("trip_duration_min") <= 6 * 60)
        .withColumn("has_positive_distance", F.col("trip_distance") > 0)
        .withColumn("has_non_negative_amount", F.col("total_amount") >= 0)
        .withColumn(
            "is_valid_trip",
            F.col("has_valid_timestamps")
            & F.col("has_positive_duration")
            & F.col("duration_within_6h")
            & F.col("has_positive_distance")
            & F.col("has_non_negative_amount")
        )
        .withColumn(
            "invalid_reason",
            F.when(~F.col("has_valid_timestamps"), F.lit("missing_timestamp"))
            .when(~F.col("has_positive_duration"), F.lit("non_positive_duration"))
            .when(~F.col("duration_within_6h"), F.lit("duration_gt_6h"))
            .when(~F.col("has_positive_distance"), F.lit("non_positive_distance"))
            .when(~F.col("has_non_negative_amount"), F.lit("negative_amount"))
            .otherwise(F.lit(None))
        )
    )
    (
        investigation_trips
        .repartition("pickup_date")
        .write.mode("overwrite")
        .partitionBy("pickup_date")
        .parquet(s(invest_output_silver))
    )

    trips_clean = investigation_trips.filter(F.col("is_valid_trip"))
    (
        trips_clean
        .repartition("pickup_date")
        .write.mode("overwrite")
        .partitionBy("pickup_date")
        .parquet(s(clean_output_silver))
    )

    spark.stop()

if __name__ =="__main__":
    main()



