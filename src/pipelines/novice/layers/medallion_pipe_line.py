from pyspark.sql import SparkSession, DataFrame, functions as F
from typing import Tuple
from paths import RAW_NYC_DIR, NOVICE_BRONZE_DIR,NOVICE_SILVER_DIR,NOVICE_GOLD_DIR, s

class MedallionPipeline:
    def __init__(self, spark: SparkSession,
                 bronze_dir=NOVICE_BRONZE_DIR,
                 silver_dir=NOVICE_SILVER_DIR,
                 gold_dir=NOVICE_GOLD_DIR):
        self.spark = spark
        self.bronze_dir = bronze_dir
        self.silver_dir = silver_dir
        self.gold_dir = gold_dir

        self.silver_clean_dir = self.silver_dir / "trips_clean"
        self.silver_invest_dir = self.silver_dir / "investigation_trips"

        self.gold_daily_dir = self.gold_dir / "daily_summary"
        self.gold_vendor_payment_dir = self.gold_dir / "vendor_payment_summary"


    def extract(self) -> DataFrame:
        csv_files = sorted(RAW_NYC_DIR.glob("*.csv"))
        if not csv_files:
            raise FileNotFoundError(f"No CSV files found in {RAW_NYC_DIR}")

        return (
            self.spark.read
            .format("csv")
            .option("header", "true")
            .option("inferSchema", "true")
            .option("mode", "FAILFAST")
            .load([s(p) for p in csv_files])
        )

    def bronze(self, raw_df: DataFrame) -> DataFrame:
        return raw_df

    def bronze_load(self, bronze_df: DataFrame) -> None:
        (
            bronze_df.write
            .mode("overwrite")
            .parquet(s(self.bronze_dir))
        )

    def silver_split(self, bronze_df: DataFrame) -> Tuple[DataFrame, DataFrame]:
        # Parse timestamps (typical NYC taxi format: "yyyy-MM-dd HH:mm:ss")
        df = (
            bronze_df
            .withColumn("pickup_ts", F.to_timestamp("tpep_pickup_datetime"))
            .withColumn("dropoff_ts", F.to_timestamp("tpep_dropoff_datetime"))
            .withColumn(
                "trip_duration_min",
                (F.col("dropoff_ts").cast("long") - F.col("pickup_ts").cast("long")) / 60.0
            )
        )

        raw_reasons = F.array(
            F.when(F.col("tpep_pickup_datetime").isNull(), F.lit("pickup_dt_null")),
            F.when(F.col("tpep_dropoff_datetime").isNull(), F.lit("dropoff_dt_null")),
            F.when(F.col("pickup_ts").isNull(), F.lit("pickup_ts_parse_fail")),
            F.when(F.col("dropoff_ts").isNull(), F.lit("dropoff_ts_parse_fail")),
            F.when(F.col("trip_duration_min").isNull(), F.lit("duration_null")),
            F.when(F.col("trip_duration_min") <= 0, F.lit("duration_le_0")),
            F.when(F.col("trip_duration_min") > 360, F.lit("duration_gt_6hr")),
            F.when(F.col("trip_distance").isNull(), F.lit("trip_distance_null")),
            F.when(F.col("trip_distance") <= 0, F.lit("trip_distance_le_0")),
            F.when(F.col("total_amount").isNull(), F.lit("total_amount_null")),
            F.when(F.col("total_amount") < 0, F.lit("total_amount_lt_0")),
        )

        # remove nulls safely
        reasons = F.filter(raw_reasons, lambda x: x.isNotNull())


        tagged = (
            df
            .withColumn("invalid_reasons", reasons)
            .withColumn("is_valid", F.size(F.col("invalid_reasons")) == 0)
        )

        clean_df = tagged.filter(F.col("is_valid")).drop("invalid_reasons", "is_valid")
        invest_df = tagged.filter(~F.col("is_valid"))

        return clean_df, invest_df

    def load_silver(self, clean_df: DataFrame, invest_df: DataFrame) -> None:
        clean_df.write.mode("overwrite").parquet(s(self.silver_clean_dir))
        invest_df.write.mode("overwrite").parquet(s(self.silver_invest_dir))

        # new: write summary counts
        counts_df = self.invalid_reason_counts(invest_df)
        (counts_df.write
         .mode("overwrite")
         .parquet(s(self.silver_dir / "invalid_reason_counts"))
         )

    def invalid_reason_counts(self, invest_df: DataFrame) -> DataFrame:
        return (
            invest_df
            .select(F.explode("invalid_reasons").alias("invalid_reason"))
            .groupBy("invalid_reason")
            .agg(F.count(F.lit(1)).alias("invalid_count"))
            .orderBy(F.col("invalid_count").desc(), F.col("invalid_reason"))
        )

    def gold_agg(self, clean_df: DataFrame) -> DataFrame:
        daily_df = (
            clean_df
            .withColumn("trip_date", F.to_date("pickup_ts"))
            .groupBy("trip_date")
            .agg(
                F.count(F.lit(1)).alias("trip_count"),
                F.avg("trip_distance").alias("avg_trip_distance"),
                F.avg("trip_duration_min").alias("avg_duration_min"),
                F.sum("total_amount").alias("total_revenue"),
            )
            .orderBy("trip_date")
        )
        return daily_df


    def gold_vendor_payment_summary(self, clean_df: DataFrame) -> DataFrame:
        return (
            clean_df
            .groupBy("VendorID", "payment_type")
            .agg(
                F.count(F.lit(1)).alias("trip_count"),
                F.sum("total_amount").alias("total_revenue"),
                F.avg("total_amount").alias("avg_total_amount"),
                F.avg("trip_distance").alias("avg_trip_distance"),
                F.avg("trip_duration_min").alias("avg_duration_min"),
            )
            .orderBy(F.col("total_revenue").desc())
        )
    def load_gold(self, daily_df: DataFrame, vendor_payment_df: DataFrame) -> None:
        daily_df.write.mode("overwrite").parquet(s(self.gold_daily_dir))
        vendor_payment_df.write.mode("overwrite").parquet(s(self.gold_vendor_payment_dir))


    def run(self) -> None:
        raw_df = self.extract()

        bronze_df = self.bronze(raw_df)
        self.bronze_load(bronze_df)

        clean_df, invest_df = self.silver_split(bronze_df)
        self.load_silver(clean_df, invest_df)

        daily_df = self.gold_agg(clean_df)
        vendor_payment_df = self.gold_vendor_payment_summary(clean_df)
        self.load_gold(daily_df, vendor_payment_df)



