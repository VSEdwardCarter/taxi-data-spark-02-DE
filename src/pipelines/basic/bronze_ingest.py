from pyspark.sql import functions as F
from src.spark import build_spark
from src.paths import RAW_NYC_DIR, BRONZE_DIR, s

input_raw = RAW_NYC_DIR / "yellow_tripdata_2016-03.csv"
output_brnze = BRONZE_DIR / "bronze_yellow_tripdata"

def main():
    spark = build_spark("2016-3-bronze-ingest") # <- initialize spark session app name is description of what is happening

    df = (
        spark.read
        .option("header", "true")
        .option("multiLine", "false")
        .option("escape", "\"")
        .option("mode", "PERMISSIVE")
        .csv(s(input_raw))
    )

    df = df.withColumn("_source_file", F.input_file_name())

    (df.write.mode("overwrite").parquet(s(output_brnze)))

    spark.stop()

if __name__ == "__main__":
    main()