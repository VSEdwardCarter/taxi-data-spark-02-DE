from pyspark.sql import functions as F
from src.spark import build_spark
from src.paths import RAW_NYC_DIR, BRONZE_DIR, s

INFILE = RAW_NYC_DIR / "yellow_tripdata_2016-01.csv"
OUTDIR = BRONZE_DIR / "yellow_tripdata_2016-01"

def main():
    spark = build_spark("nyc-taxi-bronze-ingest")

    df = (
        spark.read
        .option("header", "true")
        .option("multiLine", "false")
        .option("escape", "\"")
        .csv(s(INFILE))
    )

    # Bronze = raw + provenance
    df = df.withColumn("_source_file", F.input_file_name())

    (
        df.write
        .mode("overwrite")
        .parquet(s(OUTDIR))
    )

    spark.stop()

if __name__ == "__main__":
    main()
