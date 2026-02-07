from pyspark.sql import SparkSession
from pipelines.novice.layers.medallion_pipe_line import MedallionPipeline

spark = (
    SparkSession.builder
    .appName("NYC Taxi Medallion Pipeline")
    .master("local[*]")
    .getOrCreate()
)

pipeline = MedallionPipeline(spark)
pipeline.run()

spark.stop()
