from pyspark.sql import SparkSession
from pyspark.sql.types import StringType, StructField, StructType, ArrayType, LongType, FloatType, IntegerType, TimestampType
from pyspark.sql.functions import from_json, avg
import pyspark.sql.functions as F

class GeneralAggregator:
    def __init__(self):
        self.spark = SparkSession.builder \
            .appName("General Aggregator") \
            .config("spark.streaming.stopGracefullyOnShutdown", "true") \
            .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1") \
            .getOrCreate()

        self.spark.sparkContext.setLogLevel("ERROR")

        self.general_agg_schema = StructType([
            StructField('timestamp', TimestampType(), True),
            StructField('speed', FloatType(), True),
            StructField('fuel_level', FloatType(), True),
            StructField('odometer', IntegerType(), True),
        ])

    def run(self):
        agg_df = self.spark \
            .readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", "localhost:9092") \
            .option("subscribe", "aggregation_general") \
            .load()

        parsed_df = agg_df.select(F.from_json(F.col("value").cast("string"), self.general_agg_schema).alias("json_data")) \
            .select("json_data.*") \

        windowed_df = parsed_df.withWatermark("timestamp", "5 seconds")

        filtered_df = windowed_df.filter((F.col("speed") > 100) & (F.col("fuel_level") > 15))

        num_vehicles_query = filtered_df \
            .groupBy(F.window("timestamp", "10 seconds")) \
            .count() \
            .writeStream \
            .outputMode("complete") \
            .format("console") \
            .start()

        num_vehicles_query.awaitTermination()

        self.spark.stop()

if __name__ == "__main__":
    aggregator = GeneralAggregator()
    aggregator.run()
