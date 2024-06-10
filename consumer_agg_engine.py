from pyspark.sql import SparkSession
from pyspark.sql.types import StringType, StructField, StructType, ArrayType, LongType, FloatType, IntegerType, TimestampType
from pyspark.sql.functions import from_json, avg
import pyspark.sql.functions as F

class EngineAggregator:
    def __init__(self):
        self.spark = SparkSession.builder \
            .appName("Engine Aggregator") \
            .config("spark.streaming.stopGracefullyOnShutdown", "true") \
            .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1") \
            .getOrCreate()

        self.spark.sparkContext.setLogLevel("ERROR")

        self.engine_agg_schema = StructType([
            StructField('timestamp', TimestampType(), True),
            StructField('engine_temperature', FloatType(), True),
            StructField('battery_voltage', FloatType(), True),
        ])

    def run(self):
        agg_df = self.spark \
            .readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", "localhost:9092") \
            .option("subscribe", "aggregation_engine") \
            .load()

        parsed_df = agg_df.select(F.from_json(F.col("value").cast("string"), self.engine_agg_schema).alias("json_data")) \
            .select("json_data.*") \

        windowed_avg_df = parsed_df \
            .withWatermark("timestamp", "5 seconds") \
            .groupBy(F.window("timestamp", "10 seconds")) \
            .agg(avg("engine_temperature").alias("avg_engine_temperature"),
                 avg("battery_voltage").alias("avg_battery_voltage"))

        query = windowed_avg_df \
            .writeStream \
            .outputMode("append") \
            .format("console") \
            .start()

        query.awaitTermination()

        self.spark.stop()

if __name__ == "__main__":
    engine_aggregator = EngineAggregator()
    engine_aggregator.run()
