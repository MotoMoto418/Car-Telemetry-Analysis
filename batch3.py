from pyspark.sql import SparkSession
from pyspark.sql.types import StringType, StructField, StructType, ArrayType, LongType, FloatType, IntegerType, TimestampType
from pyspark.sql.functions import from_json, avg
import pyspark.sql.functions as F
from time import time

class TireAggregator:
    def __init__(self):
        self.spark = SparkSession.builder \
            .appName("Streaming Batch Mode") \
            .config("spark.streaming.stopGracefullyOnShutdown", "true") \
            .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1") \
            .getOrCreate()

        self.spark.sparkContext.setLogLevel("ERROR")

        batch_schema = StructType([
            StructField('vehicle_id', StringType(), True),
            StructField('timestamp', TimestampType(), True),
            StructField('latitude', StringType(), True),
            StructField('longitude', StringType(), True),
            StructField('speed', FloatType(), True),
            StructField('fuel_level', FloatType(), True),
            StructField('engine_temperature', FloatType(), True),
            StructField('odometer', IntegerType(), True),
            StructField('engine_status', StringType(), True),
            StructField('battery_voltage', FloatType(), True),
            StructField('front_left', IntegerType(), True),
            StructField('front_right', IntegerType(), True),
            StructField('rear_left', IntegerType(), True),
            StructField('rear_right', IntegerType(), True),
            StructField('owner_name', StringType(), True),
            StructField('driving_license_number', StringType(), True),
        ])

    def run(self):
        start = time()

        agg_df = self.spark \
            .readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", "localhost:9092") \
            .option("subscribe", "database") \
            .load()

        parsed_df = agg_df.select(F.from_json(F.col("value").cast("string"), self.tire_agg_schema).alias("json_data")) \
            .select("json_data.*") \

        windowed_avg_df = parsed_df \
            .withWatermark("timestamp", "11 minutes") \
            .groupBy(F.window("timestamp", "10 minutes")) \
            .agg(avg("engine_temperature").alias("avg_engine_temperature"),
                 avg("battery_voltage").alias("avg_battery_voltage"),
                 avg("front_left").alias("avg_front_left"),
                 avg("front_right").alias("avg_front_right"),
                 avg("rear_left").alias("avg_rear_left"),
                 avg("rear_right").alias("avg_rear_right"))
        
        end = time()

        with open("metrics.txt", "a") as f:
            f.write(f"consumer_agg_car.py: {end - start}s")

        query = windowed_avg_df \
            .writeStream \
            .outputMode("append") \
            .format("console") \
            .start()

        query.awaitTermination()

        self.spark.stop()

if __name__ == "__main__":
    tire_aggregator = TireAggregator()
    tire_aggregator.run()
