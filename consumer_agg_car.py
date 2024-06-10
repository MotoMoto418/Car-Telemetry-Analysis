from pyspark.sql import SparkSession
from pyspark.sql.types import StringType, StructField, StructType, ArrayType, LongType, FloatType, IntegerType, TimestampType
from pyspark.sql.functions import from_json, avg
import pyspark.sql.functions as F
from time import time

class TireAggregator:
    def __init__(self):
        self.spark = SparkSession.builder \
            .appName("Tire Aggregator") \
            .config("spark.streaming.stopGracefullyOnShutdown", "true") \
            .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1") \
            .getOrCreate()

        self.spark.sparkContext.setLogLevel("ERROR")

        self.tire_agg_schema = StructType([
            StructField('timestamp', TimestampType(), True),
            StructField('engine_temperature', FloatType(), True),
            StructField('battery_voltage', FloatType(), True),
            StructField('front_left', IntegerType(), True),
            StructField('front_right', IntegerType(), True),
            StructField('rear_left', IntegerType(), True),
            StructField('rear_right', IntegerType(), True),
        ])

    def run(self):
        agg_df = self.spark \
            .readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", "localhost:9092") \
            .option("subscribe", "aggregation_car") \
            .load()

        parsed_df = agg_df.select(F.from_json(F.col("value").cast("string"), self.tire_agg_schema).alias("json_data")) \
            .select("json_data.*") \
            
        start = time()
        print(f"\n\nstart: {start}\n\n")

        windowed_avg_df = parsed_df \
            .withWatermark("timestamp", "5 seconds") \
            .groupBy(F.window("timestamp", "10 seconds")) \
            .agg(avg("engine_temperature").alias("avg_engine_temperature"),
                 avg("battery_voltage").alias("avg_battery_voltage"),
                 avg("front_left").alias("avg_front_left"),
                 avg("front_right").alias("avg_front_right"),
                 avg("rear_left").alias("avg_rear_left"),
                 avg("rear_right").alias("avg_rear_right"))
        
        end = time()
        print(f"\n\nend: {end}\n\n")

        with open("metrics.txt", "a") as f:
            f.write(f"consumer_agg_car.py: {end - start} s\n")

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
