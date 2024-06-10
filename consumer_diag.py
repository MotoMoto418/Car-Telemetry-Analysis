from pyspark.sql import SparkSession
from pyspark.sql.types import StringType, StructField, StructType, ArrayType, LongType, FloatType, IntegerType, TimestampType
from pyspark.sql.functions import from_json, avg
import pyspark.sql.functions as F



spark = SparkSession.builder \
    .appName("Diagnoser") \
    .config("spark.streaming.stopGracefullyOnShutdown", "true") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

db_schema = StructType([
    StructField('timestamp', TimestampType(), True),
    StructField('speed', FloatType(), True),
    StructField('fuel_level', FloatType(), True),
    StructField('engine_status', StringType(), True),
    StructField('front_left', IntegerType(), True),
    StructField('front_right', IntegerType(), True),
    StructField('rear_left', IntegerType(), True),
    StructField('rear_right', IntegerType(), True),
])


agg_df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "diagnostics") \
    .load() \

parsed_df = agg_df.select(F.from_json(F.col("value").cast("string"), db_schema).alias("json_data")) \
    .select("json_data.*") \

filtered_df = parsed_df.filter(F.col("engine_status") == "NOT OK")

windowed_avg_df = filtered_df \
    .withWatermark("timestamp", "5 seconds") \
    .groupBy(F.window("timestamp", "10 seconds")) \
    .agg(avg("speed").alias("avg_speed"),
         avg("fuel_level").alias("avg_fuel_level"),
         avg("front_left").alias("avg_front_left"),
         avg("front_right").alias("avg_front_right"),
         avg("rear_left").alias("avg_rear_left"),
         avg("rear_right").alias("avg_rear_right"))

query = windowed_avg_df \
    .writeStream \
    .outputMode("append") \
    .format("console") \
    .start()

query.awaitTermination()

spark.stop()