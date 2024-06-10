from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg
from time import time

spark = SparkSession.builder \
    .appName("Batch Processing") \
    .config("spark.jars", "mysql-connector-java-5.1.46.jar") \
    .getOrCreate()

jdbc_url = "jdbc:mysql://localhost:3306/dbt_proj?useSSL=false"

connection_properties = {
    "user": "root",
    "password": "changeme",
    "driver": "com.mysql.jdbc.Driver"
}

table_name = "car_telemetry"

df = spark.read.jdbc(url=jdbc_url, table=table_name, properties=connection_properties)

batch_size = 300

total_batches = (df.count() + batch_size - 1) // batch_size

for batch_index in range(total_batches):
    start = time()
    start_index = batch_index * batch_size
    end_index = min((batch_index + 1) * batch_size, df.count())
    
    batch_df = df.select("*").orderBy("id").filter((col("id") >= start_index) & (col("id") < end_index))
    
    speed_fuel_count = batch_df.filter((batch_df["speed"] > 100) & (batch_df["fuel_level"] > 15)).count()

    avg_not_ok = batch_df.filter(batch_df["engine_status"] == "NOT OK").agg(
        avg("speed").alias("avg_speed"),
        avg("fuel_level").alias("avg_fuel_level"),
        avg("front_left").alias("avg_front_left"),
        avg("front_right").alias("avg_front_right"),
        avg("rear_left").alias("avg_rear_left"),
        avg("rear_right").alias("avg_rear_right")
    )

    avg_all = batch_df.agg(
        avg("front_left").alias("avg_front_left_all"),
        avg("front_right").alias("avg_front_right_all"),
        avg("rear_left").alias("avg_rear_left_all"),
        avg("rear_right").alias("avg_rear_right_all")
    )

    avg_engine_battery = batch_df.agg(
        avg("engine_temperature").alias("avg_engine_temperature"),
        avg("battery_voltage").alias("avg_battery_voltage")
    )

    print(f"Batch {batch_index + 1}:")
    print("Count of vehicles with speed > 100 and fuel level > 15:", speed_fuel_count)
    print("Average values for vehicles with engine status 'NOT OK':")
    avg_not_ok.show()
    print("Average values for all vehicles:")
    avg_all.show()
    print("Average engine temperature and battery voltage for all vehicles:")
    avg_engine_battery.show()

    end = time()

    with open("metrics.txt", "a") as f:
        f.write(f"batch # {batch_index}, time: {end - start} s\n")

# with open("metrics.txt", "a+") as f:
    


spark.stop()
