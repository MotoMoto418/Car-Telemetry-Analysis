from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, count
from time import time


spark = SparkSession.builder \
    .appName("Batch Processing") \
    .config("spark.streaming.stopGracefullyOnShutdown", "true") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1") \
    .config("spark.jars", "mysql-connector-java-5.1.46.jar") \
    .config("spark.executor.extraClassPath", "mysql-connector-java-5.1.46.jar") \
    .config("spark.executor.extraLibrary", "mysql-connector-java-5.1.46.jar") \
    .config("spark.driver.extraClassPath", "mysql-connector-java-5.1.46.jar") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

jdbc_url = "jdbc:mysql://localhost:3306/dbt_proj?useSSL=false"

connection_properties = {
    "user": "root",
    "password": "changeme",
    "driver": "com.mysql.jdbc.Driver"
}

table_name = "car_telemetry"

start = time()

df = spark.read.jdbc(url=jdbc_url, table=table_name, properties=connection_properties)

speed_fuel_count = df.filter((df["speed"] > 100) & (df["fuel_level"] > 15)).count()

avg_not_ok = df.filter(df["engine_status"] == "NOT OK").agg(
    avg("speed").alias("avg_speed"),
    avg("fuel_level").alias("avg_fuel_level"),
    avg("front_left").alias("avg_front_left"),
    avg("front_right").alias("avg_front_right"),
    avg("rear_left").alias("avg_rear_left"),
    avg("rear_right").alias("avg_rear_right")
)

avg_all = df.agg(
    avg("front_left").alias("avg_front_left_all"),
    avg("front_right").alias("avg_front_right_all"),
    avg("rear_left").alias("avg_rear_left_all"),
    avg("rear_right").alias("avg_rear_right_all")
)

avg_engine_battery = df.agg(
    avg("engine_temperature").alias("avg_engine_temperature"),
    avg("battery_voltage").alias("avg_battery_voltage")
)

print("Count of vehicles with speed > 100 and fuel level > 15:", speed_fuel_count)
print("Average values for vehicles with engine status 'NOT OK':")
avg_not_ok.show()
print("Average values for all vehicles:")
avg_all.show()
print("Average engine temperature and battery voltage for all vehicles:")
avg_engine_battery.show()

end = time()

with open("metrics.txt", "a") as f:
        f.write(f"full DB , time: {end - start} s\n")

spark.stop()
