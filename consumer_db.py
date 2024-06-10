import mysql.connector
from kafka import KafkaConsumer
import json

mydb = mysql.connector.connect(
    host="localhost",
    user="root",
    password="changeme",
    database="dbt_proj"
)

consumer = KafkaConsumer(
    'database',
    bootstrap_servers=['localhost:9092'],
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id='my-group',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

def insert_into_mysql(data):
    cursor = mydb.cursor()
    insert_query = "INSERT INTO car_telemetry (vehicle_id, latitude, longitude, speed, fuel_level, engine_temperature, odometer, engine_status, battery_voltage, front_left, front_right, rear_left, rear_right, owner_name, driving_license_number, timestamp) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
    values = (
        data['vehicle_id'], data['latitude'], data['longitude'], data['speed'],
        data['fuel_level'], data['engine_temperature'], data['odometer'],
        data['engine_status'], data['battery_voltage'], data['front_left'],
        data['front_right'], data['rear_left'], data['rear_right'],
        data['owner_name'], data['driving_license_number'], data['timestamp']
    )
    cursor.execute(insert_query, values)
    mydb.commit()
    cursor.close()

for message in consumer:
    data = message.value
    insert_into_mysql(data)

mydb.close()
