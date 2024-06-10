from kafka import KafkaProducer
import json
import time
from generator import Generator
from decimal import Decimal
from datetime import datetime

class Producer:
    def __init__(self, bootstrap_servers='localhost:9092'):
        self.bootstrap_servers = bootstrap_servers
        self.producer = KafkaProducer(bootstrap_servers=self.bootstrap_servers, value_serializer=lambda x: json.dumps(x).encode('utf-8'))
        self.generator = Generator()

    def convert_to_json_serializable(self, data):
        def decimal_to_float(item):
            if isinstance(item, Decimal):
                return float(item)
            elif isinstance(item, dict):
                return {k: decimal_to_float(v) for k, v in item.items()}
            elif isinstance(item, list):
                return [decimal_to_float(elem) for elem in item]
            else:
                return item

        return decimal_to_float(data)

    def send_data(self, topic, data):
        data = self.convert_to_json_serializable(data)
        self.producer.send(topic, value=data)
        print(f"Sent data to topic '{topic}': {data}")

    def run(self):
        topics = ['database', 'aggregation_car', 'aggregation_general', 'diagnostics']

        for i in range(5000):
            data = self.generator.generate_telemetry_data()
            data["timestamp"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

            db_data = data
            self.send_data('database', db_data)

            agg_tire_data = {
                "timestamp": data["timestamp"],
                "engine_temperature": data["engine_temperature"],
                "battery_voltage": data["battery_voltage"],
                "front_left": data["front_left"],
                "front_right": data["front_right"],
                "rear_left": data["rear_left"],
                "rear_right": data["rear_right"]
            }
            self.send_data('aggregation_car', agg_tire_data)

            agg_general_data = {
                "timestamp": data["timestamp"],
                "speed": data["speed"],
                "fuel_level": data["fuel_level"],
                "odometer": data["odometer"],
            }
            self.send_data('aggregation_general', agg_general_data)

            diag_data = {
                "timestamp": data["timestamp"],
                "speed": data["speed"],
                "fuel_level": data["fuel_level"],
                "engine_status": data["engine_status"],
                "front_left": data["front_left"],
                "front_right": data["front_right"],
                "rear_left": data["rear_left"],
                "rear_right": data["rear_right"]
            }
            self.send_data('diagnostics', diag_data)

            time.sleep(0.5)

        self.producer.close()

if __name__ == "__main__":
    producer = Producer()
    producer.run()
