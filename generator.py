from faker import Faker
import random

class Generator:
    def __init__(self):
        self.fake = Faker()

    def generate_numeric_string(self, length):
        return ''.join(random.choices('0123456789', k=length))

    def generate_vehicle_id(self):
        return self.generate_numeric_string(4)

    def generate_speed(self):
        return round(random.uniform(10, 140), 1)

    def generate_fuel_level(self):
        return round(random.uniform(5, 45), 1)

    def generate_engine_temperature(self):
        return round(random.uniform(40, 105), 1)

    def generate_odometer(self):
        return random.randint(0, 99999)

    def generate_engine_status(self):
        return random.choice(["OK", "NOT OK"])

    def generate_battery_voltage(self):
        return round(random.uniform(6, 14.4), 1)

    def generate_tire_pressure(self):
        return random.randint(25, 35)

    def generate_telemetry_data(self):
        data = {
            "vehicle_id": self.generate_vehicle_id(),
            "latitude": str(self.fake.latitude()),
            "longitude": str(self.fake.longitude()),
            "speed": self.generate_speed(),
            "fuel_level": self.generate_fuel_level(),
            "engine_temperature": self.generate_engine_temperature(),
            "odometer": self.generate_odometer(),
            "engine_status": self.generate_engine_status(),
            "battery_voltage": self.generate_battery_voltage(),
            "front_left": self.generate_tire_pressure(),
            "front_right": self.generate_tire_pressure(),
            "rear_left": self.generate_tire_pressure(),
            "rear_right": self.generate_tire_pressure(),
            "owner_name": self.fake.name(),
            "driving_license_number": self.generate_numeric_string(8)
        }
        return data

if __name__ == "__main__":
    generator = Generator()
    sample_telemetry_data = generator.generate_telemetry_data()
    print(sample_telemetry_data)
