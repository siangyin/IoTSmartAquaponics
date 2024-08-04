import time
import smbus2
import bme280
import RPi.GPIO as GPIO
import paho.mqtt.client as mqtt
from influxdb import InfluxDBClient
import sys


# Constants
READ_INTERVAL = 5  # Interval in seconds for sensor reading and actuator toggling

# Define the MQTT broker address and topics
MQTT_BROKER = "test.mosquitto.org"
MQTT_TOPIC_CONTROL = "tp/eng/grp5/control"
MQTT_TOPIC_DATA = "tp/eng/grp5/data"
MQTT_PORT = 1883

# Constants for InfluxDB
INFLUX_USER = "root"
INFLUX_PW = "root"
INFLUX_DB = "mydb5"
INFLUX_HOST = "localhost"
INFLUX_PORT = 8086
INFLUX_MEASUREMENT = "sensors_data"

# GPIO Pins
LED_PIN = 17
RELAY_PIN = 18
BUZZER_PIN = 25

# Setup GPIO
GPIO.setmode(GPIO.BCM)
GPIO.setup(RELAY_PIN, GPIO.OUT)
GPIO.setup(BUZZER_PIN, GPIO.OUT)
GPIO.setup(LED_PIN, GPIO.OUT)

# Setup for BME280 (using I2C)
I2C_PORT = 1  # I2C port on Raspberry Pi
BME_ADD = 0x76  # Default I2C address for BME280

# Initialize BME280 sensor
try:
    bus = smbus2.SMBus(I2C_PORT)
    bme_calibration_params = bme280.load_calibration_params(bus, BME_ADD)
except Exception as e:
    print(f"Failed to initialize BME280 sensor: {e}")
    GPIO.cleanup()
    sys.exit(1)  # Use sys.exit for clean exit


class SensorData:
    def __init__(self, data, location, sensor_id):
        self.time = time.strftime('%Y-%m-%dT%H:%M:%SZ', time.localtime())
        self.location = location
        self.sensor_id = sensor_id
        self.temperature = round(data.temperature, 2)
        self.humidity = round(data.humidity, 2)
        self.pressure = round(data.pressure, 2)

    def to_influx_payload(self):
        return [
            {
                "time": self.time,
                "measurement": INFLUX_MEASUREMENT,
                "tags": {
                    "location": self.location,
                    "sensor_id": self.sensor_id,
                },
                "fields": {
                    "temperature": self.temperature,
                    "humidity": self.humidity,
                    "pressure": self.pressure,
                },
            }
        ]

    def print_data(self):
        """Output sensor readings in a human-readable format."""
        print(
            f"Temperature: {self.temperature} °C, "
            f"Humidity: {self.humidity} %, "
            f"Pressure: {self.pressure} hPa"
        )

    def to_mqtt_payload(self):
        return (
            f"Location: {self.location}\n"
            f"Temperature: {self.temperature} °C\n"
            f"Humidity: {self.humidity} %\n"
            f"Pressure: {self.pressure} hPa\n"
            f"Time: {self.time}"
        )




# Initialize the InfluxDB client
def create_influxdb_client():
    """Initialize the InfluxDB client."""
    try:
        client = InfluxDBClient(
            host=INFLUX_HOST,
            port=INFLUX_PORT,
            username=INFLUX_USER,
            password=INFLUX_PW,
            database=INFLUX_DB,
        )
        # Ensure the database is created
        # client.create_database(INFLUX_DB)  # Uncomment if you want to ensure the database is created
        return client
    except Exception as e:
        print(f"Failed to create InfluxDB client: {e}")
        sys.exit(1)


def on_message(client, userdata, message):
    """Callback function for handling incoming MQTT messages."""
    print(f"Received message '{message.payload.decode()}' on topic '{message.topic}'")


def start_mqtt():
    """Setup MQTT client, start loop, and return the client instance."""
    client = mqtt.Client()
    print("\nCreated client object at " + time.strftime("%d-%m-%Y  %H:%M:%S", time.localtime()))
    client.on_message = on_message  # Set the callback function for message handling

    try:
        client.connect(MQTT_BROKER, MQTT_PORT)
        client.subscribe(MQTT_TOPIC_CONTROL, qos=1)
        client.loop_start()
        print("--> Connected to broker & subscribed to control topics")
    except Exception as e:
        print(f"Failed to connect to MQTT broker: {e}")
        sys.exit(1)

    return client


def publish_data(client, sensor_data):
    """Publish data to the MQTT broker."""
    payload = sensor_data.to_mqtt_payload()
    try:
        client.publish(MQTT_TOPIC_DATA, payload)
        print(f">> MQTT Data published: {payload}")
    except Exception as e:
        print(f"--Error publishing: {e}")


def read_bme280():
    """Read BME280 sensor data."""
    try:
        # Sample data from the BME280 sensor
        data = bme280.sample(bus, BME_ADD, bme_calibration_params)
        sensor_data = SensorData(data, location="Tank 1", sensor_id="sensor_01")
        sensor_data.print_data()
        return sensor_data
    except Exception as e:
        print("Failed to read from BME280 sensor. Error:", e)
        return None  # Explicitly return None to indicate failure


def toggle_led(state):
    GPIO.output(LED_PIN, state)
    print("LED", "on" if state else "off")


def toggle_relay(state):
    GPIO.output(RELAY_PIN, state)
    print("Relay", "on (Fan should be on)" if state else "off (Fan should be off)")


def toggle_buzzer(state):
    GPIO.output(BUZZER_PIN, state)
    print("Buzzer", "on" if state else "off")

def off_components():
    toggle_led(GPIO.LOW)
    toggle_relay(GPIO.LOW)
    toggle_buzzer(GPIO.LOW)

def main():
    """Main function to monitor system metrics, publish to MQTT, and write to InfluxDB."""
    mqtt_client = start_mqtt()
    influxdb_client = create_influxdb_client()
    try:
        while True:
            # Read BME280 sensor data
            curr_stat = read_bme280()
            if curr_stat:
                # Publish to MQTT
                publish_data(mqtt_client, curr_stat)
                # Write to InfluxDB
                influxdb_client.write_points(curr_stat.to_influx_payload())
                print("Written data to InfluxDB:", curr_stat.to_influx_payload())

            time.sleep(READ_INTERVAL)

            # Toggle components
            toggle_led(GPIO.HIGH)
            time.sleep(READ_INTERVAL)
            toggle_led(GPIO.LOW)
            time.sleep(READ_INTERVAL)

            toggle_relay(GPIO.HIGH)
            time.sleep(READ_INTERVAL)
            toggle_relay(GPIO.LOW)
            time.sleep(READ_INTERVAL)

            toggle_buzzer(GPIO.HIGH)
            time.sleep(READ_INTERVAL)
            toggle_buzzer(GPIO.LOW)
            time.sleep(READ_INTERVAL)

    except KeyboardInterrupt:
        print("Stopping program...")
        off_components()
    finally:
        mqtt_client.loop_stop()
        mqtt_client.disconnect()
        GPIO.cleanup()
        print("GPIO cleanup & MQTT disconnected from broker and program stopped.")


if __name__ == "__main__":
    main()