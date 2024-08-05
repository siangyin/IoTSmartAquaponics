import time
import smbus2
import bme280
import RPi.GPIO as GPIO
import paho.mqtt.client as mqtt
from influxdb import InfluxDBClient
import sys

# Constants
READ_INTERVAL = 5  # Interval in seconds for sensor reading and actuator toggling
TEMP_THRESHOLD = 30  # Temperature threshold for alerts
NORMAL_TEMPERATURE = 'N'
HIGH_TEMPERATURE = 'H'

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
    print(f"[ERROR]\tFailed to initialize BME280 sensor: {e}")
    GPIO.cleanup()
    sys.exit(1)  # Use sys.exit for clean exit


class SensorData:
    """
    A class to encapsulate sensor data and provide methods for formatting and evaluation.
    """
    def __init__(self, data, location, sensor_id):
        self.time = time.strftime('%Y-%m-%dT%H:%M:%SZ', time.localtime())
        self.location = location
        self.sensor_id = sensor_id
        self.temperature = round(data.temperature, 2)
        self.humidity = round(data.humidity, 2)
        self.pressure = round(data.pressure, 2)

    def to_influx_payload(self):
        """
        Convert sensor data to the payload format expected by InfluxDB.
        """
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
    
        
    def to_mqtt_payload(self):
        """
        Convert sensor data to a string payload for MQTT transmission.
        """
        return (
            f"Location: {self.location}\n"
            f"Temperature: {self.temperature} °C\n"
            f"Humidity: {self.humidity} %\n"
            f"Pressure: {self.pressure} hPa\n"
            f"Time: {self.time}"
        )
    
    def get_temperature_level(self):
        """
        Determine the temperature level based on a predefined threshold.
        """
        if self.temperature > TEMP_THRESHOLD:
            return HIGH_TEMPERATURE
        else:
            return NORMAL_TEMPERATURE

    def print_data(self):
        """
        Print sensor readings in a human-readable format.
        """
        print(
            f"[INFO]\t Sensors Data:"
            f"Temperature: {self.temperature} °C, "
            f"Humidity: {self.humidity} %, "
            f"Pressure: {self.pressure} hPa"
        )


# Initialize the InfluxDB client
def create_influxdb_client():
    """
    Initialize and return the InfluxDB client.
    """
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
        print(f"[ERROR]\tFailed to create InfluxDB client: {e}")
        sys.exit(1)


def on_message(client, userdata, message):
    """
    Callback function for handling incoming MQTT messages.
    Controls the GPIO based on received commands.
    """
    print(f"[INFO]\tnReceived message '{message.payload.decode()}' on topic '{message.topic}'")
    # Handle message to control GPIO
    if message.topic == MQTT_TOPIC_CONTROL:
        command = message.payload.decode().upper()
        if command == "LED ON":
            toggle_led(GPIO.HIGH)
        elif command == "LED OFF":
            toggle_led(GPIO.LOW)
        elif command == "RELAY ON" or command == "FAN ON":
            toggle_fan(GPIO.HIGH)
        elif command == "RELAY OFF" or command == "FAN OFF":
            toggle_fan(GPIO.LOW)
        elif command == "BUZZER ON":
            toggle_buzzer(GPIO.HIGH)
        elif command == "BUZZER OFF":
            toggle_buzzer(GPIO.LOW)


def start_mqtt():
    """
    Initialize MQTT client, connect to broker, and subscribe to control topics. 
    Handles setup for communication with the MQTT broker.
    """
    client = mqtt.Client()
    client.on_message = on_message  # Set the callback function for message handling

    try:
        client.connect(MQTT_BROKER, MQTT_PORT)
        client.subscribe(MQTT_TOPIC_CONTROL, qos=1)
        client.loop_start()
        print("[INFO]\tConnected to broker & subscribed to control topics")
    except Exception as e:
        print(f"[ERROR]\tFailed to connect to MQTT broker: {e}")
        sys.exit(1)

    return client


def publish_data(client, sensor_data):
    """
    Publish sensor data to the MQTT broker.
    """
    try:
        client.publish(MQTT_TOPIC_DATA, sensor_data.to_mqtt_payload())
        print("[INFO]\tPublished sensor data to MQTT")
    except Exception as e:
        print(f"[ERROR]\tFailed to publish data to MQTT: {e}")


def read_bme280():
    """
    Read and return BME280 sensor data.
    """
    try:
        # Sample data from the BME280 sensor
        data = bme280.sample(bus, BME_ADD, bme_calibration_params)
        return SensorData(data, location="tank 1", sensor_id="sensor 1")
    except Exception as e:
        print("[ERROR]\tFailed to read from BME280 sensor. Error:", e)
        return None  # Explicitly return None to indicate failure


def toggle_led(state):
    GPIO.output(LED_PIN, state)

def toggle_fan(state):
    GPIO.output(RELAY_PIN, state)


def toggle_buzzer(state):
    GPIO.output(BUZZER_PIN, state)

def off_components():
    """
    Turn off all components.
    """
    toggle_led(GPIO.LOW)
    toggle_fan(GPIO.LOW)
    toggle_buzzer(GPIO.LOW)


def start_alert():
    """
    Activate the buzzer for an alert sequence.
    """
    toggle_buzzer(GPIO.HIGH)
    time.sleep(2)
    toggle_buzzer(GPIO.LOW)
    time.sleep(1)
    toggle_buzzer(GPIO.HIGH)
    time.sleep(2)
    toggle_buzzer(GPIO.LOW)


def main():
    """
    Main function to monitor temperature, publish data, and handle alerts.
    Coordinates sensor reading, data publishing, and alerting.
    """
    # Start the MQTT client
    mqtt_client = start_mqtt()
    # Initialize the InfluxDB client
    influxdb_client = create_influxdb_client()
    
    prev_temp_lvl = NORMAL_TEMPERATURE
    curr_temp_lvl = NORMAL_TEMPERATURE

    try:
        while True:
            # Read BME280 sensor data
            curr_stat = read_bme280()
            
            if curr_stat:
                curr_stat.print_data()
                # Publish to MQTT
                publish_data(mqtt_client, curr_stat)
                # Write to InfluxDB
                influxdb_client.write_points(curr_stat.to_influx_payload())
                print("[INFO]\tPublished sensor data to InfluxDB")
                
                prev_temp_lvl = curr_temp_lvl  # Update previous temperature level
                curr_temp_lvl = curr_stat.get_temperature_level()
                    
                # temperature changes
                if prev_temp_lvl == NORMAL_TEMPERATURE and curr_temp_lvl == HIGH_TEMPERATURE:
                    print(f"[ALERT]\tTemperature Exceeded Threshold ({TEMP_THRESHOLD}°C)!")
                    toggle_led(GPIO.HIGH)
                    toggle_fan(GPIO.HIGH)
                    start_alert()
                elif prev_temp_lvl == HIGH_TEMPERATURE and curr_temp_lvl == NORMAL_TEMPERATURE:
                    print(f"[INFO]\tTemperature is now within safe range")
                    toggle_led(GPIO.LOW)
                    toggle_fan(GPIO.LOW)
                    time.sleep(READ_INTERVAL)
                else:
                    time.sleep(READ_INTERVAL)
                    

    except KeyboardInterrupt:
        print("[INFO]\tStopping program...")
        off_components()
    finally:
        mqtt_client.loop_stop()
        mqtt_client.disconnect()
        GPIO.cleanup()
        print("[INFO]\tGPIO cleanup & MQTT disconnected from broker and program stopped.")


if __name__ == "__main__":
    main()
