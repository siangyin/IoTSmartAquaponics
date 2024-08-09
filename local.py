import argparse
from influxdb import InfluxDBClient
from influxdb.client import InfluxDBClientError
import time
import paho.mqtt.client as mqtt
import json
import sys

# Define the MQTT broker address and topics
MQTT_BROKER = "test.mosquitto.org"
MQTT_TOPIC_CONTROL = "tp/eng/grp5/control"
MQTT_TOPIC_DATA = "tp/eng/grp5/data"
MQTT_PORT = 1883

# Constants
READ_INTERVAL = 5  # Interval in seconds for sensor reading and actuator toggling
TEMP_THRESHOLD = 30  # Temperature threshold for alerts
NORMAL_TEMPERATURE = 'N'
HIGH_TEMPERATURE = 'H'

# Constants for InfluxDB
INFLUX_USER = "root"
INFLUX_PW = "root"
INFLUX_DB = "mydb5"
INFLUX_HOST = "localhost"
INFLUX_PORT = 8086
INFLUX_MEASUREMENT = "sensors_data"

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


def on_message(client, userdata, message):
    """
    Callback function for handling incoming MQTT messages.

    """
    try:
        mqtt_data = json.loads(message.payload.decode("utf-8"))
        print(mqtt_data)
        print(f"[INFO]\tReceived message '{message.payload.decode()}' on topic '{message.topic}'")
    except Exception as e:
        print(f"[ERROR] Processing data: {str(e).upper()}")




def start_mqtt():
    """
    Initialize MQTT client, connect to broker, and subscribe to control topics.
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


# Main function
def main():
    mqtt_client = start_mqtt()
    influxdb_client = create_influxdb_client()
    
    
    try:
        # Keep the script running to maintain MQTT connection
        while True:
            print("Run program")
 
    except KeyboardInterrupt:
        print("[INFO]\tStopping program...")
 
    finally:
        # Clean up resources
        if influxdb_client:
            influxdb_client.close()
        mqtt_client.loop_stop()
        mqtt_client.disconnect()
        print("[INFO]\tMQTT disconnected from broker and program stopped.")

 
if __name__ == "__main__":
    main()