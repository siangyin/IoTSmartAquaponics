import time
import smbus2
import bme280
import RPi.GPIO as GPIO
import paho.mqtt.client as mqtt
from influxdb import InfluxDBClient
import json
import psutil

# Constants
READ_INTERVAL = 5  # Interval in seconds for sensor reading and actuator toggling

# Define the MQTT broker address and topics
MQTT_BROKER = "test.mosquitto.org"
MQTT_TOPIC_CONTROL = "tp/eng/grp5/control"
MQTT_TOPIC_DATA = "tp/eng/grp5/data"
MQTT_PORT = 1883

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
    exit(1)


def on_message(client, userdata, message):
    """Callback function for handling incoming messages."""
    print(f"Received message '{message.payload.decode()}' on topic '{message.topic}'")


def start_mqtt():
    """Setup MQTT client, start loop, and return the client instance."""
    client = mqtt.Client()
    print(
        "\nCreated client object at "
        + time.strftime("%d-%m-%Y  %H:%M:%S", time.localtime())
    )
    client.on_message = on_message  # Set the callback function for message handling

    try:
        client.connect(MQTT_BROKER, MQTT_PORT)
        client.subscribe(MQTT_TOPIC_CONTROL, qos=1)
        client.loop_start()
        print("--> connected to broker & Subscribed to control topics")
    except Exception as e:
        print(f"Failed to connect to MQTT broker: {e}")
        exit(1)

    return client


def publish_data(client, data):
    """Publish data to the MQTT broker."""
    payload = (
        f"Location: Tank 1\n"
        f"Temperature: {data['temperature']} °C\n"
        f"Humidity: {data['humidity']} %\n"
        f"Pressure: {data['pressure']} hPa\n"
        f"Time: {time.strftime('%d-%m-%Y  %H:%M:%S', time.localtime())}"
    )
    try:
        client.publish(MQTT_TOPIC_DATA, payload)
        print(f"Data published: {payload}")
    except Exception as e:
        print(f"--error publishing: {e}")


def read_bme280():
    """Read and print BME280 sensor data."""
    try:
        # Sample data from the BME280 sensor
        data = bme280.sample(bus, BME_ADD, bme_calibration_params)

        # Debugging: Print raw data
        print(f"BME280 Raw Data: {data}")

        # Round the sensor data for clarity
        sensor_data = {
            "temperature": round(data.temperature, 2),
            "humidity": round(data.humidity, 2),
            "pressure": round(data.pressure, 2),
        }

        # Debugging: Print formatted sensor data
        print(f"Formatted Sensor Data: {sensor_data}")

        # Output sensor readings in a human-readable format
        print(
            f"Temperature: {sensor_data['temperature']} °C, "
            f"Humidity: {sensor_data['humidity']} %, "
            f"Pressure: {sensor_data['pressure']} hPa"
        )

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
    #  Main function to monitor system metrics and publish to MQTT
    mqtt_client = start_mqtt()

    try:
        while True:
            curr_stat = read_bme280()
            if curr_stat:
                publish_data(mqtt_client, curr_stat)
            time.sleep(READ_INTERVAL)
            
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
