import time
import smbus2
import bme280
import RPi.GPIO as GPIO
import paho.mqtt.client as mqtt
from influxdb import InfluxDBClient
import json


# Constants
READ_INTERVAL = 5  # Interval in seconds for sensor reading and actuator toggling

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
try:
    # Initialize BME280 sensor
    bus = smbus2.SMBus(I2C_PORT)
    bme_calibration_params = bme280.load_calibration_params(bus, BME_ADD)
except Exception as e:
    print(f"Failed to initialize BME280 sensor: {e}")
    GPIO.cleanup()
    exit(1)


# Function to read and print data from BME280
def read_bme280():
    print(
        "Reads and prints temperature, humidity, and pressure from the BME280 sensor."
    )
    try:
        data = bme280.sample(bus, BME_ADD, bme_calibration_params)
        temperature = data.temperature
        humidity = data.humidity
        pressure = data.pressure
        print(
            f"Temperature: {temperature:.2f} °C, Humidity: {humidity:.2f} %, Pressure: {pressure:.2f} hPa"
        )
    except Exception as e:
        print("Failed to read from BME280:", e)


def toggle_led(state):
    GPIO.output(LED_PIN, state)
    print("LED", "on" if state else "off")


def toggle_relay(state):
    GPIO.output(RELAY_PIN, state)
    print("Relay", "on (Fan should be on)" if state else "off (Fan should be off)")


def toggle_buzzer(state):
    GPIO.output(BUZZER_PIN, state)
    print("Buzzer", "on" if state else "off")


def main():
    try:
        while True:
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

            read_bme280()
            time.sleep(READ_INTERVAL)

    except KeyboardInterrupt:
        print("Stopping program...")
    finally:
        GPIO.cleanup()
        print("GPIO cleanup completed and program stopped.")


if __name__ == "__main__":
    main()