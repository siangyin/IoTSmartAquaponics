import smbus2
import bme280
import time
import paho.mqtt.client as mqtt
import RPi.GPIO as GPIO
import json
 
# Configuration
mqtt_broker = "test.mosquitto.org"
topic_temperature = "factory/temperature"
topic_humidity = "factory/humidity"
topic_alert = "factory/alert"
topic_fan_control = "factory/fan_control"
topic_all = "factory/all"
port = 1
address = 0x76
bus = smbus2.SMBus(port)
calibration_params = bme280.load_calibration_params(bus, address)
buzzer_pin = 20  # GPIO pin connected to the buzzer
led_pin = 16  # GPIO pin connected to the LED
fan_pin = 21  # GPIO pin connected to the fan
temperature_threshold = 32.00  # Temperature threshold in °C
 
# GPIO setup
GPIO.setmode(GPIO.BCM)
GPIO.setup(buzzer_pin, GPIO.OUT)
GPIO.setup(led_pin, GPIO.OUT)
GPIO.setup(fan_pin, GPIO.OUT)
GPIO.output(buzzer_pin, GPIO.LOW)
GPIO.output(led_pin, GPIO.LOW)
GPIO.output(fan_pin, GPIO.LOW)
 
# Variable to track fan state
fan_state = GPIO.LOW
 
# Initialise MQTT client
my_mqtt = mqtt.Client()
 
# MQTT message callback
def onMessage(client, userdata, message):
    global fan_state
    payload = message.payload.decode('utf-8')
    if message.topic == topic_fan_control:
        if payload == "on":
            fan_state = GPIO.HIGH
            GPIO.output(fan_pin, GPIO.HIGH)
            print("\n\n" + time.strftime(">>> %d/%m/%Y @ %H:%M:%S: FAN TURNED ON REMOTELY"))
        elif payload == "off":
            fan_state = GPIO.LOW
            GPIO.output(fan_pin, GPIO.LOW)
            print("\n\n" + time.strftime(">>> %d/%m/%Y @ %H:%M:%S: FAN TURNED OFF REMOTELY"))
 
# Assign MQTT callbacks
my_mqtt.on_message = onMessage
 
def main():
    try:
        # Connect to MQTT broker
        my_mqtt.connect(mqtt_broker, port = 1883)
        print("\n\n" + time.strftime(">>> %d/%m/%Y @ %H:%M:%S: CONNECTED TO BROKER"))
 
        # Subscribe to fan control topic
        my_mqtt.subscribe(topic_fan_control, qos = 1)
 
        # Start MQTT loop to handle callbacks
        my_mqtt.loop_start()
 
        while True:
            try:
                # Read temperature and humidity from BME280 sensor
                data = bme280.sample(bus, address, calibration_params)
                temperature = data.temperature
                humidity = data.humidity
 
                pay_load_temperature = "Temperature: {:.2f} °C".format(temperature) + time.strftime("\n(%d/%m/%Y @ %H:%M:%S) ")
                pay_load_humidity = "Humidity: {:.2f} %".format(humidity) + time.strftime("\n(%d/%m/%Y @ %H:%M:%S) ")
 
                mqtt_data = json.dumps({
                    "Temperature": temperature,
                    "Humidity": humidity
                })
 
                # Publish temperature and humidity to MQTT
                my_mqtt.publish(topic_temperature, pay_load_temperature)
                my_mqtt.publish(topic_humidity, pay_load_humidity)
                my_mqtt.publish(topic_all, mqtt_data)
                print("\n\n Temperature :  %.2f °C" % temperature)
                print(" Humidity    :  %.2f %%" % humidity)
                print("-------------------------")
                print(time.strftime("  %d/%m/%Y @ %H:%M:%S  "))
                print("-------------------------")
 
                # Check temperature threshold and activate buzzer and LED
                if temperature >= temperature_threshold:
                    alert_message = "WARNING! HIGH TEMPERATURE!\nTemperature: {:.2f} °C".format(temperature) + time.strftime("\n(%d/%m/%Y @ %H:%M:%S) ")
                    my_mqtt.publish(topic_alert, alert_message)
 
                    print("\n\n" + time.strftime(">>> %d/%m/%Y @ %H:%M:%S: WARNING! HIGH TEMPERATURE!"))
 
                    # Blink LED and buzzer until temperature drops below threshold
                    while temperature >= temperature_threshold:
                        GPIO.output(buzzer_pin, GPIO.HIGH)
                        GPIO.output(led_pin, GPIO.HIGH)
                        time.sleep(0.5)
                        GPIO.output(buzzer_pin, GPIO.LOW)
                        GPIO.output(led_pin, GPIO.LOW)
                        time.sleep(0.5)
 
                        # Re-read the temperature to check if it still exceeds the threshold
                        data = bme280.sample(bus, address, calibration_params)
                        temperature = data.temperature
 
                        if temperature < temperature_threshold:
                            alert_message = "SAFE TEMPERATURE\nTemperature: {:.2f} °C".format(temperature) + time.strftime("\n(%d/%m/%Y @ %H:%M:%S) ")
                            my_mqtt.publish(topic_alert, alert_message)
 
                            print("\n\n" + time.strftime(">>> %d/%m/%Y @ %H:%M:%S: SAFE TEMPERATURE"))
 
                else:
                    GPIO.output(buzzer_pin, GPIO.LOW)
                    GPIO.output(led_pin, GPIO.LOW)
 
                # Ensure fan state remains as per MQTT command
                GPIO.output(fan_pin, fan_state)
 
            except Exception as e:
                print("\n\n" + time.strftime(">>> %d/%m/%Y @ %H:%M:%S: ERROR READING SENSOR DATA:") + str(e).upper())
 
            # Wait for 10 seconds before the next reading
            time.sleep(10)
 
    except Exception as e:
        print("\n\n" + time.strftime(">>> %d/%m/%Y @ %H:%M:%S: ERROR CONNECTING TO MQTT BROKER:") + str(e).upper())
 
    finally:
        # Disconnect from MQTT broker
        my_mqtt.loop_stop()
        my_mqtt.disconnect()
        print("\n\n\n" + time.strftime("<<< %d/%m/%Y @ %H:%M:%S: DISCONNECTED FROM BROKER\n\n"))
        GPIO.cleanup()
 
if __name__ == "__main__":
    main()