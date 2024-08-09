import argparse
from influxdb import InfluxDBClient
from influxdb.client import InfluxDBClientError
import datetime
import time
import paho.mqtt.client as mqtt
import json
 
# MQTT broker and topics
mqtt_broker = "test.mosquitto.org"
topic_all = "factory/all"
 
# Database configuration
USER = "root"
PASSWORD = "root"
DBNAME = "database4"
HOST = "localhost"
PORT = 8086
dbclient = None;
 
# MQTT client
my_mqtt = mqtt.Client()
 
# MQTT message callback
def onMessage(client, userdata, message):
    try:
        # Decode the JSON payload
        mqtt_data = json.loads(message.payload.decode("utf-8"))
        temperature = float(mqtt_data["Temperature"])
        humidity = float(mqtt_data["Humidity"])
        print("\n\n Temperature :  %.2f °C" % temperature)
        print(" Humidity    :  %.2f %%" % humidity)
        print("-------------------------")
        print(time.strftime("  %d/%m/%Y @ %H:%M:%S  "))
        print("-------------------------")
 
        # Prepare the data for InfluxDB
        now = time.gmtime()
        data = [
            {
                "time": time.strftime("%Y-%m-%d %H:%M:%S", now),
                "measurement": "reading",
                "tags": {
                    "nodeId": "node_1",
                },
                "fields": {
                    "Temperature": temperature,
                    "Humidity": humidity
                },
            }
        ]
 
        # Write data to InfluxDB
        dbclient.write_points(data)
        print("DATA WRITTEN TO INFLUXDB")
 
    except Exception as e:
        print(f"\nERROR PROCESSING MESSAGE: {str(e).upper()}")
 
# Start MQTT client and subscribe to topics
def startMQTT():
    global dbclient
    try:
        # Initialise InfluxDB client
        dbclient = InfluxDBClient(HOST, PORT, USER, PASSWORD, DBNAME)
 
        # Connect to MQTT broker and set up callbacks
        my_mqtt.on_message = onMessage
        my_mqtt.connect(mqtt_broker, port = 1883)
        my_mqtt.subscribe(topic_all, qos = 1)
        my_mqtt.loop_start()
 
    except Exception as e:
        print(f"\nFAILED TO START MQTT OR CONNECT TO DATABASE: {str(e).upper()}")
 
# Main function
def main():
    startMQTT()
    try:
        # Keep the script running to maintain MQTT connection
        while True:
            time.sleep(2)
 
    except KeyboardInterrupt:
        print("\nINTERRUPTED BY USER, STOPPING...")
 
    finally:
        # Clean up resources
        if dbclient:
            dbclient.close()
        my_mqtt.loop_stop()
        my_mqtt.disconnect()
        print("\nDISCONNECTED FROM MQTT BROKER AND CLEANED UP RESOURCES")
 
if __name__ == "__main__":
    main()