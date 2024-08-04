import time  # Importing the time module to work with time-related functions
import paho.mqtt.client as mqtt  # Importing the MQTT client from the Paho library
import psutil  # Importing the psutil library to retrieve system performance metrics
import sys  # Importing sys to handle system exit

# Define the MQTT broker address and topics
MQTT_BROKER = "test.mosquitto.org"
MQTT_TOPIC_CONTROL = "tp/eng/grp5/control"
MQTT_TOPIC_DATA = "tp/eng/grp5/data"
MQTT_PORT=1883

# Define the callback function for handling incoming messages
def on_message(client, userdata, message):
    """Callback function for handling incoming messages."""
    print(f"Received message '{message.payload.decode()}' on topic '{message.topic}'")

def start_mqtt():
    """Connect to the MQTT broker and return the client instance."""
    client = mqtt.Client()
    print("\nCreated client object at " +  time.strftime("%d-%m-%Y  %H:%M:%S",time.localtime()))
    client.on_message = on_message  # Set the callback function for message handling
    
    try:
        client.connect(MQTT_BROKER, MQTT_PORT)
        print("--connected to broker")

        client.subscribe(MQTT_TOPIC_CONTROL, qos=1)
        client.loop_start()
        print("Subscribed to topics")
    except Exception as e:
        print(f"Couldn't connect to the MQTT broker: {e}")
        sys.exit(1)
        
    return client

def publish_data(client, cpu_usage, vm_avail):
    """Publish CPU usage and virtual memory data to the MQTT broker."""
    payload = "Location: Tank 1"+"\nCPU: " + str(cpu_usage)+"\nVM Avail: " + str(vm_avail)+"\nTime: " + time.strftime("%d-%m-%Y  %H:%M:%S", time.localtime())

    try:
        client.publish(MQTT_TOPIC_DATA, payload)

        print("--cpu usage percent = %.1f \t v.mem avail = %d KB" % (cpu_usage, vm_avail/1024))
    except Exception as e:
        print(f"--error publishing: {e}")

def main():
    """Main function to monitor system metrics and publish to MQTT."""
    mqtt_client = start_mqtt()
    
    try:
        while True:
            # Get the CPU usage percentage over an interval of 10 seconds
            cpu_usage = psutil.cpu_percent(interval=10)
            
            # Get the available virtual memory in bytes
            vm_avail = psutil.virtual_memory().available
            
            # Publish the CPU usage and available virtual memory
            publish_data(mqtt_client, cpu_usage, vm_avail)
            
            # Sleep for a bit before the next iteration to avoid rapid publishing
            time.sleep(10)
    except KeyboardInterrupt:
        # Gracefully handle keyboard interruption (Ctrl+C)
        print("Interrupted by user, disconnecting...")
    finally:
        # Ensure the client disconnects gracefully on exit
        mqtt_client.loop_stop()
        mqtt_client.disconnect()
        print("--disconnected from broker")

if __name__ == "__main__":
    main()