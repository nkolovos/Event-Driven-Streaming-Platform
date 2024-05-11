import paho.mqtt.client as mqtt
import json
import time



# MQTT server details
mqtt_host = 'localhost'
mqtt_port = 1885
mqtt_topic = 'simple-mqtt'
mqtt_username = 'test'
mqtt_password = 'mytest32'

# Create an MQTT client
client = mqtt.Client()

# Set username and password
client.username_pw_set(mqtt_username, mqtt_password)

# Connect to the MQTT server
client.connect(mqtt_host, mqtt_port)

try:
    while True:
        # Test data
        data = {
                "deviceId": "mqtt_sensor",
                "latitude": 39.3810,
                "longitude": 22.9924,
                "pm25": 40,
                "temperature": 40,
                "humidity": 40,
                "timestamp": int(time.time())  # Current time in milliseconds
                }
        # Convert user data to JSON
        user_json = json.dumps(data)

        # Publish JSON data to MQTT topic
        client.publish(mqtt_topic, user_json)

        print(f" [x] Sent JSON data to topic '{mqtt_topic}'")

        time.sleep(5)  # wait for 5 seconds before sending the next message
except KeyboardInterrupt:
    # Disconnect from the MQTT server when Ctrl+C is pressed
    client.disconnect()