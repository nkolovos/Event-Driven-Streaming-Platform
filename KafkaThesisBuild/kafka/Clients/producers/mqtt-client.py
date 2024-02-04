import paho.mqtt.client as mqtt
import json
import time

# Test data
data = {
            "deviceId": "mqtt_sensor",
            "latitude": 39.38103,
            "longitude": 22.99248,
            "pm25": 40,
            "humidity": 40,
            "temperature": 40,
            "timestamp": int(time.time())  # Current time in milliseconds
        }

# MQTT server details
mqtt_host = 'localhost'
mqtt_topic = 'simple-mqtt'
mqtt_username = 'nikos'
mqtt_password = '56312012'

# Create an MQTT client
client = mqtt.Client()

# Set username and password
client.username_pw_set(mqtt_username, mqtt_password)

# Connect to the MQTT server
client.connect(mqtt_host)

try:
    while True:
        # Convert user data to JSON
        user_json = json.dumps(data)

        # Publish JSON data to MQTT topic
        client.publish(mqtt_topic, user_json)

        print(f" [x] Sent JSON data to topic '{mqtt_topic}'")

        time.sleep(5)  # wait for 5 seconds before sending the next message
except KeyboardInterrupt:
    # Disconnect from the MQTT server when Ctrl+C is pressed
    client.disconnect()