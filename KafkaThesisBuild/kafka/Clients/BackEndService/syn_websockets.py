import asyncio
import signal
import json
import threading
import logging
import time
from confluent_kafka import Consumer, KafkaException
from confluent_kafka.serialization import SerializationContext, MessageField
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from websockets import WebSocketServerProtocol, serve
import websockets
from queue import Queue
from dotenv import load_dotenv
import os


# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Initialize sets for WebSocket clients and subscribed Kafka topics
websocket_clients = set()
# kafka_subscribed_topics = set()
simple_kafka_subscribed_topics = set()
avro_kafka_subscribed_topics = set()

# Load environment variables from .env file
load_dotenv()
bootstrap_servers = os.getenv("BOOTSTRAP_SERVERS")
schema_registry_url = os.getenv("SCHEMA_REGISTRY_URL")


##################################################

# Initialize Kafka consumer
simple_consumer_conf = {'bootstrap.servers': bootstrap_servers, 'group.id': "group", 'auto.offset.reset': "latest"}
simple_consumer = Consumer(simple_consumer_conf)


##################################################
# Initialize Avro Kafka consumer for Avro messages
schema_registry_conf = {'url': schema_registry_url}
schema_registry_client = SchemaRegistryClient(schema_registry_conf)
avro_deserializer = AvroDeserializer(schema_registry_client)
avro_consumer_conf = {'bootstrap.servers': bootstrap_servers, 'group.id': "group2", 'auto.offset.reset': "latest"}
avro_consumer = Consumer(avro_consumer_conf)

####################################################

# Dictionary to store the last message for each topic
last_messages = {}

# Initialize a thread-safe asyncio Queue for Kafka messages
kafka_message_queue = Queue()


def poll_kafka_messages():
    """Poll Kafka for messages and put them in the queue."""

    while True:
        try:
            # Poll simple messages
            # logging.info("Polling Kafka for messages...")
            simple_message = simple_consumer.poll(1)

            if simple_message is not None and not simple_message.error():
                simple_message_value = simple_message.value().decode('utf-8')
                simple_message.set_value(simple_message_value)
                kafka_message_queue.put_nowait(simple_message)

            # Poll Avro messages
            avro_message = avro_consumer.poll(1)

            if avro_message is not None and not avro_message.error():
                deserialized_value = avro_deserializer(avro_message.value(), SerializationContext(avro_message.topic(), MessageField.VALUE))
                # deserialized_value = json.dumps(deserialized_value)
                json_value = json.dumps(deserialized_value)
                avro_message.set_value(json_value)
                kafka_message_queue.put_nowait(avro_message)

            # time.sleep(1)                        
        except KafkaException as e:
            logging.error(f"Message deserialization failed: {e}")

async def subscribe_to_new_kafka_topics():
    """Check for new Kafka topics and subscribe to them."""
    
    while True:
        # Get metadata from both consumers
        simple_metadata = simple_consumer.list_topics()
        avro_metadata = avro_consumer.list_topics()

        # Get the set of topics from both consumers, excluding those that start with '_'
        simple_topics = set(topic for topic in simple_metadata.topics.keys() if topic.startswith('simple-') and not topic.startswith('_'))
        avro_topics = set(topic for topic in avro_metadata.topics.keys() if topic.startswith('avro-') and not topic.startswith('_'))

        # Get the new topics for both consumers
        new_simple_topics = simple_topics - simple_kafka_subscribed_topics
        new_avro_topics = avro_topics - avro_kafka_subscribed_topics

        # # If there are new topics, update the subscribed topics and subscribe the consumers to the new topics
        if new_simple_topics:
            simple_kafka_subscribed_topics.update(new_simple_topics)
            simple_consumer.subscribe(list(new_simple_topics))
            logging.info(f"Simple consumer subscribed to: {new_simple_topics}")

        if new_avro_topics:
            avro_kafka_subscribed_topics.update(new_avro_topics)
            avro_consumer.subscribe(list(new_avro_topics))
            logging.info(f"Avro consumer subscribed to: {new_avro_topics}")

        await asyncio.sleep(100)  # Check for new topics every second


async def send_kafka_messages_to_websocket_clients():
    """Send Kafka messages to WebSocket clients."""

    while True:
        # Process all available messages in the queue
        while not kafka_message_queue.empty():
            # Get the next message from the Kafka queue
            message = await loop.run_in_executor(None, kafka_message_queue.get_nowait)

            # Extract the value from the message
            message_value = message.value()
            # print(message_value)

            # message_value = json.dumps(message.value())

            # Extract the key from the message
            device_id = message.key()

            # Store the last message for each device
            last_messages[device_id] = message_value

            logging.info(f"From device: {device_id}. Number of unique devices: {len(last_messages)}")

            # If there are any WebSocket clients, send the message to all of them
            if websocket_clients:
                for client in websocket_clients.copy():
                    try:
                        await client.send(message_value)

                    except (websockets.exceptions.ConnectionClosedOK, websockets.exceptions.ConnectionClosedError):
                        continue

        # Sleep for 5 seconds before processing the next batch of messages
        await asyncio.sleep(5)
        
async def handle_websocket_connection(websocket: WebSocketServerProtocol, path):
    """Handle a new WebSocket connection."""

    # Add the new WebSocket client to the set of clients
    logging.info(f"New WebSocket connection: {path}")
    websocket_clients.add(websocket)

    try:
        # Send the last message for each device ID upon connection
        for device_id, last_message in last_messages.items():
            await websocket.send(last_message)

        # Wait for the WebSocket connection to close
        await websocket.wait_closed()

    finally:
        # Remove the WebSocket client from the set of clients
        websocket_clients.remove(websocket)


# Start polling Kafka messages in a separate thread
threading.Thread(target=poll_kafka_messages, daemon=True).start()

# Create an asyncio event loop
loop = asyncio.get_event_loop()

# Start a WebSocket server
start_server = serve(handle_websocket_connection, '0.0.0.0', 18080)

# Ensure that the function to send Kafka messages to WebSocket clients is running
send_kafka_messages_task = loop.create_task(send_kafka_messages_to_websocket_clients())

# Ensure that the function to subscribe to new Kafka topics is running
subscribe_to_new_kafka_topics_task = loop.create_task(subscribe_to_new_kafka_topics())

# Run the WebSocket server, and the Kafka message and topic handlers
loop.run_until_complete(start_server)

# Run the event loop forever
loop.run_forever()


def shutdown(signal, loop):
    """Shutdown gracefully on SIGINT or SIGTERM."""
    
    logging.info("Received exit signal, shutting down...")
    logging.info("Closing Kafka consumers...")

    # Close the Kafka consumers
    simple_consumer.close()  # Close simple Kafka consumer
    avro_consumer.close()  # Close Avro Kafka consumer

    # Close all WebSocket connections
    logging.info("Closing WebSocket clients...")
    for client in websocket_clients:
        loop.run_until_complete(client.close())

    # Cancel all running asyncio tasks
    logging.info("Cancelling asyncio tasks...")
    tasks = [t for t in asyncio.all_tasks() if t is not asyncio.current_task()]

    [task.cancel() for task in tasks]

    logging.info("Cancelling complete! Waiting for tasks to finish...")
    loop.run_until_complete(asyncio.gather(*tasks, return_exceptions=True))
    logging.info("Tasks finished cancelling.")

    # Stop the asyncio event loop
    loop.stop()

# Add signal handlers for SIGINT and SIGTERM to perform a graceful shutdown
for sig in ('SIGINT', 'SIGTERM'):
    loop.add_signal_handler(getattr(signal, sig), lambda: loop.create_task(shutdown(sig, loop)))

# Run the event loop until a shutdown signal is received
try:
    loop.run_forever()

except KeyboardInterrupt:
    print("KeyboardInterrupt received, shutting down...")
    loop.run_until_complete(shutdown(None, loop))

except Exception as e:
    print(f"An error occurred: {e}")

finally:
    loop.close()