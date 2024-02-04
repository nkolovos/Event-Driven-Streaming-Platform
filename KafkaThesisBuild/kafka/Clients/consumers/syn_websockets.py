import asyncio
import signal
import json
import threading
from confluent_kafka import Consumer, KafkaException
from confluent_kafka.serialization import SerializationContext, MessageField
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from websockets import WebSocketServerProtocol, serve

# Initialize sets for WebSocket clients and subscribed Kafka topics
websocket_clients = set()
# kafka_subscribed_topics = set()
simple_kafka_subscribed_topics = set()
avro_kafka_subscribed_topics = set()


##################################################

simple_consumer_conf = {'bootstrap.servers': "localhost:9092",
                        'group.id': "group",
                        'auto.offset.reset': "latest"}

# Initialize Kafka consumer
simple_consumer = Consumer(simple_consumer_conf)


##################################################
# Initialize Avro Kafka consumer for Avro messages
schema_registry_conf = {'url': "http://localhost:8081"}

schema_registry_client = SchemaRegistryClient(schema_registry_conf)

avro_deserializer = AvroDeserializer(schema_registry_client)

avro_consumer_conf = {'bootstrap.servers': "localhost:9092",
                    'group.id': "group2",
                    'auto.offset.reset': "latest"}

avro_consumer = Consumer(avro_consumer_conf)

####################################################

# Dictionary to store the last message for each topic
last_messages = {}

# Initialize a thread-safe asyncio Queue for Kafka messages
kafka_message_queue = asyncio.Queue()

def poll_kafka_messages():
    """Poll Kafka for messages and put them in the queue."""
    while True:
        # Poll simple messages
        simple_message = simple_consumer.poll(1.0)

        if simple_message is not None and not simple_message.error():

            # json_value = json.dumps(simple_message.value())

            # simple_message.set_value(json_value)
            simple_message_value = simple_message.value().decode('utf-8')

            simple_message.set_value(simple_message_value)

            kafka_message_queue.put_nowait(simple_message)

        # Poll Avro messages
        try:
            avro_message = avro_consumer.poll(1.0)

            if avro_message is not None and not avro_message.error():

                deserialized_value = avro_deserializer(avro_message.value(), SerializationContext(avro_message.topic(), MessageField.VALUE))

                deserialized_value = json.dumps(deserialized_value)

                # print("deserialized_value: ", deserialized_value)

                avro_message.set_value(deserialized_value)

                kafka_message_queue.put_nowait(avro_message)
                
        except KafkaException as e:

            print("Message deserialization failed for {}: {}".format(avro_message, e))

# Start polling Kafka messages in a separate thread
threading.Thread(target=poll_kafka_messages, daemon=True).start()

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

        # If there are new topics, update the subscribed topics and subscribe the consumers to the new topics
        if new_simple_topics:

            simple_kafka_subscribed_topics.update(new_simple_topics)

            simple_consumer.subscribe(list(new_simple_topics))

            print(f"Simple consumer subscribed to: {new_simple_topics}")

        if new_avro_topics:

            avro_kafka_subscribed_topics.update(new_avro_topics)

            avro_consumer.subscribe(list(new_avro_topics))

            print(f"Avro consumer subscribed to: {new_avro_topics}")

        await asyncio.sleep(2)  # Check for new topics every second

async def send_kafka_messages_to_websocket_clients():
    """Send Kafka messages to WebSocket clients."""
    while True:
        message = await kafka_message_queue.get()

        message_value = message.value()

        # message_value = message.value().decode('utf-8')

        # message_value = json.dumps(message.value())

        topic = message.topic()

        # Store the last message for each topic
        last_messages[topic] = message_value

        print(f"Consumed message: {message.offset()} from topic: {topic}")

        if websocket_clients:  # Check if there are any WebSocket clients
            await asyncio.gather(*[client.send(message_value) for client in websocket_clients])


async def handle_websocket_connection(websocket: WebSocketServerProtocol):
    """Handle a new WebSocket connection."""
    websocket_clients.add(websocket)

    try:
        # Send the last message for each subscribed topic upon connection
        for topic, last_message in last_messages.items():

            await websocket.send(last_message)
            
        await websocket.wait_closed()

    finally:
        websocket_clients.remove(websocket)


# Start a WebSocket server
start_server = serve(handle_websocket_connection, 'localhost', 18080)

# Run the WebSocket server, and the Kafka message and topic handlers
asyncio.get_event_loop().run_until_complete(start_server)

asyncio.ensure_future(send_kafka_messages_to_websocket_clients())

asyncio.ensure_future(subscribe_to_new_kafka_topics())

asyncio.get_event_loop().run_forever()


def shutdown(signal, loop):
    """Shutdown gracefully on SIGINT or SIGTERM."""

    print("Received exit signal, shutting down...")

    simple_consumer.close()  # Close simple Kafka consumer
    avro_consumer.close()  # Close Avro Kafka consumer

    for client in websocket_clients:  # Close WebSocket connections
        loop.run_until_complete(client.close())

    loop.stop()  # Stop the asyncio event loop


# Handle SIGINT and SIGTERM for graceful shutdown
loop = asyncio.get_event_loop()

for sig in ('SIGINT', 'SIGTERM'):

    loop.add_signal_handler(getattr(signal, sig), shutdown, sig, loop)

try:
    loop.run_forever()
except KeyboardInterrupt:
    print("KeyboardInterrupt received, shutting down...")
    shutdown(None, loop)
finally:
    loop.close()