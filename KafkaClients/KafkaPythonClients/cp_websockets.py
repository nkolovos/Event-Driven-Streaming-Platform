import asyncio
import json
import threading
from confluent_kafka import Consumer, KafkaException
from websockets import WebSocketServerProtocol, serve

# Initialize Kafka consumer
consumer = Consumer({
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'group',
    'auto.offset.reset': 'earliest'
})

# Initialize sets for WebSocket clients and subscribed Kafka topics
websocket_clients = set()
kafka_subscribed_topics = set()

# Initialize a thread-safe asyncio Queue for Kafka messages
kafka_message_queue = asyncio.Queue()

def poll_kafka_messages():
    """Poll Kafka for messages and put them in the queue."""
    while True:
        message = consumer.poll(5.0)
        if message is not None and not message.error():
            kafka_message_queue.put_nowait(message)

# Start polling Kafka messages in a separate thread
threading.Thread(target=poll_kafka_messages, daemon=True).start()

async def subscribe_to_new_kafka_topics():
    """Check for new Kafka topics and subscribe to them."""
    while True:
        metadata = consumer.list_topics()
        topics = set(topic for topic in metadata.topics.keys() if not topic.startswith('_'))
        new_topics = topics - kafka_subscribed_topics
        if new_topics:
            kafka_subscribed_topics.update(new_topics)
            consumer.subscribe(list(kafka_subscribed_topics))  # Subscribe to all topics
            consumer.poll(1)  # Trigger a network poll to update the subscription set
        await asyncio.sleep(5)  # Check for new topics every second

async def send_kafka_messages_to_websocket_clients():
    """Send Kafka messages to WebSocket clients."""
    while True:
        message = await kafka_message_queue.get()
        message_value = message.value().decode('utf-8')
        print(f"Consumed message: {message_value}")  # Print the consumed message
        if websocket_clients:  # Check if there are any WebSocket clients
            await asyncio.wait([client.send(message_value) for client in websocket_clients])

async def handle_websocket_connection(websocket: WebSocketServerProtocol):
    """Handle a new WebSocket connection."""
    websocket_clients.add(websocket)
    try:
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