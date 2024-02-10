import asyncio
import signal
import threading
import logging
from websockets import serve
from kafka_consumer import poll_kafka_messages, subscribe_to_new_kafka_topics, simple_consumer, avro_consumer
from websocket_server import handle_websocket_connection, send_kafka_messages_to_websocket_clients, websocket_clients
from utils import shutdown

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Start polling Kafka messages in a separate thread
threading.Thread(target=poll_kafka_messages, daemon=True).start()

# Start a WebSocket server
start_server = serve(handle_websocket_connection, 'localhost', 18080)

# Get the event loop
loop = asyncio.get_event_loop()

# Run the WebSocket server, and the Kafka message and topic handlers
loop.run_until_complete(start_server)

# Ensure that the function to send Kafka messages to WebSocket clients is running
asyncio.ensure_future(send_kafka_messages_to_websocket_clients())

# Ensure that the function to subscribe to new Kafka topics is running
asyncio.ensure_future(subscribe_to_new_kafka_topics())

# Add signal handlers for SIGINT and SIGTERM to perform a graceful shutdown
for sig in ('SIGINT', 'SIGTERM'):
    loop.add_signal_handler(getattr(signal, sig), shutdown, sig, loop, simple_consumer, avro_consumer, websocket_clients)

# Run the event loop forever
loop.run_forever()