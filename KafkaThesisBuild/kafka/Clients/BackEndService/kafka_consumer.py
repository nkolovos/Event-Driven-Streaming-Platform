from confluent_kafka import Consumer, KafkaException
from confluent_kafka.serialization import SerializationContext, MessageField
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
import asyncio
import logging
import json
import time

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Initialize Kafka consumer
simple_consumer_conf = {'bootstrap.servers': "localhost:9092", 'group.id': "group", 'auto.offset.reset': "latest"}
simple_consumer = Consumer(simple_consumer_conf)

# Initialize Avro Kafka consumer for Avro messages
schema_registry_conf = {'url': "http://localhost:8081"}
schema_registry_client = SchemaRegistryClient(schema_registry_conf)
avro_deserializer = AvroDeserializer(schema_registry_client)
avro_consumer_conf = {'bootstrap.servers': "localhost:9092", 'group.id': "group2", 'auto.offset.reset': "latest"}
avro_consumer = Consumer(avro_consumer_conf)

# Initialize sets for subscribed Kafka topics
simple_kafka_subscribed_topics = set()
avro_kafka_subscribed_topics = set()

# Initialize a thread-safe asyncio Queue for Kafka messages
kafka_message_queue = asyncio.Queue()

# Dictionary to store the last message for each topic
last_messages = {}

def poll_kafka_messages():
    """Poll Kafka for messages and put them in the queue."""

    while True:
        try:
            # Poll Avro messages
            avro_message = avro_consumer.poll(2)

            if avro_message is not None and not avro_message.error():
                deserialized_value = avro_deserializer(avro_message.value(), SerializationContext(avro_message.topic(), MessageField.VALUE))
                deserialized_value = json.dumps(deserialized_value)
                avro_message.set_value(deserialized_value)
                kafka_message_queue.put_nowait(avro_message)

        except KafkaException as e:
            logging.error(f"Message deserialization failed: {e}")

async def subscribe_to_new_kafka_topics():
    """Check for new Kafka topics and subscribe to them."""
    
    while True:
        # Get metadata from Avro consumer
        avro_metadata = avro_consumer.list_topics()

        # Get the set of topics from Avro consumer, excluding those that start with '_'
        avro_topics = set(topic for topic in avro_metadata.topics.keys() if topic.startswith('avro-') and not topic.startswith('_'))

        # Get the new topics for Avro consumer
        new_avro_topics = avro_topics - avro_kafka_subscribed_topics

        if new_avro_topics:
            avro_kafka_subscribed_topics.update(new_avro_topics)
            avro_consumer.subscribe(list(new_avro_topics))
            logging.info(f"Avro consumer subscribed to: {new_avro_topics}")

        await asyncio.sleep(5)  # Check for new topics every second