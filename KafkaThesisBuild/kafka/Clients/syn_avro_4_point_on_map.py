#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Copyright 2020 Confluent Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at


# A simple example demonstrating use of AvroSerializer.

import argparse
import os
import traceback
from itertools import cycle
import logging
import sys
import json
import time
from uuid import uuid4
from fastavro.validation import validate
from fastavro._validate_common import ValidationError as FastavroValidationError

from confluent_kafka import Producer, KafkaException
from confluent_kafka.admin import AdminClient
from confluent_kafka.serialization import StringSerializer, SerializationContext, MessageField
from confluent_kafka.schema_registry import SchemaRegistryClient, SchemaRegistryError
from confluent_kafka.schema_registry.avro import AvroSerializer

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logging.basicConfig(level=logging.ERROR, format='%(asctime)s - %(levelname)s - %(message)s')


def delivery_report(err, msg):
    """
    Reports the failure or success of a message delivery.

    Args:
        err (KafkaError): The error that occurred on None on success.

        msg (Message): The message that was produced or failed.

    Note:
        In the delivery report callback the Message.key() and Message.value()
        will be the binary format as encoded by any configured Serializers and
        not the same object that was passed to produce().
        If you wish to pass the original object(s) for key and value to delivery
        report callback we recommend a bound callback or lambda where you pass
        the objects along.
    """

    if err is not None:
        logging.info("$Delivery failed for User record {}: {}".format(msg.key(), err))
        return
    logging.info('User record %s successfully produced to %s [%s] at offset %s', 
             msg.key(), msg.topic(), msg.partition(), msg.offset())

if __name__ == '__main__':

    # Extract the 'topic' and 'specific' arguments from the command line input
    # will be passed as envirnment variables in a production deployment
    broker = 'localhost:9092'
    topic = 'avro-sensor'

    # Create a Schema Registry client with the provided configuration
    schema_registry_conf = {'url': "http://localhost:8081"}
    schema_registry_client = SchemaRegistryClient(schema_registry_conf)

    # Fetch the Avro schema from the Schema Registry
    subject_name = 'avro-sensor1'

    try:
        schema = schema_registry_client.get_latest_version(subject_name).schema.schema_str
        # schema_json = json.loads(schema)  # Convert the schema string to a dictionary

    except SchemaRegistryError as e:
        if 'not found' in str(e):
            logging.error(f"Schema '{subject_name}' not found.")
            exit(1)
            # Handle the error, e.g., create the schema, exit the program, etc.
        else:
            # Re-raise the exception if it's a different error
            raise

    # Create a custom subject name strategy function
    def custom_subject_name_strategy(topic, record_name):
        return subject_name  # Use the subject_name variable directly

    # Create a configuration dictionary for AvroSerializer
    avro_serializer_conf = {
        "auto.register.schemas": False,
        "subject.name.strategy": custom_subject_name_strategy
    }

    # Initialize AvroSerializer with the Schema Registry client and the Avro schema string
    avro_serializer = AvroSerializer(schema_registry_client, schema, conf=avro_serializer_conf)

    # Initialize a StringSerializer for key serialization using 'utf-8' encoding
    string_serializer = StringSerializer('utf_8')

    # Configure the producer with bootstrap servers and 'acks' set to 'all' for message acknowledgment
    producer_conf = ({'bootstrap.servers': broker})
    # ,                 'acks': "all"}

    # Initialize the producer with the above configuration
    producer = Producer(producer_conf)

    # Check if the topic exists in the list of excisting topics in the Kafka cluster
    # in order to avoid creating a topic that does not exist using the Producer Class
    if topic not in producer.list_topics().topics:
        logging.info(f"Topic '{topic}' does not exist")
        exit(1)

    # Initialize the record counter 'i' to keep track of the number of records processed
    i = 0

    logging.info("Producing user records to topic %s. ^C to exit.", topic)


    # Key generation 
    # key=string_serializer(str(uuid4()))
    # key = string_serializer("sensor1")
    counter = 0

    # List of (latitude, longitude) tuples
    coordinates = [
    (39.6289, 21.3425),
    (39.6843, 22.1578),
    (39.5981, 21.7969),
    (39.5457, 21.4421),
]

    # Create lists of pm25, temperature, and humidity values
    pm25_values = [40, 43.5, 52, 49.5, 60, 65, 70]
    temperature_values = [20, 22, 24, 26, 28, 30, 32, 34]
    humidity_values = [40, 45, 50, 55, 60, 65, 70, 75]

    # Create cyclic iterators for these values
    pm25_cycle = cycle(pm25_values)
    temperature_cycle = cycle(temperature_values)
    humidity_cycle = cycle(humidity_values)


    # Create a cyclic iterator for the coordinates
    coordinates_cycle = cycle(coordinates)

    # Create a list of sensor names
    sensor_names = [f"sensor{i}" for i in range(1, 5)]

    # Create a cyclic iterator for the sensor names
    sensor_names_cycle = cycle(sensor_names)
    

try:
    while True:
        # Get the next set of coordinates
        lat, lon = next(coordinates_cycle)
        sensor_name = next(sensor_names_cycle)

        pm25 = next(pm25_cycle)
        temperature = next(temperature_cycle)
        humidity = next(humidity_cycle)

        # Key generation 
        key = sensor_name

        # Prepare the JSON data
        data = {
            "deviceId": sensor_name,
            "latitude": lat,
            "longitude": lon,
            "pm25": pm25,
            "temperature": temperature,
            "humidity": humidity,
            "timestamp": int(time.time())  # Current time in seconds
        }

        try:
            # Produce the 'user_data' to a Kafka topic using the 'producer.produce' function
            # The 'key' is generated as a string representation of a UUID (unique identifier)
            # The 'value' is serialized using the 'avro_serializer' with the 'SerializationContext'
            # The 'delivery_report' function is used for reporting the status of message delivery
            # logging.info(f"Simple consumer subscribed to: {key}")
            
            producer.produce(topic,
                             key=key,
                             value=avro_serializer(data, SerializationContext(topic, MessageField.VALUE)),
                             on_delivery=delivery_report)
            
        except ValueError as e:
            logging.error('Value error: %s...........Please check your inserted value structure!', e)
            break

        except BufferError:
            logging.error('Local producer queue is full (%d messages awaiting delivery): try again', len(producer))            
            time.sleep(1)  # Wait for a short delay before retrying
            continue  # Retry the current iteration of the loop

        except KafkaException as e:
            logging.error('Kafka error: %s', e)
            break

        # Serve delivery callback queue.
        producer.poll(0)

        # Wait until all messages have been delivered
        sys.stderr.write('%% Waiting for %d deliveries\n' % len(producer))
        producer.flush()

        # Wait for 5 seconds before sending the next message
        time.sleep(3)
        # counter += 1

except KeyboardInterrupt:
    sys.stderr.write('%% Aborted by user\n')

finally:
    # Ensure all messages have been delivered before exiting
    producer.flush()