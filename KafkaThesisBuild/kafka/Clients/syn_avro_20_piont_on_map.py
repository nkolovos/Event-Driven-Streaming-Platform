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
        print("$Delivery failed for User record {}: {}".format(msg.key(), err))
        return
    print('User record {} successfully produced to {} [{}] at offset {}'.format(
        msg.key(), msg.topic(), msg.partition(), msg.offset()))


if __name__ == '__main__':
    
    # Extract the 'topic' and 'specific' arguments from the command line input
    broker = 'localhost:9092'

    topic = 'avro-sensor'

    # Create a Schema Registry client with the provided configuration
    schema_registry_conf = {'url': "http://localhost:8081"}
    schema_registry_client = SchemaRegistryClient(schema_registry_conf)

    # Fetch the Avro schema from the Schema Registry
    subject_name = 'avro-sensor1-value'

    try:
        schema = schema_registry_client.get_latest_version(subject_name).schema.schema_str

    except SchemaRegistryError as e:
        if 'not found' in str(e):
            print(f"Schema '{subject_name}' not found.")
            # Handle the error, e.g., create the schema, exit the program, etc.
        else:
            # Re-raise the exception if it's a different error
            raise

    # Initialize AvroSerializer with the Schema Registry client and the Avro schema string
    avro_serializer = AvroSerializer(schema_registry_client, schema)

    # Initialize a StringSerializer for key serialization using 'utf-8' encoding
    string_serializer = StringSerializer('utf_8')

    # Configure the producer with bootstrap servers and 'acks' set to 'all' for message acknowledgment
    producer_conf = {'bootstrap.servers': "localhost:9092"}
    # ,                 'acks': "all"}

    # Initialize the producer with the above configuration
    producer = Producer(producer_conf)

    # Check if the topic exists in the list of excisting topics in the Kafka cluster
    # in order to avoid creating a topic that does not exist using the Producer Class
    if topic not in producer.list_topics().topics:
        print(f"Topic '{topic}' does not exist")
        exit(1)

    # Initialize the record counter 'i' to keep track of the number of records processed
    i = 0

    print("Producing user records to topic {}. ^C to exit.".format(topic))

    # Key generation 
    # key=string_serializer(str(uuid4()))
    # key = string_serializer("sensor1")
    counter = 0

    # List of (latitude, longitude) tuples
    coordinates = [
(40.2938, 22.0916),
(40.2938, 22.5936),
(40.2938, 21.9482),
(40.2938, 22.0241),
(40.2938, 21.6161),
(40.2938, 22.6127),
(40.2938, 21.2708),
(40.2938, 21.8368),
(40.2938, 21.7493),
(40.2938, 22.5838),
(40.2938, 21.3103),
(40.2938, 21.8197),
(40.2938, 21.4662),
(40.2938, 21.5698),
(40.2938, 21.6408),
(40.2938, 21.7492),
(40.2938, 22.0832),
(40.2938, 21.4771),
(40.2938, 22.138),
(40.2938, 22.0306),
]

    # Create a cyclic iterator for the coordinates
    coordinates_cycle = cycle(coordinates)

    # Create a list of sensor names
    sensor_names = [f"sensor{i}" for i in range(1, 21)]

    # Create a cyclic iterator for the sensor names
    sensor_names_cycle = cycle(sensor_names)
    

try:
    while True:
        # Get the next set of coordinates
        lat, lon = next(coordinates_cycle)
        sensor_name = next(sensor_names_cycle)

        # Key generation 
        key = sensor_name

        # Prepare the JSON data
        data = {
            "deviceId": sensor_name,
            "latitude": lat,
            "longitude": lon,
            "pm25": 40.5,
            "humidity": 40,
            "temperature": 40,
            "timestamp": int(time.time())  # Current time in seconds
        }

        try:
            # Produce the 'user_data' to a Kafka topic using the 'producer.produce' function
            # The 'key' is generated as a string representation of a UUID (unique identifier)
            # The 'value' is serialized using the 'avro_serializer' with the 'SerializationContext'
            # The 'delivery_report' function is used for reporting the status of message delivery
            print(f"Producing record: {key}")

            producer.produce(topic,
                             key=key,
                             value=avro_serializer(data, SerializationContext(topic, MessageField.VALUE)),
                             on_delivery=delivery_report)

        except BufferError:
            sys.stderr.write('%% Local producer queue is full (%d messages awaiting delivery): try again\n' %
                             len(producer))
            
        except KafkaException as e:
            sys.stderr.write('%% Serialization error: %s\n' % e)

        # Serve delivery callback queue.
        producer.poll(0)

        # Wait until all messages have been delivered
        sys.stderr.write('%% Waiting for %d deliveries\n' % len(producer))
        producer.flush()

        # Wait for 5 seconds before sending the next message
        time.sleep(1)
        # counter += 1

except KeyboardInterrupt:
    sys.stderr.write('%% Aborted by user\n')

finally:
    # Ensure all messages have been delivered before exiting
    producer.flush()