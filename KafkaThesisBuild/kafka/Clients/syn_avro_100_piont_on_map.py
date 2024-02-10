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
(39.5153, 21.9358),
(40.0625, 22.3272),
(39.2103, 21.8452),
(39.5639, 21.3767),
(39.4862, 22.0611),
(39.0831, 21.3625),
(40.297, 22.5443),
(39.2447, 22.058),
(39.1849, 21.5475),
(39.6039, 21.4845),
(40.2938, 21.1858),
(39.8153, 21.9613),
(40.0386, 22.6117),
(40.0355, 22.3259),
(39.7839, 22.4991),
(39.9781, 21.9133),
(39.4877, 21.5268),
(39.3013, 21.2248),
(39.2484, 21.4496),
(39.0872, 22.4497),
(40.1783, 21.5025),
(40.3508, 22.2293),
(40.3559, 21.4144),
(39.8234, 22.5656),
(39.3958, 21.3938),
(40.3738, 22.278),
(39.8972, 21.2341),
(40.2789, 22.3148),
(39.5616, 21.7839),
(39.6878, 22.03),
(40.0366, 21.7419),
(39.9462, 21.4316),
(39.254, 22.017),
(39.7763, 22.3597),
(39.1215, 22.3169),
(39.0235, 21.3278),
(40.1007, 22.1297),
(39.9599, 21.4127),
(38.9126, 21.9776),
(39.8203, 22.3235),
(39.1045, 21.2618),
(39.5814, 21.1854),
(40.3228, 22.4289),
(40.3347, 22.4751),
(40.2443, 22.1861),
(38.9123, 21.5676),
(39.3364, 21.953),
(39.9214, 22.0251),
(39.709, 22.5503),
(39.5435, 21.4125),
(39.1243, 21.5998),
(39.9698, 22.5684),
(39.2843, 22.2434),
(40.1013, 21.5104),
(40.3157, 22.0031),
(40.4059, 22.3003),
(40.1924, 22.0443),
(40.3145, 21.5735),
(39.0094, 22.3948),
(39.8026, 21.4549),
(39.8234, 22.2089),
(39.1323, 21.7381),
(40.3274, 22.4639),
(38.8882, 21.3089),
(39.6539, 22.1668),
(39.678, 22.1431),
(39.284, 22.1429),
(39.5332, 21.6251),
(40.1196, 21.9042),
(40.1149, 21.4816),
(40.0464, 22.2427),
(40.2326, 22.3538),
(39.2392, 21.7419),
(39.5007, 22.464),
(40.0993, 22.0279),
(39.8491, 22.5604),
(40.3786, 21.2963),
(40.0449, 21.7069),
(40.3035, 21.7947),
(39.3363, 21.7947),
(38.9742, 21.9423),
(38.9263, 22.0963),
(40.0197, 22.5146),
(40.2475, 22.5032),
(39.1887, 22.1224),
(39.7745, 22.3814),
(38.9842, 22.5737),
(39.0002, 22.1265),
(40.3724, 22.3356),
(39.1373, 21.9237),
(39.019, 22.4488),
(39.0512, 22.1355),
(39.2953, 22.2834),
(38.9281, 22.2914),
(39.3299, 22.3465),
(40.0699, 22.2156),
(39.3709, 21.7212),
(40.2287, 21.2463),
(39.8007, 22.2573),
(39.4303, 21.5888),
]

    # Create a cyclic iterator for the coordinates
    coordinates_cycle = cycle(coordinates)

    # Create a list of sensor names
    sensor_names = [f"sensor{i}" for i in range(1, 101)]

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