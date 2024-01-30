#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Copyright 2020 Confluent Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


import argparse
import os
import json

from confluent_kafka import Consumer, KafkaException
from confluent_kafka.serialization import SerializationContext, MessageField
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer


def main(args):
    topic = args.topic
    is_specific = args.specific == "true"

    if is_specific:
        schema = "avro_schema_long_konbert.avsc"
    else:
        schema = "avro_schema_short.avsc"

    path = os.path.realpath(os.path.dirname(__file__))
    with open(f"{path}/avro/{schema}") as f:
        schema_str = f.read()

    sr_conf = {'url': args.schema_registry}

    schema_registry_client = SchemaRegistryClient(sr_conf)

    avro_deserializer = AvroDeserializer(schema_registry_client,
                                         schema_str)

    consumer_conf = {'bootstrap.servers': args.bootstrap_servers,
                     'group.id': args.group,
                     'auto.offset.reset': "earliest"}

    consumer = Consumer(consumer_conf)

    # Subscribe to topic
    consumer.subscribe([topic])

    # Assign path to output directory
    output_dir = os.path.join(path, "json_output")

    # Create output directory if it does not exist
    os.makedirs(output_dir, exist_ok=True)

    while True:
        try:
            # SIGINT can't be handled when polling, limit timeout to 1 second.
            msg = consumer.poll(2.0)
            # print("msg: ", msg)

            if msg is None:

                continue

            if msg.error():

                raise KafkaException(msg.error())

            # Deserialize the message using the Avro deserializer
            user_data = avro_deserializer(msg.value(), SerializationContext(msg.topic(), MessageField.VALUE))
            
            # Store the deserialized message in a JSON file 
            if user_data is not None:

                output_file = os.path.join(output_dir, f"output_user_{msg.offset()}.json")

                with open(output_file, "w") as json_file:
                    json.dump(user_data, json_file, indent=2)
        
        # Handle keyboard interrupt
        except KeyboardInterrupt:
            break

    consumer.close()


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="AvroDeserializer example")
    parser.add_argument('-b', dest="bootstrap_servers", required=True,
                        help="Bootstrap broker(s) (host[:port])")
    parser.add_argument('-s', dest="schema_registry", required=True,
                        help="Schema Registry (http(s)://host[:port]")
    parser.add_argument('-t', dest="topic", default="example_serde_avro",
                        help="Topic name")
    parser.add_argument('-g', dest="group", default="example_serde_avro",
                        help="Consumer group")
    parser.add_argument('-p', dest="specific", default="true",
                        help="Avro specific record")

    main(parser.parse_args())
