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


# A simple example demonstrating use of AvroSerializer.

import argparse
import os
import traceback
import json
import time
from uuid import uuid4
from fastavro.validation import validate
from fastavro._validate_common import ValidationError as FastavroValidationError

from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient
from confluent_kafka.serialization import StringSerializer, SerializationContext, MessageField
from confluent_kafka.schema_registry import SchemaRegistryClient
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


def main(args):
    # Extract the 'topic' and 'specific' arguments from the command line input
    topic = args.topic

    is_specific = args.specific == "true"

    # Select the schema based on whether it is specific or not
    if is_specific:
        schema = "avro_schema_long_konbert.avsc"
    else:
        schema = "avro_schema_short.avsc"

    # Get the absolute path of the current directory
    path = os.path.realpath(os.path.dirname(__file__))

    # Read the Avro schema from the corresponding file and store it as a string
    with open(f"{path}/avro/{schema}") as f:
        schema_str = f.read()

    # Load the Avro schema as a JSON object for later validation
    with open(f"{path}/avro/{schema}") as f:
        schema_obj = json.load(f)

    # Create a Schema Registry client with the provided configuration
    schema_registry_conf = {'url': args.schema_registry}

    # Initialize a Schema Registry client
    schema_registry_client = SchemaRegistryClient(schema_registry_conf)

    # Initialize AvroSerializer with the Schema Registry client and the Avro schema string
    avro_serializer = AvroSerializer(schema_registry_client, schema_str)

    # Initialize a StringSerializer for key serialization using 'utf-8' encoding
    string_serializer = StringSerializer('utf_8')

    # Configure the producer with bootstrap servers and 'acks' set to 'all' for message acknowledgment
    producer_conf = {'bootstrap.servers': args.bootstrap_servers, 
                     'acks': "all"}

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
    key=string_serializer(str(uuid4()))

    while i in range(5):

        # Serve on_delivery callbacks from previous calls to produce()
        producer.poll(0.0)
        try:
            # Assuming "json_random_chatgpt.json" is the different file you want to read from
            json_file_path = os.path.join(path,"avro" ,"input.json")
            
            # Open the JSON file and load its contents into 'user_data'
            with open(json_file_path, "r") as json_file:
                user_data = json.load(json_file)
            
            # Validate the 'user_data' against the provided schema using the 'validate' function
            validate(user_data, schema_obj, raise_errors=True, strict=True)
            
            # Produce the 'user_data' to a Kafka topic using the 'producer.produce' function
            # The 'key' is generated as a string representation of a UUID (unique identifier)
            # The 'value' is serialized using the 'avro_serializer' with the 'SerializationContext'
            # The 'delivery_report' function is used for reporting the status of message delivery
            producer.produce(topic=topic,
                             key=key,
                             value=avro_serializer(user_data, 
                                                   SerializationContext(topic, MessageField.VALUE)),
                                                   on_delivery=delivery_report)

            # Pause for 5 seconds before continuing with the next iteration of the loop
            time.sleep(0)
            
            # Increment the counter 'i' to process the next record
            i += 1

        # Exception handling
        # If a KeyboardInterrupt is encountered, the loop will break, allowing the program to exit gracefully
        # If a ValueError occurs, it indicates an invalid input, and the current record is discarded
        # If a FastavroValidationError occurs during serialization, the traceback is printed, and the current record is skipped
        # In both cases, 'i' is incremented to process the next record
        except KeyboardInterrupt:
            break

        except ValueError:
            print("Invalid input, discarding record...")
            i += 1
            continue

        except FastavroValidationError:
            traceback.print_exc()
            i += 1
            continue
        

    print("\nFlushing records...")
    producer.flush()


if __name__ == '__main__':
    
    parser = argparse.ArgumentParser(description="AvroSerializer example")
    parser.add_argument('-b', dest="bootstrap_servers", required=True,
                        help="Bootstrap broker(s) (host[:port])")
    parser.add_argument('-s', dest="schema_registry", required=True,
                        help="Schema Registry (http(s)://host[:port]")
    parser.add_argument('-t', dest="topic", default="example_serde_avro",
                        help="Topic name")
    parser.add_argument('-p', dest="specific", default="true",
                        help="Avro specific record")

    main(parser.parse_args())
