# Introduction

This multi-module project provides connectors for Kafka Connect to read and write data to RabbitMQ. The module kafka-connect-rabbitmq, thus named for legacy reasons, contains the source connector. The module kafka-connect-rabbitmq-sink contains the sink connector. Both connectors are versioned together but released as different artefacts.

## RabbitMQSourceConnector

The RabbitMQSourceConnector is used to write data from RabbitMQ queue to a Kafka topic. More details can be found in the README file inside the kafka-connect-rabbitmq module and on Confluent's [docs site](https://docs.confluent.io/current/connect/kafka-connect-rabbitmq/).

## RabbitMQSinkConnector

The RabbitMQSinkConnector is used to write data from kafka topic(s) to a RabbitMQ exchange. More details on features and configuration can be found in the README and QUICKSTART files inside the kafka-connect-rabbitmq-sink module and on Confluent's [docs site](https://docs.confluent.io/current/connect/kafka-connect-rabbitmq/sink/).