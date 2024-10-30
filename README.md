# **What does this repository contain?**

## This repository contains the following folders:
- **KafkaClients** : This folder contains the code for Kafka clients, both custom and confluent SDK clients. They are useful for making various operations on our Kafka cluster from the client side.
- **KafkaDocker** : This folder contains the docker-compose file for setting up our application in various configurations and approches.
- **KafkaThesisBuild** : This folder contains the final build of our Thesis streaming application, excluding the React frontend Live-map, which is located in the [LiveMapReact](https://github.com/nkolovos/LiveMapReact)  repo. Here you will find detailed inforamtion on the **architecture** and the implementation of the application, in addtion with useful information on event-driven applications.

# **Future Goals**

The whole project was developed with scalability in mind, and it is designed to be easily extended and modified. One of the future goals is the implementation of **Apache Flink** for real-time processing of the data streams, instead of the current custom monolithic approach.

In addition, the development of enterprise-level security by utilizing SASL_SSL and TLS(SSL) security protocols. Our configuration would involve using
SSL for internal listeners(clients) and inter-broker communications. Additionally, external
listeners(clients) could be configured to utilize SASL_SSL, integrated with a Kerberos server
through the GSSAPI authentication mechanism. Such integration necessitates thorough research and testing based on the cluster’s needs and priorities, in order to achieve the optimal
balance between security and performance. Additionally, we can enhance the default behavior of Kafka’s key mapping mechanism. This could be achieved by integrating an external
load balancing service or by developing and configuring our own key hashing mechanism,
to distribute the events across the partitions equally and efficiently.

Each new feature will be developed in a **separate folder** in the repository, and the main Thesis application will remain untouched. This way, we can easily track the changes and the evolution of the application based on the new features.