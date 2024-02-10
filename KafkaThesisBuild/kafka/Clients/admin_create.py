from confluent_kafka.admin import AdminClient, NewTopic

def create_topics(a, topics):
    """ Create topics """

    new_topics = [NewTopic(topic, num_partitions=1, replication_factor=3) for topic in topics]
    fs = a.create_topics(new_topics)

    for topic, f in fs.items():
        try:
            f.result()
            print("Topic {} created".format(topic))
        except Exception as e:
            print("Failed to create topic {}: {}".format(topic, e))

# Create an AdminClient
a = AdminClient({'bootstrap.servers': 'localhost:9092'})

# Create a list of topic names
topics = ['avro-sensor{}'.format(i) for i in range(1, 101)]

# Call the function to create the topics
create_topics(a, topics)