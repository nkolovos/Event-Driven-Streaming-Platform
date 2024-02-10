from confluent_kafka.admin import AdminClient

def delete_topics(a, topics):
    """ Delete topics """

    fs = a.delete_topics(topics, operation_timeout=30)

    for topic, f in fs.items():
        try:
            f.result()  # The result itself is None
            print("Topic {} deleted".format(topic))
        except Exception as e:
            print("Failed to delete topic {}: {}".format(topic, e))

# Create an AdminClient
a = AdminClient({'bootstrap.servers': 'localhost:9092'})

# Create a list of topic names
topics = ['avro-sensor{}'.format(i) for i in range(1, 101)]

# Call the function to delete the topics
delete_topics(a, topics)