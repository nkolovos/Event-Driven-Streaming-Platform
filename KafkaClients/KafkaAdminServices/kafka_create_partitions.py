from confluent_kafka.admin import AdminClient, NewPartitions

a = AdminClient({'bootstrap.servers': 'localhost:9092'})

def create_partitions(a, topics):
    """ create partitions """

    new_partions = [NewPartitions(topic, int(new_total_count)) for
                 topic, new_total_count in zip(topics[0::2], topics[1::2])]

    # Try switching validate_only to True to only validate the operation
    # on the broker but not actually perform it.
    fs = a.create_partitions(new_partions, validate_only=False)

    # Wait for operation to finish.
    for topic, f in fs.items():
        try:
            f.result()  # The result itself is None
            print("Additional partitions created for topic {}".format(topic))
        except Exception as e:
            print("Failed to add partitions to topic {}: {}".format(topic, e))