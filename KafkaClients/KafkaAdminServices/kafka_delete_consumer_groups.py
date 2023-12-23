from confluent_kafka.admin import AdminClient
from confluent_kafka import KafkaException

a = AdminClient({'bootstrap.servers': 'localhost:9092'})

def delete_consumer_groups(a, args):
    """
    Delete Consumer Groups
    """
    groups = a.delete_consumer_groups(args, request_timeout=10)
    for group_id, future in groups.items():
        try:
            future.result()  # The result itself is None
            print("Deleted group with id '" + group_id + "' successfully")
        except KafkaException as e:
            print("Error deleting group id '{}': {}".format(group_id, e))
        except Exception:
            raise
