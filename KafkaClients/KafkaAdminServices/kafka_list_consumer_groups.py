from confluent_kafka.admin import AdminClient
from confluent_kafka import ConsumerGroupState

a = AdminClient({'bootstrap.servers': 'localhost:9092'})

def list_consumer_groups(a, args):
    """
    List Consumer Groups
    """
    states = {ConsumerGroupState[state] for state in args}
    future = a.list_consumer_groups(request_timeout=10, states=states)
    try:
        list_consumer_groups_result = future.result()
        print("{} consumer groups".format(len(list_consumer_groups_result.valid)))
        for valid in list_consumer_groups_result.valid:
            print("    id: {} is_simple: {} state: {}".format(
                valid.group_id, valid.is_simple_consumer_group, valid.state))
        print("{} errors".format(len(list_consumer_groups_result.errors)))
        for error in list_consumer_groups_result.errors:
            print("    error: {}".format(error))
    except Exception:
        raise