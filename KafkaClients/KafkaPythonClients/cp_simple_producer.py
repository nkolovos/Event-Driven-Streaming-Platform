from confluent_kafka import Producer
import sys
import json
import time

if __name__ == '__main__':
    # Specify your Kafka broker and topic
    broker = 'localhost:9092'
    topic = 'testing'

    # Producer configuration
    conf = {'bootstrap.servers': broker}

    # Create Producer instance
    p = Producer(**conf)

    def delivery_callback(err, msg):
        if err:
            sys.stderr.write('%% Message failed delivery: %s\n' % err)
        else:
            sys.stderr.write('%% Message delivered to %s [%d] @ %d\n' %
                             (msg.topic(), msg.partition(), msg.offset()))

    # Prepare the JSON data
    data = {
        "sensorId": "sensor123",
        "latitude": 39.3831,
        "longitude": 22.981,
        "pm25": 15.5,
        "humidity": 40,
        "temperature": 25.3,
        "timestamp": int(time.time() * 1000)  # Current time in milliseconds
    }

    # Convert the data to a JSON string
    data_str = json.dumps(data)

    try:
        # Produce the JSON string to Kafka
        p.produce(topic, data_str, callback=delivery_callback)

    except BufferError:
        sys.stderr.write('%% Local producer queue is full (%d messages awaiting delivery): try again\n' %
                         len(p))

    # Serve delivery callback queue.
    p.poll(0)

    # Wait until all messages have been delivered
    sys.stderr.write('%% Waiting for %d deliveries\n' % len(p))
    p.flush()