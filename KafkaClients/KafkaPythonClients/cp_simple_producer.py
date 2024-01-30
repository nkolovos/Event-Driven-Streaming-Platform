from confluent_kafka import Producer
import sys
import json
import time

if __name__ == '__main__':
    # Specify your Kafka broker and topic
    broker = 'localhost:9092'
    topic = 'simple-sensor1'

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

    try:
        while True:
            # Prepare the JSON data
            data = {
                "deviceID": "sensor1",
                "latitude": 39.36103,
                "longitude": 22.99248,
                "pm25": 40,
                "humidity": 40,
                "temperature": 40,
                "timestamp": int(time.time())  # Current time in milliseconds
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

            # Wait for 5 seconds before sending the next message
            time.sleep(5)

    except KeyboardInterrupt:
        sys.stderr.write('%% Aborted by user\n')

    finally:
        # Ensure all messages have been delivered before exiting
        p.flush()