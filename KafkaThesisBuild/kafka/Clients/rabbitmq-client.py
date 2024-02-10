import pika
import json

# RabbitMQ server details
rabbitmq_host = 'localhost'  # or the IP address/hostname of your Docker host machine
rabbitmq_queue = 'rabbit-queue'
rabbitmq_username = 'nikos'  # make sure this matches your RabbitMQ server credentials
rabbitmq_password = '56312012'  # make sure this matches your RabbitMQ server credentials

# Create a connection to the RabbitMQ server
credentials = pika.PlainCredentials(rabbitmq_username, rabbitmq_password)
parameters = pika.ConnectionParameters(host=rabbitmq_host, credentials=credentials)
connection = pika.BlockingConnection(parameters)

# Create a channel
channel = connection.channel()

# Declare the queue
channel.queue_declare(queue=rabbitmq_queue, durable=True)

# Prepare a message in JSON format
data = {
    'message': 'Testing value',
    'other_key': 'other_value',  # add more key-value pairs as needed
}
message = json.dumps(data)

# Send the message to the queue
channel.basic_publish(exchange='', routing_key=rabbitmq_queue, body=message)

print(f" [x] Sent '{message}'")

# Close the connection
connection.close()