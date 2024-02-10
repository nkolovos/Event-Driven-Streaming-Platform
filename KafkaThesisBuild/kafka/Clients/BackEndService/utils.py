import logging

def shutdown(signal, loop, simple_consumer, avro_consumer, websocket_clients):
    """Shutdown gracefully on SIGINT or SIGTERM."""
    
    logging.info("Received exit signal, shutting down...")
    logging.info("Closing Kafka consumers...")

    # Close the Kafka consumers
    simple_consumer.close()  # Close simple Kafka consumer
    avro_consumer.close()  # Close Avro Kafka consumer

    # Close all WebSocket connections
    logging.info("Closing WebSocket clients...")
    for client in websocket_clients:
        loop.run_until_complete(client.close())

    # Stop the asyncio event loop
    loop.stop()