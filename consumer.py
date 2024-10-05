import pika
import json
from esdbclient import EventStoreDBClient, NewEvent, StreamState

def callback(ch, method, properties, body):
    """Callback to handle incoming RabbitMQ messages and write them to EventStoreDB."""
    event = json.loads(body)
    client = EventStoreDBClient(uri="esdb://localhost:2113?Tls=false")

    event_data = NewEvent(
        type=event["event_type"],
        data=json.dumps(event).encode("utf-8")
    )

    stream_name = method.routing_key

    try:
        client.append_to_stream(
            stream_name=stream_name,
            current_version=StreamState.ANY,  # Allow optimistic concurrency control
            events=[event_data]
        )
        print(f"Event written to EventStoreDB: {event}")
    except Exception as e:
        print(f"Error writing to EventStoreDB: {e}")

def consume_from_rabbitmq():
    """Consumes events from RabbitMQ and writes them to EventStoreDB."""
    connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
    channel = connection.channel()

    # Declare the exchange and queue for binding
    exchange_name = 'ecommerce-exchange'
    channel.exchange_declare(exchange=exchange_name, exchange_type='topic')

    # Create a queue and bind to all topics
    queue_name = 'eventstore-writer-queue'
    channel.queue_declare(queue=queue_name)
    channel.queue_bind(exchange=exchange_name, queue=queue_name, routing_key="#")

    # Consume messages
    channel.basic_consume(queue=queue_name, on_message_callback=callback, auto_ack=True)
    print("Consuming events from RabbitMQ...")

    channel.start_consuming()

if __name__ == "__main__":
    try:
        consume_from_rabbitmq()
    except KeyboardInterrupt:
        print("Event consumption stopped.")
