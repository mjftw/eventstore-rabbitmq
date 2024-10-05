from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from esdbclient import EventStoreDBClient
import pika
import json

# FastAPI app
app = FastAPI()

# Initialize EventStoreDB client
es_client = EventStoreDBClient(uri="esdb://localhost:2113?Tls=false")

# RabbitMQ connection parameters
rabbitmq_params = pika.ConnectionParameters('localhost')

# Pydantic model to validate request data
class ReplayRequest(BaseModel):
    event_type: str
    target_queue: str

def publish_to_rabbitmq(queue_name, event):
    """Publishes an event to a specified RabbitMQ queue."""
    connection = pika.BlockingConnection(rabbitmq_params)
    channel = connection.channel()

    # Declare the queue to ensure it exists
    channel.queue_declare(queue=queue_name)

    # Convert the event data from bytes to JSON serializable dictionary
    event_data = json.loads(event.decode("utf-8"))

    # Publish the message to the specified queue
    channel.basic_publish(
        exchange='',
        routing_key=queue_name,
        body=json.dumps(event_data),
        properties=pika.BasicProperties(content_type='application/json')
    )

    print(f"Event published to RabbitMQ queue '{queue_name}': {event_data}")
    connection.close()

@app.post("/replay")
async def replay_event(request: ReplayRequest):
    """API endpoint to replay events from EventStoreDB to RabbitMQ."""
    event_type = request.event_type
    target_queue = request.target_queue

    # Fetch historical events from EventStoreDB
    try:
        recorded_events = es_client.get_stream(stream_name=event_type)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error reading from EventStoreDB: {e}")

    # Replay each event by publishing to RabbitMQ
    if not recorded_events:
        raise HTTPException(status_code=404, detail=f"No events found for event type '{event_type}'")

    for event in recorded_events:
        publish_to_rabbitmq(target_queue, event.data)

    return {"message": f"Replayed {len(recorded_events)} events of type '{event_type}' to queue '{target_queue}'."}
