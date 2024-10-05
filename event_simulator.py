import uuid
import random
import time
from datetime import datetime
import json
from esdbclient import EventStoreDBClient, NewEvent, StreamState

# Expanded list of sample products and users
PRODUCTS = [
    {"product_id": str(uuid.uuid4()), "name": "Laptop", "price": 1200},
    {"product_id": str(uuid.uuid4()), "name": "Smartphone", "price": 800},
    {"product_id": str(uuid.uuid4()), "name": "Headphones", "price": 150},
    {"product_id": str(uuid.uuid4()), "name": "Smartwatch", "price": 250},
    {"product_id": str(uuid.uuid4()), "name": "Tablet", "price": 600},
    {"product_id": str(uuid.uuid4()), "name": "Gaming Console", "price": 400},
    {"product_id": str(uuid.uuid4()), "name": "Camera", "price": 900},
    {"product_id": str(uuid.uuid4()), "name": "Wireless Charger", "price": 50},
    {"product_id": str(uuid.uuid4()), "name": "Bluetooth Speaker", "price": 120},
    {"product_id": str(uuid.uuid4()), "name": "Keyboard", "price": 80}
]

USERS = [
    {"user_id": str(uuid.uuid4()), "username": "alice"},
    {"user_id": str(uuid.uuid4()), "username": "bob"},
    {"user_id": str(uuid.uuid4()), "username": "charlie"},
    {"user_id": str(uuid.uuid4()), "username": "diana"},
    {"user_id": str(uuid.uuid4()), "username": "edward"},
    {"user_id": str(uuid.uuid4()), "username": "frank"},
    {"user_id": str(uuid.uuid4()), "username": "georgia"},
    {"user_id": str(uuid.uuid4()), "username": "harry"},
    {"user_id": str(uuid.uuid4()), "username": "ivy"},
    {"user_id": str(uuid.uuid4()), "username": "jack"}
]

EVENT_TYPES = {
    "UserRegistered": "user-events-stream",
    "ProductViewed": "product-views-stream",
    "ProductAddedToCart": "cart-events-stream",
    "ProductRemovedFromCart": "cart-events-stream",
    "OrderPlaced": "order-events-stream",
    "PaymentProcessed": "payment-events-stream",
    "OrderShipped": "shipping-events-stream",
    "OrderDelivered": "delivery-events-stream",
    "ProductReviewed": "review-events-stream",
    "UserLoggedIn": "user-auth-stream",
    "UserLoggedOut": "user-auth-stream",
    "ProductWishlistAdded": "wishlist-events-stream",
    "ProductWishlistRemoved": "wishlist-events-stream"
}

def simulate_event(users, products, event_types):
    """Simulates a realistic e-commerce event."""
    user = random.choice(users)
    product = random.choice(products)

    event_type = random.choice(list(event_types.keys()))
    event = {
        "event_id": str(uuid.uuid4()),
        "event_type": event_type,
        "timestamp": datetime.utcnow().isoformat(),
        "user_id": user["user_id"],
        "username": user["username"]
    }

    if event_type in ["ProductViewed", "ProductAddedToCart", "ProductRemovedFromCart", "ProductReviewed", "ProductWishlistAdded", "ProductWishlistRemoved"]:
        event.update({
            "product_id": product["product_id"],
            "product_name": product["name"],
            "price": product["price"]
        })

    elif event_type == "OrderPlaced":
        num_products = random.randint(1, 3)
        order_products = random.sample(products, num_products)
        total_price = sum([p["price"] for p in order_products])
        event.update({
            "order_id": str(uuid.uuid4()),
            "products": [{"product_id": p["product_id"], "name": p["name"], "price": p["price"]} for p in order_products],
            "total_price": total_price
        })

    elif event_type == "PaymentProcessed":
        event.update({
            "order_id": str(uuid.uuid4()),
            "amount": random.randint(500, 2000),
            "payment_status": random.choice(["Success", "Failed"])
        })

    elif event_type in ["OrderShipped", "OrderDelivered"]:
        event.update({"order_id": str(uuid.uuid4())})

    return event

def event_generator(event_types=EVENT_TYPES, users=USERS, products=PRODUCTS, interval=(0.5, 2.0)):
    """Generates an infinite stream of e-commerce events with adjustable parameters."""
    while True:
        yield simulate_event(users, products, event_types)
        time.sleep(random.uniform(*interval))  # Random delay between events to mimic real traffic

# Example Usage: Push Data into EventStoreDB
def write_to_eventstore(event, event_types):
    """Writes an event to EventStoreDB."""
    # Use esdbclient to connect to EventStoreDB
    client = EventStoreDBClient(uri="esdb://localhost:2113?Tls=false")

    # Create a NewEvent object for the event
    event_data = NewEvent(
        type=event["event_type"],
        data=json.dumps(event).encode("utf-8")
    )

    # Get stream name based on event type
    stream_name = event_types[event["event_type"]]

    # Append the new event to the appropriate stream
    try:
        client.append_to_stream(
            stream_name=stream_name,
            current_version=StreamState.ANY,  # Allow optimistic concurrency control
            events=[event_data]
        )
        print(f"Event written to {stream_name}: {event}")
    except Exception as e:
        print(f"Error writing to EventStoreDB: {e}")

if __name__ == "__main__":
    try:
        for event in event_generator(interval=(0.01, 0.1)):  # Adjust interval to control event rate
            write_to_eventstore(event, EVENT_TYPES)
    except KeyboardInterrupt:
        print("Event generation stopped.")
