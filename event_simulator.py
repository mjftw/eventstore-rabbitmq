import uuid
import random
import time
from datetime import datetime
import json
import pika

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
    "UserRegistered": "user-events",
    "ProductViewed": "product-views",
    "ProductAddedToCart": "cart-events",
    "ProductRemovedFromCart": "cart-events",
    "OrderPlaced": "order-events",
    "PaymentProcessed": "payment-events",
    "OrderShipped": "shipping-events",
    "OrderDelivered": "delivery-events",
    "ProductReviewed": "review-events",
    "UserLoggedIn": "user-auth",
    "UserLoggedOut": "user-auth",
    "ProductWishlistAdded": "wishlist-events",
    "ProductWishlistRemoved": "wishlist-events"
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

def publish_event_to_rabbitmq(event, event_type):
    """Publishes an event to RabbitMQ."""
    connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
    channel = connection.channel()

    # Declare the exchange and routing key based on event type
    exchange_name = 'ecommerce-exchange'
    routing_key = event_type

    # Ensure the exchange exists
    channel.exchange_declare(exchange=exchange_name, exchange_type='topic')

    # Publish the message
    channel.basic_publish(
        exchange=exchange_name,
        routing_key=routing_key,
        body=json.dumps(event),
        properties=pika.BasicProperties(content_type='application/json')
    )

    print(f"Event published to RabbitMQ: {event}")
    connection.close()

if __name__ == "__main__":
    try:
        # Use event_generator to produce events
        for event in event_generator(interval=(0.01, 0.5)):  # Adjust interval to control event rate
            publish_event_to_rabbitmq(event, EVENT_TYPES[event["event_type"]])
    except KeyboardInterrupt:
        print("Event generation stopped.")
