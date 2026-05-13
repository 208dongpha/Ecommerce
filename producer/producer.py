from kafka import KafkaProducer
import json
import random
import time
from datetime import datetime

producer = KafkaProducer (
    bootstrap_servers="localhost:29092",
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

actions = [
    "view",
    "add_to_cart",
    'purchase'
]

categories = [
    "electronics",
    "fashion",
    "books",
    "sports"
]

print("Sending events to Kafka...\n")

while True:

    event = {
        "event_time": str(datetime.now()),
        "user_id": random.randint(1, 1000),
        "product_id": random.randint(1, 100),
        "action": random.choice(actions),
        "price": random.randint(10, 1000),
        "category": random.choice(categories)
    }

    producer.send(
        "ecommerce-events",
        value=event
    )

    print(event)

    time.sleep(1)