from kafka import KafkaProducer
from kafka.errors import KafkaError
import json
import random
import time
from datetime import datetime, timezone

# =========================
# Kafka Producer
# =========================
producer = KafkaProducer(
    bootstrap_servers="localhost:29092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),

    # reliability
    acks='all',
    retries=10,

    # batching
    linger_ms=10,

    # timeout
    request_timeout_ms=30000,
    max_block_ms=60000
)

CATEGORY_CONFIG = {
    "electronics": {"price_range": (100, 1500), "weight": 10},
    "fashion": {"price_range": (15, 150), "weight": 20},
    "books": {"price_range": (5, 50), "weight": 10},
    "sports": {"price_range": (20, 300), "weight": 10},
    "groceries": {"price_range": (2, 30), "weight": 25},
    "home_living": {"price_range": (20, 500), "weight": 12},
    "beauty_health": {"price_range": (8, 150), "weight": 10},
    "toys_baby": {"price_range": (5, 100), "weight": 3}
}

ACTIONS = [
    "search",
    "view",
    "click_ads",
    "add_to_wishlist",
    "add_to_cart",
    "remove_from_cart",
    "apply_coupon",
    "purchase",
    "comment",
    "return_refund"
]

ACTION_WEIGHTS = [15, 45, 10, 5, 12, 3, 5, 4, 2, 1]

print("Sending events to Kafka...\n")

try:

    while True:

        category = random.choices(
            list(CATEGORY_CONFIG.keys()),
            weights=[cfg["weight"] for cfg in CATEGORY_CONFIG.values()]
        )[0]

        min_p, max_p = CATEGORY_CONFIG[category]["price_range"]
        price = random.randint(min_p, max_p)

        action = random.choices(
            ACTIONS,
            weights=ACTION_WEIGHTS
        )[0]

        if action == "purchase":
            revenue = price
        elif action == "return_refund":
            revenue = -price
        else:
            revenue = 0

        event = {
            "event_time": datetime.now(timezone.utc).isoformat(),
            "user_id": f"USER_{random.randint(1,1000):04d}",
            "product_id": f"PROD-{category[:3].upper()}-{random.randint(1,100):03d}",
            "action": action,
            "price": price,
            "revenue": revenue,
            "category": category
        }

        future = producer.send(
            "ecommerce-events",
            value=event
        )

        # force send + catch errors
        record_metadata = future.get(timeout=10)

        print(
            f"[OK] topic={record_metadata.topic} "
            f"partition={record_metadata.partition} "
            f"offset={record_metadata.offset}"
        )

        print(event)

        producer.flush()

        time.sleep(1)

except KeyboardInterrupt:
    print("\nStopping producer...")

except KafkaError as e:
    print(f"\nKafka Error: {e}")

except Exception as e:
    print(f"\nUnexpected Error: {e}")

finally:
    producer.flush()
    producer.close()