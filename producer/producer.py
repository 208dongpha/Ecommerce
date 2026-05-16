from kafka import KafkaProducer
import json
import random
import time
from datetime import datetime, timezone

# Initialize Kafka Producer
# Connecting to the external listener port (29092) on the local Docker host
producer = KafkaProducer(
    bootstrap_servers="localhost:29092",
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Configuration for categories, their specific price ranges, and distribution weights
# Total weight equals 100 for a realistic simulation of product popularity
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

# Simulated user actions representing a typical e-commerce conversion funnel
# Weights reflect actual user behavior (High views/searches, low purchases/refunds)
ACTIONS = [
    "search", "view", "click_ads", "add_to_wishlist", 
    "add_to_cart", "remove_from_cart", "apply_coupon", 
    "purchase", "comment", "return_refund"
]
ACTION_WEIGHTS = [15, 45, 10, 5, 12, 3, 5, 4, 2, 1]  

print("Sending realistic e-commerce events to Kafka... Press Ctrl+C to stop.\n")

try:
    while True:
        # Randomly select a product category based on configured weights
        category = random.choices(
            list(CATEGORY_CONFIG.keys()), 
            weights=[cfg["weight"] for cfg in CATEGORY_CONFIG.values()]
        )[0]
        
        # Generate a random price within the selected category's price range
        min_p, max_p = CATEGORY_CONFIG[category]["price_range"]
        price = random.randint(min_p, max_p)
        
        # Randomly select a user action based on behavior weights
        action = random.choices(ACTIONS, weights=ACTION_WEIGHTS)[0]

        # Apply financial logic: only 'purchase' and 'return_refund' affect cash flow
        if action == "purchase":
            revenue = price
        elif action == "return_refund":
            revenue = -price
        else:
            revenue = 0

        # Construct the streaming event payload
        event = {
            # Standard ISO 8601 format with UTC timezone for downstream accuracy
            "event_time": datetime.now(timezone.utc).isoformat(),
            "user_id": f"USER_{random.randint(1, 1000):04d}", 
            "product_id": f"PROD-{category[:3].upper()}-{random.randint(1, 100):03d}",
            "action": action,
            "price": price,         # List price of the item
            "revenue": revenue,     # Net financial impact (useful for continuous aggregation)
            "category": category
        }

        # Publish the event to the designated Kafka topic
        producer.send("ecommerce-events", value=event)
        print(event)

        # Stream throttling: wait 0.5 seconds before generating the next event
        time.sleep(1)

except KeyboardInterrupt:
    print("\nStopping Producer...")
finally:
    # Ensure all pending messages are reliably sent before shutting down
    producer.flush()
    producer.close()