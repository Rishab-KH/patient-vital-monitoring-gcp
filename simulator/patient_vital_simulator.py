import time
import json
import random
import os
from google.cloud import pubsub_v1
from dotenv import load_dotenv
from google.api_core.exceptions import AlreadyExists

# Load environment variables from .env file
load_dotenv()

PROJECT_ID = os.getenv("GCP_PROJECT")
TOPIC_ID = os.getenv("PUBSUB_TOPIC")
SUBSCRIPTION_ID = os.getenv("PUBSUB_SUBSCRIPTION")
PATIENT_COUNT = int(os.getenv("PATIENT_COUNT", 20))
STREAM_INTERVAL = int(os.getenv("STREAM_INTERVAL", 2))
# fraction of error records
ERROR_RATE = float(os.getenv("ERROR_RATE", 0.1)) 

# Pub/Sub client
publisher = pubsub_v1.PublisherClient()
subscriber = pubsub_v1.SubscriberClient()

topic_path = publisher.topic_path(PROJECT_ID, TOPIC_ID)
subscription_path = subscriber.subscription_path(PROJECT_ID, SUBSCRIPTION_ID)

# Create topic if it doesn't exist

try:
    topic = publisher.create_topic(request={"name": topic_path})
    print(f"Created topic: {topic.name}")
except AlreadyExists:
    print(f"Topic already exists: {topic_path}")

# Create subscription

try:
    subscriber.create_subscription(
        request={
            "name": subscription_path,
            "topic": topic_path,
        }
    )
    print(f"Created subscription: {subscription_path}")
except AlreadyExists:
    print(f"Subscription already exists: {subscription_path}")

# Generate list of patient IDs
patient_ids = [f"P{i:03d}" for i in range(1, PATIENT_COUNT + 1)]

def generate_vitals():
    """Generate one patient vital record with a chance to inject errors."""
    record = {
        "patient_id": random.choice(patient_ids),
        "timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "heart_rate": round(random.uniform(60, 120), 1),
        "spo2": round(random.uniform(90, 100), 1),
        "temperature": round(random.uniform(36.0, 39.0), 1),
        "bp_systolic": round(random.uniform(100, 140), 1),
        "bp_diastolic": round(random.uniform(60, 90), 1)
    }

    # Inject errors based on ERROR_RATE
    if random.random() < ERROR_RATE:
        error_type = random.choice(["missing_field", "negative_value", "out_of_range"])
        if error_type == "missing_field":
            # remove a random field
            field_to_remove = random.choice(list(record.keys()))
            record[field_to_remove] = None
        elif error_type == "negative_value":
            record["heart_rate"] = -1
        elif error_type == "out_of_range":
            record["spo2"] = 150  # invalid SpO2
    return record

print("Starting patient vitals simulator with error injection... Press Ctrl+C to stop.")

while True:
    vitals = generate_vitals()
    message_json = json.dumps(vitals)
    publisher.publish(topic_path, message_json.encode("utf-8"))
    print(f"Published: {message_json}")
    time.sleep(STREAM_INTERVAL)