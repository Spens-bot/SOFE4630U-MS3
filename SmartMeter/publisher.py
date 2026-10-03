import json
import time
from google.cloud import pubsub_v1
PROJECT_ID = "project-f73d751e-effe-49f9-b0f"
TOPIC_ID = "smartmeter_input"

publisher =pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(PROJECT_ID, TOPIC_ID)

measurements = [{"pressure": 101.3, "temperature": 20.0},
    {"pressure": 98.5, "temperature": 25.0},
    {"pressure": None, "temperature": 22.0},
    {"pressure": 103.2, "temperature": None},
    {"pressure": 100.0, "temperature": 30.0},]

for measurement in measurements:
    message = json.dumps(measurement).encode("utf-8")
    future = publisher.publish(topic_path, message)
    print("Published:", measurement)
    print("Message ID:", future.result())
    time.sleep(1)
