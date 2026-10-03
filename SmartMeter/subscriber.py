import json
from google.cloud import pubsub_v1


PROJECT_ID = "project-f73d751e-effe-49f9-b0f"
SUBSCRIPTION_ID = "smartmeter_output_sub"


subscriber = pubsub_v1.SubscriberClient()
subscription_path = subscriber.subscription_path(
    PROJECT_ID,
    SUBSCRIPTION_ID
)


def callback(message):
    data = json.loads(message.data.decode("utf-8"))

    print("Received:")
    print(data)
    print()

    message.ack()


streaming_pull_future = subscriber.subscribe(
    subscription_path,
    callback=callback
)

print("Waiting for processed messages...")

with subscriber:
    try:
        streaming_pull_future.result()
    except KeyboardInterrupt:
        streaming_pull_future.cancel()
