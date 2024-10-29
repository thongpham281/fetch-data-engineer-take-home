from confluent_kafka import Producer


# Define a delivery callback function
def delivery_report(err, msg):
    if err is not None:
        print(f"Message delivery failed: {err}")
    else:
        print(f"Message delivered to {msg.topic()}")


def main(producer: Producer, topic, key, value):
    producer.produce(
        topic,
        key=key,
        value=value,
        callback=delivery_report
    )
    producer.flush()
