import json
import os
import uuid
from collections import Counter

from confluent_kafka import Producer
from jsonschema import validate
from kafka import KafkaConsumer
from kafka.admin import KafkaAdminClient, NewTopic

import aggregator
import producer as producer_module
import transformer

KAFKA_BOOTSTRAP_SERVERS = 'localhost:29092'
KAFKA_CONSUME_TOPIC = 'user-login'
KAFKA_CONSUME_GROUP_ID = 'user-login-consumer-group-production'
KAFKA_PROCEDURE_TOPIC = 'user-login-processed-production'
KAFKA_DEAD_LETTER_TOPIC = 'user-login-dead-letter'


def main():
    # Check if the 'user-login' topic exists
    admin_client = KafkaAdminClient(bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS)
    topic_exists = KAFKA_PROCEDURE_TOPIC in admin_client.list_topics()
    if not topic_exists:
        new_topic = NewTopic(name=KAFKA_PROCEDURE_TOPIC, num_partitions=1, replication_factor=1)
        try:
            print(f"Creating topic '{KAFKA_PROCEDURE_TOPIC}'...")
            admin_client.create_topics(new_topics=[new_topic], validate_only=False)
        except TopicAlreadyExistsError:
            pass

    # Define and enforce the schema for messages to handle invalid messages/missing fields
    with open(f'../schema/{KAFKA_CONSUME_TOPIC}.json') as f:
        message_schema = json.load(f)

    producer = Producer(
        {
            'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
        }
    )

    consumer = KafkaConsumer(
        KAFKA_CONSUME_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        # auto_offset_reset='earliest',  # Historical data processing
        auto_offset_reset='latest',  # Real-time streaming
        enable_auto_commit=True,
        group_id=KAFKA_CONSUME_GROUP_ID,
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))  # Deserialize JSON from bytes
    )

    # Data aggregation structures
    device_type_count = Counter()
    locale_count = Counter()

    print(f"Listening to the '{KAFKA_CONSUME_TOPIC}' topic...")
    try:
        for message in consumer:
            try:
                # Validate before processing
                validate(instance=message.value, schema=message_schema)
                print(f"Valid message received: {message.value}")

                # Perform basic processing on the message
                processed_message = transformer.transform(message.value)

                # Extract and analyze data by device type and locale
                device_type = processed_message['device_type']
                locale = processed_message['locale']
                device_type_count[device_type] += 1
                locale_count[locale] += 1

                # Procedure processed message to new topic
                producer_module.main(producer, KAFKA_PROCEDURE_TOPIC, str(uuid.uuid4()), json.dumps(processed_message))

            except Exception as e:
                # We are going to store invalid messages in dead-letter topic in raw format
                producer_module.main(producer, KAFKA_DEAD_LETTER_TOPIC, str(uuid.uuid4()), json.dumps(message.value))


    except KeyboardInterrupt:
        print("Consumer stopped.")

    finally:
        # Aggregated results after stopping the consumer
        aggregator.aggregate_device(device_type_count)
        aggregator.aggregate_locale(locale_count)

        consumer.close()
        producer.flush()


if __name__ == '__main__':
    main()
