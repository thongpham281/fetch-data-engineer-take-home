import json
import os

from jsonschema import validate
from kafka import KafkaConsumer

KAFKA_BOOTSTRAP_SERVERS = 'localhost:29092'
KAFKA_CONSUME_TOPIC = 'user-login-processed-production'
KAFKA_CONSUME_GROUP_ID = 'user-login-consumer-group-production'


def main():
    # Define and enforce the schema for messages to handle invalid messages/missing fields
    with open(f'../schema/{KAFKA_CONSUME_TOPIC}.json') as f:
        message_schema = json.load(f)

    consumer = KafkaConsumer(
        KAFKA_CONSUME_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        # auto_offset_reset='earliest',  # Historical data processing
        auto_offset_reset='latest',  # Real-time streaming
        enable_auto_commit=True,
        group_id=KAFKA_CONSUME_GROUP_ID,
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))  # Deserialize JSON from bytes
    )

    print(f"Listening to the '{KAFKA_CONSUME_TOPIC}' topic...")
    try:
        for message in consumer:
            try:
                # Validate before processing
                validate(instance=message.value, schema=message_schema)
                print(f"Valid message received: {message.value}")

            except Exception as e:
                invalid_file_path = os.path.join('../data/invalid', 'data_processed.txt')
                with open(invalid_file_path, 'a') as invalid_file:
                    json.dump(message.value, invalid_file)
                    invalid_file.write('\n')

    except KeyboardInterrupt:
        print("Consumer stopped.")

    finally:
        consumer.close()


if __name__ == '__main__':
    main()
