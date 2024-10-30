import json
import os

from jsonschema import validate
from kafka import KafkaConsumer

KAFKA_BOOTSTRAP_SERVERS = 'localhost:29092'
KAFKA_CONSUME_TOPIC = 'user-login-dead-letter'
KAFKA_CONSUME_GROUP_ID = 'user-login-consumer-group-production-4'


def main():
    # Define and enforce the schema for messages to handle invalid messages/missing fields
    original_schema = f'../schema/user-login-processed-production.json'
    with open(original_schema) as f:
        message_schema = json.load(f)

    consumer = KafkaConsumer(
        KAFKA_CONSUME_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        auto_offset_reset='earliest',  # Historical data processing
        # auto_offset_reset='latest',  # Real-time streaming
        enable_auto_commit=True,
        group_id=KAFKA_CONSUME_GROUP_ID,
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))  # Deserialize JSON from bytes
    )

    print(f"Listening to the '{KAFKA_CONSUME_TOPIC}' topic...")
    try:
        for invalid_message in consumer:
            try:
                # Handle invalid messages based on each scenario, if SUCCESS, push back to user-logic topic
                print(f"Valid message received: {invalid_message.value}")
                pass

            except Exception as e:
                # Unpredicted error will be stored in the 'invalid' folder
                invalid_file_path = os.path.join('../data/invalid', 'unpredicted_invalid_msg.txt')
                with open(invalid_file_path, 'a') as invalid_file:
                    json.dump(invalid_message.value, invalid_file)
                    invalid_file.write('\n')


    except KeyboardInterrupt:
        print("Consumer stopped.")

    finally:
        consumer.close()


if __name__ == '__main__':
    main()
