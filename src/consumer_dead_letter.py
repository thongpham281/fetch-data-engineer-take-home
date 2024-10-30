import json
import os
import uuid

from confluent_kafka import Producer
from jsonschema import validate
from kafka import KafkaConsumer

import producer as producer_module

KAFKA_BOOTSTRAP_SERVERS = 'localhost:29092'
KAFKA_CONSUME_ORIGINAL_TOPIC = 'user-login'
KAFKA_CONSUME_TOPIC = 'user-login-dead-letter'
KAFKA_CONSUME_GROUP_ID = 'user-login-consumer-group-production'


def main():
    # Define and enforce the schema for messages to handle invalid messages/missing fields
    original_schema = f'../schema/user-login.json'
    with open(original_schema) as f:
        message_schema = json.load(f)

    producer = Producer(
        {
            'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
        }
    )

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
                consumed_msg = invalid_message.value
                print(f"Valid message received: {consumed_msg}")

                # Handle missing fields based on business logic
                missed_cols = []
                for col in message_schema['required']:
                    if col not in consumed_msg:
                        missed_cols.append(col)
                if missed_cols:
                    for col in missed_cols:
                        if message_schema['properties'][col]['type'] == 'string':
                            consumed_msg[col] = ''
                        elif message_schema['properties'][col]['type'] == 'number':
                            consumed_msg[col] = 0

                validate(instance=consumed_msg, schema=message_schema)

                # Procedure fixed message to original topic
                producer_module.main(producer, KAFKA_CONSUME_ORIGINAL_TOPIC, str(uuid.uuid4()),
                                     json.dumps(consumed_msg))


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
        producer.flush()


if __name__ == '__main__':
    main()
