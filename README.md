# Fetch Data Engineer Take Home

## Background

You are working for a company that deals with real-time data processing and analytics. The requirement is to build a
real-time streaming data pipeline using Kafka and Docker. The pipeline should be capable of ingesting streaming data,
processing it in real-time, and storing the processed data into a new Kafka Topic.

## Design Choices

### 1. Kafka for Real-Time Processing:

- Kafka is the central messaging platform in this pipeline, chosen for its durability, scalability, and high throughput.
  It supports fault-tolerant message storage, and partitioned topic architecture.

### 2. Docker for Consistency and Ease of Deployment:

- Docker is used to containerize Kafka and other pipeline components, ensuring consistency across different
  environments. Docker Compose simplifies setup, enabling quick testing and deployment on local machines.

### 3. JSON Schema for Validation:

- Each message follows a predefined JSON schema, enforcing consistency across ingested data. Validation ensures that
  only correctly structured data passes through, which protects downstream processes from schema-related issues.

## Data Flow

### 0. Prerequisite:

- Set up and define Kafka topics.
- Set up and define schema registry for user-login messages (will be used to enforce schema consistency).

### 1. Data Ingestion:

- A simulated data producer in docker-compose generates streaming data in JSON format and sends it to the user-login
  Kafka topic.
- Messages include information like user_id, app_version, device_type, ip, locale, device_id and timestamp.

### 2. Data Consumption and Processing:

- A Kafka consumer (main.py) listens to **user-login**, validates each message against the **predefined schema**, and
  transforms it by adding a readable timestamp.
- Transformed messages are sent to a new Kafka topic **user-login-processed-production** using a Kafka producer.
- **Invalid messages** are captured and pushed back to the **user-login-dead-letter** topic. 
- **consumer_dead_letter.py** is a Kafka consumer that listens to **user-login-dead-letter** topic and handles **invalid cases**.

### 3. Aggregation and Analytics:

- After a defined period, the consumer script (main.py) is stopped to perform data aggregation. Basic analytics (e.g.,
  most popular device type, top locales) are calculated, providing insights into user behavior.

### 4. Testing and Display:

- A separate consumer (consumer.py) reads from **user-login-processed-production** topic to test the output and display
  processed messages.

## How To Run

### 1. Clone the Project

Clone the repository to your local machine:

```bash
git clone https://github.com/thongpham281/fetch-data-engineer-take-home.git
```

### 2. Navigate to Project Directory

Change into the project directory:

```bash
cd fetch-data-engineer-take-home
```

### 3. Project Setup

Ensure you have all the required dependencies installed. Run the following command in the project root directory to
install packages from `requirements.txt`:

```bash
pip install -r requirements.txt
```

### 4. Set Up Kafka and Data Generator

Use Docker Compose to set up Kafka locally along with the data generator that acts as a data producer:

```bash
docker-compose up -d
```

### 5. Start Data Processing in `consumer_dead_letter.py` and `main.py`

Navigate to the `src` directory:

```bash
cd src
```

Run `consumer_dead_letter.py` to consume invalid messages from Kafka, perform basic fix depend on each error case, and produce the fixed data to a old Kafka `user-login` topic:

```bash
python consumer_dead_letter.py
```

Run `main.py` to consume data from Kafka, perform basic transformation, and produce the processed data to a new Kafka topic:

```bash
python main.py
```

Should get the following output:

```text
Valid message received: {'user_id': 'c31f2f84-43e6-446c-a022-d88f9caa677b', 'app_version': '2.3.0', 'ip': '164.116.28.203', 'locale': 'MD', 'device_id': '8d06229a-b32c-4d37-b218-a7566b4e184b', 'timestamp': 1730244200, 'device_type': 'iOS'}
Message delivered to user-login-processed-production
Valid message received: {'user_id': 'da112001-0ac8-43b9-9679-8766dcb12c1b', 'app_version': '2.3.0', 'ip': '245.17.10.63', 'locale': 'NM', 'device_id': '723fcbd1-9467-4c89-8a7c-60437d8ba5e2', 'timestamp': 1730244201, 'device_type': 'iOS'}
Message delivered to user-login-processed-production
```

### 6. Stop `main.py` to Get Aggregated Results

After observing and processing a suitable amount of data, stop `main.py` to analyze the aggregated results.
Should get the similar following output:

```text
Consumer stopped.
All device type count:
        - Counter({'iOS': 116, 'android': 105})

Top 2 device types by login count:
        - iOS: 116 logins
        - android: 105 logins

Top 3 locales by login count:
        - VT: 8 logins
        - MT: 8 logins
        - LA: 7 logins
```

### 7. Test and Display Processed Data with `consumer.py`

To validate and display the processed data, run `consumer.py`:

```bash
python consumer.py
```

Should get the following output with the different in timestamp field:

```text
Valid message received: {'user_id': '833aa9a0-dfc3-4444-84a9-4d748c92cc28', 'app_version': '2.3.0', 'ip': '85.212.232.216', 'locale': 'MA', 'device_id': '536e6032-a32c-4fcd-9546-e0c6d79679f5', 'timestamp': '2024-10-29 16:20:23', 'device_type': 'android'}
Valid message received: {'user_id': '44040ec7-9da7-4b7b-84b6-2c3f73071c72', 'app_version': '2.3.0', 'ip': '95.157.239.136', 'locale': 'MS', 'device_id': 'd46aedf1-6dfc-4f35-911b-da267a6e70c5', 'timestamp': '2024-10-29 16:20:23', 'device_type': 'iOS'}
Valid message received: {'user_id': 'e48feb27-c408-4104-bd15-df766023d2fc', 'app_version': '2.3.0', 'ip': '101.116.160.62', 'locale': 'MT', 'device_id': '66eb5a28-6e14-4a6b-b6e5-5b535e6074f7', 'timestamp': '2024-10-29 16:20:24', 'device_type': 'iOS'}
```

## Additional Questions

### 1. How would you deploy this application in production?

- To deploy this application in production, I would use **Kubernetes** to manage
  and scale the Docker containers running Kafka, data producers, and consumers. Kafka would be set up as a managed
  service for easier scaling and reliability.
- **Apache Airflow**  would be introduced to manage pipeline workflows.

### 2. What other components would you want to add to make this production-ready?

- **Monitoring and Logging**: Use **Prometheus** and **Grafana** for monitoring system metrics (CPU, memory usage, etc.)
  and Kafka-specific metrics (consumer lag, message throughput).
- **Schema Registry**: Add a better solution (Datahub) to enforce schema consistency and prevent incompatible messages.
- **Error Handling & Retry Mechanism**: Implement and improve error handling and a retry mechanism, where failed
  messages can be retried or stored in a **dead-letter queue** / **cloud storage** for later analysis.
- **CI/CD Pipelines**: Integrate **Jenkins** to automate testing, building, and deployment workflows.
- **Data Warehouse Integration**: Set up and integrate with a data warehouse like **Amazon Redshift** or **Snowflake**
  to store processed data for further analytics.

### 3. How can this application scale with a growing dataset?

- This application can scale by **horizontally scaling Kafka consumers** across multiple partitions, enabling parallel
  data processing.
- Using auto-scaling **Kubernetes** clusters will allow for on-demand scaling based on traffic or processing needs.