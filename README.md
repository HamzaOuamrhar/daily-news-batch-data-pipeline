# .env example

## Image version for Airflow
AIRFLOW_IMAGE_NAME=apache/airflow:3.0.2

## PostgreSQL credentials
POSTGRES_USER=airflow <br>
POSTGRES_PASSWORD=airflow <br>
POSTGRES_DB=airflow

## Local project directory (used to mount dags/, logs/, config/, plugins/)
AIRFLOW_PROJ_DIR=.

## Optional: Airflow Web UI credentials
_AIRFLOW_WWW_USER_USERNAME=airflow <br>
_AIRFLOW_WWW_USER_PASSWORD=airflow

## Optional: Additional Python packages to install
_PIP_ADDITIONAL_REQUIREMENTS=

## UID to avoid permission issues (especially on Linux)
AIRFLOW_UID=50000



# Useful Kafka commands

## Topic management

### List all topics
kafka-topics.sh --bootstrap-server $BROKER --list

### Create a new topic (1 partition, replication factor 1)
kafka-topics.sh --bootstrap-server $BROKER --create \
  --topic my-topic \
  --partitions 1 \
  --replication-factor 1

### Describe a topic
kafka-topics.sh --bootstrap-server $BROKER --describe --topic my-topic

### Delete a topic
kafka-topics.sh --bootstrap-server $BROKER --delete --topic my-topic

## Producing Messages

### Start a producer for a topic
kafka-console-producer.sh --bootstrap-server $BROKER --topic my-topic

### Send messages from a file
kafka-console-producer.sh --bootstrap-server $BROKER --topic my-topic < messages.txt

## Consuming Messages

### Consume messages from a topic (from beginning)
kafka-console-consumer.sh --bootstrap-server $BROKER --topic my-topic --from-beginning

### Consume with consumer group (commits offsets)
kafka-console-consumer.sh --bootstrap-server $BROKER \
  --topic my-topic \
  --group my-consumer-group \
  --from-beginning

### List all consumer groups
kafka-consumer-groups.sh --bootstrap-server $BROKER --list

### Describe a consumer group
kafka-consumer-groups.sh --bootstrap-server $BROKER \
  --describe --group my-consumer-group

### Delete a consumer group (if needed)
kafka-consumer-groups.sh --bootstrap-server $BROKER \
  --delete --group my-consumer-group


## Kafka Storage (KRaft only)

### Generate a new UUID for your cluster
kafka-storage.sh random-uuid

### Format storage directory (only run once per broker!)
kafka-storage.sh format \
  --cluster-id kraft-cluster-id \
  -c /path/to/server.properties

## Cleanup (for Dev/Local Use)

### Delete topic data manually (be careful!)
rm -rf /tmp/kafka-logs /tmp/kraft-combined-logs

### Reset offsets for a consumer group (dangerous)
kafka-consumer-groups --bootstrap-server $BROKER \
  --group my-consumer-group \
  --topic my-topic \
  --reset-offsets --to-earliest --execute

# Spark commands

## Check Spark version
spark-submit --version

## Run a PySpark script
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,org.apache.kafka:kafka-clients:3.5.1 /opt/spark-apps/kafka_consumer.py

# psql commands for postgres

\l => List all databases
\c <dbname> => Connect to a database
\du => List all users/roles
\dt => List tables in the current database
\d <tablename> => Describe table structure
\q => Quit psql shell
