import os
from dotenv import load_dotenv

load_dotenv()

# Kafka Broker Settings
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS")
KAFKA_TOPIC_MATCHES = os.getenv("KAFKA_TOPIC_MATCHES")

# Consumer Group IDs
CONSUMER_GROUP_POSTGRES = "postgres-loader-group"
CONSUMER_GROUP_GRAFANA = "grafana-realtime-group"
