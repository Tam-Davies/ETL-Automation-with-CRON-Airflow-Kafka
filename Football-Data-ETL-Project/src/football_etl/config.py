import os
from dotenv import load_dotenv

load_dotenv()

# API & Database Credentials
API_KEY_ACCESS = os.getenv("API_KEY_ACCESS")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
DB_NAME = os.getenv("DB_NAME")

# Kafka Settings
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS")
KAFKA_TOPIC_MATCHES = os.getenv("KAFKA_TOPIC_MATCHES")

# Consumer Group IDs
# CONSUMER_GROUP_POSTGRES = os.getenv("CONSUMER_GROUP_POSTGRES")
# CONSUMER_GROUP_GRAFANA = os.getenv("CONSUMER_GROUP_GRAFANA")