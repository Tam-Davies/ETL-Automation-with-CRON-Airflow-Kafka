# Kafka (KRaft) and Python Setup Guide

This project uses Apache Kafka 4.x in KRaft mode and Python's `kafka-python` client. Kafka runs locally at `127.0.0.1:9092`; the Python examples create and use a `bankbranch` topic.

## 1. Prerequisites and Python environment

Linux distributions that enforce PEP 668 require Python packages to be installed in a virtual environment. From the project directory, create and activate one, then install the client library:

```bash
python3 -m venv venv
source venv/bin/activate
python -m pip install --upgrade pip
python -m pip install kafka-python
```

If `kafka-python` has compatibility issues with your Python version, try its maintained alternative instead:

```bash
python -m pip install kafka-python-ng
```

Use one client package at a time in the active environment.

## 2. Format Kafka storage (first-time setup)

Kafka 4.x runs in KRaft mode and does not require ZooKeeper. Generate a cluster ID and format the storage configured in `config/server.properties`:

```bash
KAFKA_CLUSTER_ID="$(bin/kafka-storage.sh random-uuid)"
echo "Cluster ID: $KAFKA_CLUSTER_ID"
bin/kafka-storage.sh format -t "$KAFKA_CLUSTER_ID" -c config/server.properties --standalone
```

Formatting is a one-time step for a fresh storage directory. Keep the generated cluster ID for this Kafka data directory; do not format it again when starting the broker.

## 3. Start the Kafka broker

In a dedicated terminal, from the project directory, start Kafka and leave it running:

```bash
bin/kafka-server-start.sh config/server.properties
```

## 4. Create the topic

In another terminal, activate the virtual environment and run the project's admin script:

```bash
source venv/bin/activate
python admin.py
```

The script in `admin.py` connects to `127.0.0.1:9092` and creates `bankbranch` with two partitions and a replication factor of one. Kafka reports an error if the topic already exists.

## 5. Produce and consume messages

The examples below assume the broker is running and `bankbranch` has been created. Run each command from the project directory with the virtual environment active.

Send two sample transactions:

```bash
python producer.py
```

Interactively send transactions for ATM 1 or 2; enter `n` to stop:

```bash
python new_producer.py
```

Read messages from the beginning of the topic:

```bash
python consumer.py
```

The consumer runs continuously until interrupted with `Ctrl+C`.