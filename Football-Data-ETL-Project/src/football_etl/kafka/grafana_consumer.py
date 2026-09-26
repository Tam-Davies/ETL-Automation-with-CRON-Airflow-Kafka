import json
from config import (
    CONSUMER_GROUP_GRAFANA,
    KAFKA_BOOTSTRAP_SERVERS,
    KAFKA_TOPIC_MATCHES,
)
from kafka import KafkaConsumer
from prometheus_client import Counter, start_http_server

MATCHES_PROCESSED = Counter(
    "football_matches_processed_total",
    "Total matches processed by real-time stream",
)
TOTAL_GOALS = Counter(
    "football_total_goals_scored", "Cumulative goals scored", ["team_type"]
)


def run_grafana_consumer():
  start_http_server(8000)
  print(
      "--- Grafana Prometheus Exporter running on http://localhost:8000/metrics"
      " ---"
  )

  consumer = KafkaConsumer(
      KAFKA_TOPIC_MATCHES,
      bootstrap_servers=[KAFKA_BOOTSTRAP_SERVERS],
      auto_offset_reset="latest",
      enable_auto_commit=True,
      group_id=CONSUMER_GROUP_GRAFANA,
      value_deserializer=lambda x: json.loads(x.decode("utf-8")),
  )

  for message in consumer:
    match = message.value
    home_score = match.get("home_score") or 0
    away_score = match.get("away_score") or 0

    MATCHES_PROCESSED.inc()
    TOTAL_GOALS.labels(team_type="home").inc(home_score)
    TOTAL_GOALS.labels(team_type="away").inc(away_score)

    print(
        f"[Grafana Stream] Processed: {match['home_team']} {home_score} -"
        f" {away_score} {match['away_team']}"
    )


if __name__ == "__main__":
  run_grafana_consumer()