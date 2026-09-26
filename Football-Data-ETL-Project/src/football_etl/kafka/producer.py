import json
import time
from config import API_KEY_ACCESS, KAFKA_BOOTSTRAP_SERVERS, KAFKA_TOPIC_MATCHES
from ingestion import extract_multiple_seasons as ems
from kafka import KafkaProducer


def run_producer():
  producer = KafkaProducer(
      bootstrap_servers=[KAFKA_BOOTSTRAP_SERVERS],
      value_serializer=lambda v: json.dumps(v, default=str).encode("utf-8"),
  )

  print(
      f"--- Fetching match data from API to publish to {KAFKA_TOPIC_MATCHES}"
      " ---"
  )

  # Fetch matches 
  df_matches = ems([2023, 2024, 2025])
  matches_list = df_matches.to_dict(orient="records")

  print(f"--- PRODUCER: Starting transmission of {len(matches_list)} matches ---")

  for match in matches_list:
    producer.send(KAFKA_TOPIC_MATCHES, value=match)
    print(
        f"Sent -> Match ID: {match['match_id']} | {match['home_team']} vs"
        f" {match['away_team']}"
    )
    time.sleep(0.01)

  producer.flush()
  print("--- PRODUCER: All match records successfully published to Kafka! ---")


if __name__ == "__main__":
  run_producer()