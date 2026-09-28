import json
import time
import pandas as pd
from kafka import KafkaProducer
from football_etl.config import API_KEY_ACCESS, KAFKA_BOOTSTRAP_SERVERS, KAFKA_TOPIC_MATCHES
from football_etl.ingestion import extract_multiple_seasons as ems


def run_producer():
    producer = KafkaProducer(
        bootstrap_servers=[KAFKA_BOOTSTRAP_SERVERS],
        value_serializer=lambda v: json.dumps(v, default=str).encode("utf-8"),
    )

    print(
        f"--- Fetching match data from API to publish to {KAFKA_TOPIC_MATCHES}"
        " ---"
    )

    # Fetch matches using your API key and ingestion logic
    raw_data = ems([2023, 2024, 2025, 2026])

    # Flatten and extract matches properly from the API payload structure
    matches_list = []
    if isinstance(raw_data, pd.DataFrame):
        matches_list = raw_data.to_dict(orient="records")
    elif isinstance(raw_data, list):
        for item in raw_data:
            if isinstance(item, dict) and "matches" in item:
                matches_list.extend(item["matches"])
            else:
                matches_list.append(item)
    elif isinstance(raw_data, dict):
        matches_list = raw_data.get("matches", [raw_data])

    print(f"--- PRODUCER: Starting transmission of {len(matches_list)} matches ---")

    for match in matches_list:
        # Handle different API response field naming conventions safely
        match_id = match.get('id') or match.get('match_id', 'N/A')
        home_team = match.get('homeTeam', {}).get('name') or match.get('home_team', 'Home')
        away_team = match.get('awayTeam', {}).get('name') or match.get('away_team', 'Away')

        producer.send(KAFKA_TOPIC_MATCHES, value=match)
        print(f"Sent -> Match ID: {match_id} | {home_team} vs {away_team}")
        time.sleep(0.01)

    producer.flush()
    print("--- PRODUCER: All match records successfully published to Kafka! ---")


if __name__ == "__main__":
    run_producer()