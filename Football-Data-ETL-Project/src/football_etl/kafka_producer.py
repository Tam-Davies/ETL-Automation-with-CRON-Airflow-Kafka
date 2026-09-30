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
    # raw_data = ems([2023, 2024, 2025])

    # Extract all individual matches dynamically from any nesting structure
    matches_list = []
    
    def extract_matches(data):
        if isinstance(data, list):
            for item in data:
                extract_matches(item)
        elif isinstance(data, dict):
            # If this dictionary contains a 'matches' key which is a list, grab it
            if "matches" in data and isinstance(data["matches"], list):
                matches_list.extend(data["matches"])
            else:
                # Otherwise, check all values in the dictionary
                for val in data.values():
                    if isinstance(val, (list, dict)):
                        extract_matches(val)

    if isinstance(raw_data, pd.DataFrame):
        matches_list = raw_data.to_dict(orient="records")
    elif isinstance(raw_data, (list, dict)):
        extract_matches(raw_data)
        # Fallback: if no nested 'matches' key was found, treat the raw_data itself as items
        if not matches_list and isinstance(raw_data, list):
            matches_list = raw_data
        elif not matches_list and isinstance(raw_data, dict):
            matches_list = [raw_data]

    print(f"--- PRODUCER: Starting transmission of {len(matches_list)} matches ---")

    for match in matches_list:
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