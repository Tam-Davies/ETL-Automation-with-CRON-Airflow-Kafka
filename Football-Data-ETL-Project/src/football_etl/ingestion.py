import os
from dotenv import load_dotenv
import requests

load_dotenv()
api_key = os.getenv("API_KEY_ACCESS")

headers = {"X-Auth-Token": api_key, "accept": "application/json"}

# Define the seasons you want to extract
seasons = [2023, 2024, 2025, 2026]


def extract_multiple_seasons(season_list):
  """Fetches raw JSON payloads directly from the API for the given seasons

  and returns them as a dictionary"""
  raw_data_map = {}

  for season in season_list:
    params = {"season": season}
    url = "https://api.football-data.org/v4/competitions/PL/matches"

    try:
      response = requests.get(url, headers=headers, params=params)
      response.raise_for_status()

      # Store the raw JSON dictionary for this season
      raw_data_map[season] = response.json()
      print(f"Successfully fetched raw JSON for season {season}")

    except requests.exceptions.HTTPError as e:
      print(f"Failed to fetch season {season}: {e}")

  return raw_data_map


# Self test
if __name__ == "__main__":
  data = extract_multiple_seasons(seasons)
  print(f"Ready to push {len(data)} season payloads to Kafka.")