import os
from dotenv import load_dotenv
import pandas as pd
import requests

load_dotenv()
api_key = os.getenv("API_KEY_ACCESS")

headers = {"X-Auth-Token": api_key, "accept": "application/json"}

# Define the seasons you want to extract
seasons = [2023, 2024, 2025, 2026]


def extract_multiple_seasons(season_list):
  all_parsed_matches = []

  for season in season_list:
    params = {"season": season}
    url = "https://api.football-data.org/v4/competitions/PL/matches"

    try:
      response = requests.get(url, headers=headers, params=params)
      response.raise_for_status()

      data = response.json()
      matches_list = data.get("matches", [])

      for match in matches_list:
        match_record = {
            "season": season,
            "match_id": match.get("id"),
            "matchday": match.get("matchday"),
            "date": match.get("utcDate"),
            "status": match.get("status"),
            "home_team": match.get("homeTeam", {}).get("name"),
            "away_team": match.get("awayTeam", {}).get("name"),
            "home_score": match.get("score", {}).get("fullTime", {}).get("home"),
            "away_score": match.get("score", {}).get("fullTime", {}).get("away"),
        }
        all_parsed_matches.append(match_record)

      print(f"Successfully extracted season {season}")

    except requests.exceptions.HTTPError as e:
      print(f"Failed to fetch season {season}: {e}")
    df = pd.DataFrame(all_parsed_matches)
    return df

  


# Self test
# if __name__ == "__main__":
#   df = extract_multiple_seasons(seasons)
#   print(df.info())


