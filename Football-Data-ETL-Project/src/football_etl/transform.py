import pandas as pd
from .ingestion import extract_multiple_seasons as ems


def transform_match_data(df):
  # Convert date column from string to proper datetime objects
  df["date"] = pd.to_datetime(df["date"])

  # Feature Engineering: Extract calendar attributes
  df["match_year"] = df["date"].dt.year
  df["match_month"] = df["date"].dt.month
  df["day_of_week"] = df["date"].dt.day_name()

  # Feature Engineering: Match Metrics
  df["total_goals"] = df["home_score"] + df["away_score"]
  df["goal_difference"] = df["home_score"] - df["away_score"]

  # Feature Engineering: Match Outcome (Home Win, Away Win, Draw)
  def determine_outcome(row):
    if row["home_score"] > row["away_score"]:
      return "HOME_WIN"
    elif row["home_score"] < row["away_score"]:
      return "AWAY_WIN"
    else:
      return "DRAW"

  df["match_outcome"] = df.apply(determine_outcome, axis=1)

  # Convert text columns to category type
  df["status"] = df["status"].astype("category")

  # Order Sorting
  df = df.sort_values(by=["matchday", "date"]).reset_index(drop=True)

  return df


# Self Test
if __name__ == "__main__":
  # Pass your target seasons list here if ems() requires it
  df_matches = ems([2023, 2024, 2025])
  df_transformed = transform_match_data(df_matches)
  print(
      df_transformed[
          ["matchday", "home_team", "away_team", "total_goals", "match_outcome"]
      ].head()
  )