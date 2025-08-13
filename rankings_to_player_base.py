import pandas as pd
import glob
import os
from typing import List, Set, Dict

def load_players(filepath: str) -> pd.DataFrame:
    """
    Load existing player data, parse birth_date, and coerce player_id to int.
    """
    df = pd.read_csv(
        filepath,
        keep_default_na=False,
        parse_dates=["birth_date", "first_appearance", "last_appearance"],
    )
    df['player_id'] = (
        pd.to_numeric(df['player_id'], errors='coerce')
          .dropna().astype(int)
    )
    return df

def load_rankings(directory: str, pattern: str = 'data*.csv') -> pd.DataFrame:
    """
    Read and combine all weekly ranking files from the given directory.
    """
    files = glob.glob(os.path.join(directory, pattern))
    frames = []
    for fn in files:
        df = pd.read_csv(fn, parse_dates=['date'])
        df['player_id'] = (
            pd.to_numeric(df['player_id'], errors='coerce')
              .dropna().astype(int)
        )
        frames.append(df)
    return pd.concat(frames, ignore_index=True)

def find_new_ids(existing: pd.DataFrame, rankings: pd.DataFrame) -> Set[int]:
    """
    Identify player_ids present in rankings but not in existing players.
    """
    existing_ids = set(existing['player_id'])
    ranking_ids = set(rankings['player_id'])
    return ranking_ids - existing_ids

def summarize_new_players(ranks_df: pd.DataFrame, new_ids: Set[int], columns: List[str]) -> pd.DataFrame:
    """
    Create summary rows for each new player_id.
    """
    new_rows: List[Dict] = []
    for pid in sorted(new_ids):
        sub = ranks_df[ranks_df['player_id'] == pid]
        new_rows.append({
            'player_id':           pid,
            'full_name':           sub['full_name'].iloc[0],
            'birth_date':          pd.NaT,
            'birthplace':          '',
            'plays':               '',
            'height_inches':       '',
            'height_cm':           '',
            'represented_country': '',
            'best_rank':           int(sub['ranking'].min()),
            'first_appearance':    sub['date'].min().strftime('%Y-%m-%d'),
            'last_appearance':     sub['date'].max().strftime('%Y-%m-%d'),
        })
    return pd.DataFrame(new_rows, columns=columns)

def update_last_appearances(players: pd.DataFrame, ranks_df: pd.DataFrame) -> pd.DataFrame:
    """
    For existing players, update their last_appearance if they appear
    in ranks_df with a more recent date.
    """
    # Compute the latest date per player in the rankings
    latest = (
        ranks_df
        .groupby('player_id')['date']
        .max()
        .rename('new_last')
        .reset_index()
    )

    # Merge into players_df
    merged = players.merge(latest, on='player_id', how='left')

    # Ensure both columns are datetimes for safe comparison
    merged['last_appearance'] = pd.to_datetime(merged['last_appearance'], errors='coerce')
    # new_last already came from ranks_df.parse_dates so it's datetime.

    # Where new_last is not NaT and later than last_appearance, replace it
    mask = merged['new_last'].notna() & (merged['new_last'] > merged['last_appearance'])
    merged.loc[mask, 'last_appearance'] = merged.loc[mask, 'new_last']

    # Drop the helper column
    return merged.drop(columns=['new_last'])

def save_players(df: pd.DataFrame, output_path: str) -> None:
    """
    Save the DataFrame to CSV.
    """
    df.to_csv(output_path, index=False)
