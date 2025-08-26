# ranking_to_player_base_atp.py
from pathlib import Path
import pandas as pd
import os
import glob
from typing import List, Set, Dict

def load_players_atp(filepath: str) -> pd.DataFrame:
    """
    Lecture robuste du master CSV ATP.
    Si absent: création d'un template (évite crash CI).
    NOTE: player_id est lu comme string (ATP utilise parfois des codes alphanumériques).
    """
    p = Path(filepath)
    if not p.exists():
        p.parent.mkdir(parents=True, exist_ok=True)
        cols = [
            "player_id","full_name","birth_date","birthplace","represented_country",
            "best_rank","plays","height_inches","height_cm",
            "first_appearance","last_appearance"
        ]
        pd.DataFrame(columns=cols).to_csv(str(p), index=False, encoding="utf-8")
        print(f"[INFO] players CSV absent — template créé: {p}")
        return pd.DataFrame(columns=cols)

    # read existing file; don't coerce player_id to int
    df = pd.read_csv(
        str(p),
        keep_default_na=False,
        parse_dates=["birth_date", "first_appearance", "last_appearance"],
    )
    # keep player_id as str (but drop empty strings)
    df['player_id'] = df['player_id'].astype(str).replace('nan', '').replace('<NA>', '')
    return df

def load_rankings_atp(directory: str, pattern: str = 'data*.csv') -> pd.DataFrame:
    """
    Read and combine ranking files in directory. Try to keep player_id as string if non-numeric.
    """
    files = glob.glob(os.path.join(directory, pattern))
    frames = []
    for fn in files:
        df = pd.read_csv(fn, parse_dates=['date'])
        # attempt numeric conversion when possible, but keep original as string fallback
        if 'player_id' in df.columns:
            df['player_id_str'] = df['player_id'].astype(str)
            # try numeric id column also
            try:
                df['player_id'] = pd.to_numeric(df['player_id'], errors='coerce').dropna().astype(int)
            except Exception:
                pass
        frames.append(df)
    if frames:
        return pd.concat(frames, ignore_index=True)
    return pd.DataFrame(columns=['player_id', 'player_id_str', 'full_name', 'ranking', 'date'])

def find_new_ids_atp(existing: pd.DataFrame, rankings: pd.DataFrame) -> Set[str]:
    """
    Identify player_ids present in rankings but not in existing players.
    Keep as strings.
    """
    existing_ids = set(existing['player_id'].astype(str))
    ranking_ids = set(rankings['player_id_str'].astype(str)) if 'player_id_str' in rankings.columns else set(rankings['player_id'].astype(str))
    return ranking_ids - existing_ids

def summarize_new_players_atp(ranks_df: pd.DataFrame, new_ids: Set[str], columns: List[str]) -> pd.DataFrame:
    """
    Create summary rows for each new player_id (strings).
    Uses available ranking info: full_name, ranking, date.
    """
    new_rows = []
    for pid in sorted(new_ids):
        sub = ranks_df[(ranks_df['player_id_str'].astype(str) == str(pid)) | (ranks_df['player_id'].astype(str) == str(pid))]
        if sub.empty:
            continue
        new_rows.append({
            'player_id':           str(pid),
            'full_name':           sub['full_name'].iloc[0],
            'birth_date':          pd.NaT,
            'birthplace':          '',
            'plays':               '',
            'height_inches':       '',
            'height_cm':           '',
            'represented_country': '',
            'best_rank':           int(sub['ranking'].min()) if 'ranking' in sub.columns else None,
            'first_appearance':    sub['date'].min().strftime('%Y-%m-%d'),
            'last_appearance':     sub['date'].max().strftime('%Y-%m-%d'),
        })
    return pd.DataFrame(new_rows, columns=columns)

import numpy as np

def update_last_appearances(players: pd.DataFrame, ranks_df: pd.DataFrame) -> pd.DataFrame:
    """
    For existing players, update their last_appearance if they appear
    in ranks_df with a more recent date, AND update their best_rank if
    the rankings week contains a better (smaller) rank.
    Returns the updated players DataFrame (with helper columns removed).
    """
    # Compute the latest date per player in the rankings
    latest = (
        ranks_df
        .groupby('player_id')['date']
        .max()
        .rename('new_last')
        .reset_index()
    )

    # Compute the best (minimum) ranking observed in ranks_df for each player
    best_from_ranks = (
        ranks_df
        .groupby('player_id')['ranking']
        .min()
        .rename('week_best')
        .reset_index()
    )

    # Merge helpers into players
    merged = players.merge(latest, on='player_id', how='left')
    merged = merged.merge(best_from_ranks, on='player_id', how='left')

    # Ensure both columns are datetimes for safe comparison
    merged['last_appearance'] = pd.to_datetime(merged['last_appearance'], errors='coerce')
    # new_last came from ranks_df.parse_dates so should already be datetime or NaT

    # Update last_appearance where new_last is newer
    mask_last = merged['new_last'].notna() & (merged['new_last'] > merged['last_appearance'])
    merged.loc[mask_last, 'last_appearance'] = merged.loc[mask_last, 'new_last']

    # --- Update best_rank ---
    # Normalize existing best_rank to numeric; if missing -> +inf so comparison "smaller is better" works
    merged['best_rank'] = pd.to_numeric(merged.get('best_rank', pd.Series(dtype='float')), errors='coerce')
    merged['best_rank'] = merged['best_rank'].fillna(np.inf)

    # Normalize week_best from ranks
    merged['week_best'] = pd.to_numeric(merged['week_best'], errors='coerce')

    # If we have a week_best and it's better (smaller) than stored best_rank -> update
    mask_best = merged['week_best'].notna() & (merged['week_best'] < merged['best_rank'])
    if mask_best.any():
        merged.loc[mask_best, 'best_rank'] = merged.loc[mask_best, 'week_best']

    # Optionally: convert best_rank to Int64 (nullable) replacing inf by NA so CSV doesn't contain inf
    # Uncomment if you prefer integer column with <NA> for unknown:
    # merged['best_rank'] = merged['best_rank'].replace(np.inf, pd.NA).astype('Int64')

    # Drop helper columns and return
    merged = merged.drop(columns=['new_last', 'week_best'])
    return merged


def save_players_atp(df: pd.DataFrame, output_path: str) -> None:
    p = Path(output_path)
    parent = p.parent
    if not parent.exists():
        raise RuntimeError(f"save_players_atp: parent dir does not exist: {parent} (refusing to create it)")
    print(f"DEBUG(save_players_atp): writing players CSV to {p.resolve()}")
    df.to_csv(str(p), index=False)
