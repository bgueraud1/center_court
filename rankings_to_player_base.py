from pathlib import Path
import pandas as pd
import os
import glob
from typing import List, Set, Dict


def load_players(filepath: str) -> pd.DataFrame:
    """
    Lecture robuste du master CSV. Si le fichier est absent, on lève une
    erreur claire ou on crée un template selon ton choix. Ici on préfère
    créer un template vide pour que le pipeline continue proprement.
    """

    
    p = Path(filepath)
    if not p.exists():
        # créer dossier si besoin et un CSV template pour éviter crash
        p.parent.mkdir(parents=True, exist_ok=True)
        cols = [
            "player_id","full_name","birth_date","birthplace","represented_country",
            "best_rank","plays","height_inches","height_cm",
            "first_appearance","last_appearance"
        ]
        pd.DataFrame(columns=cols).to_csv(str(p), index=False, encoding="utf-8")
        print(f"[INFO] players CSV absent — template créé: {p}")
        return pd.DataFrame(columns=cols)


    if not os.path.exists(filepath):
        raise FileNotFoundError(f"players CSV not found at {filepath} (CI workspace). Aborting to avoid creating a new template.")
    # existing implementation continues...
    df = pd.read_csv(
        filepath,
        keep_default_na=False,
        parse_dates=["birth_date", "first_appearance", "last_appearance"],
    )

    # si le fichier existe, le lire en protégeant les conversions
    df = pd.read_csv(
        str(p),
        keep_default_na=False,
        parse_dates=["birth_date", "first_appearance", "last_appearance"],
    )
    # tenter de normaliser player_id en entier (ignore les erreurs)
    try:
        df['player_id'] = (
            pd.to_numeric(df['player_id'], errors='coerce')
              .dropna().astype(int)
        )
    except Exception:
        # si conversion impossible, on laisse tel quel (debug print)
        print("[WARN] Impossible de convertir player_id en int pour certains enregistrements.")
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
    Save the DataFrame to CSV. Do NOT create unexpected parent directories;
    require the parent directory to already exist to avoid nested/misplaced files.
    """
    p = Path(output_path)
    parent = p.parent
    if not parent.exists():
        raise RuntimeError(f"save_players: parent dir does not exist: {parent} (refusing to create it)")
    print(f"DEBUG(save_players): writing players CSV to {p.resolve()}")
    df.to_csv(str(p), index=False)