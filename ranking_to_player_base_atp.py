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

def update_last_appearances_atp(players: pd.DataFrame, ranks_df: pd.DataFrame) -> pd.DataFrame:
    """
    Update last_appearance using ranks (attempt using player_id_str if needed).
    """
    # prefer player_id numeric if present in ranks_df; else use player_id_str
    if 'player_id' in ranks_df and ranks_df['player_id'].dtype.kind in 'iu':
        latest = ranks_df.groupby('player_id')['date'].max().rename('new_last').reset_index()
        # ensure players.player_id numeric where possible (skip otherwise)
        try:
            players_int = players.copy()
            players_int['player_id_num'] = pd.to_numeric(players_int['player_id'], errors='coerce')
            merged = players_int.merge(latest, left_on='player_id_num', right_on='player_id', how='left')
            merged['last_appearance'] = pd.to_datetime(merged['last_appearance'], errors='coerce')
            mask = merged['new_last'].notna() & (merged['new_last'] > merged['last_appearance'])
            merged.loc[mask, 'last_appearance'] = merged.loc[mask, 'new_last']
            merged = merged.drop(columns=['new_last','player_id','player_id_num'])
            # reattach original player_id column
            merged['player_id'] = players['player_id']
            return merged
        except Exception:
            pass

    # fallback: no numeric ids -> group by player_id_str if exists
    if 'player_id_str' in ranks_df:
        latest = ranks_df.groupby('player_id_str')['date'].max().rename('new_last').reset_index()
        merged = players.merge(latest, left_on='player_id', right_on='player_id_str', how='left')
        merged['last_appearance'] = pd.to_datetime(merged['last_appearance'], errors='coerce')
        mask = merged['new_last'].notna() & (merged['new_last'] > merged['last_appearance'])
        merged.loc[mask, 'last_appearance'] = merged.loc[mask, 'new_last']
        return merged.drop(columns=['new_last','player_id_str'])

    return players

def save_players_atp(df: pd.DataFrame, output_path: str) -> None:
    p = Path(output_path)
    parent = p.parent
    if not parent.exists():
        raise RuntimeError(f"save_players_atp: parent dir does not exist: {parent} (refusing to create it)")
    print(f"DEBUG(save_players_atp): writing players CSV to {p.resolve()}")
    df.to_csv(str(p), index=False)
