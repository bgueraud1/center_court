# main.py (modifié)
import requests
import pandas as pd
import datetime
import os
import time
from pathlib import Path
import shutil

from config import players_path, output_path, rankings_dir, min_first_date, overwrite_wiki, overwrite_ioc, begin_index_ioc, begin_index_wiki, end_index_wiki, end_index_ioc
from scrape_player_ranking_wta import scrape_data
from rankings_to_player_base import load_players, load_rankings, find_new_ids, summarize_new_players, save_players, update_last_appearances
from add_ioc_to_player import enrich_country_codes
from scrape_wiki_wta import enrich_csv, make_retry_session




# main.py — ajouter tout en haut, après imports
from pathlib import Path
import sys
import os

# players_path vient de config import (déjà présent plus bas)
# mais si tu veux tester avant import, tu peux vérifier après l'import config

# Diagnostic helper (affiche beaucoup d'info utiles dans CI logs)
def debug_find_csv(filename="player_data_wta.csv"):
    print(">>> DEBUG: Searching for player_data_wta.csv in workspace (case-insensitive)")
    # liste racine
    os.system("pwd || true")
    os.system("ls -la || true")
    # recherche insensible à la casse et maxdepth raisonnable
    os.system("find . -maxdepth 8 -iname '*player_data_wta.csv' -print -exec ls -l {} \\; || true")
    # vérifier git index
    os.system("git ls-files | grep -i player_data_wta.csv || true")
    # tenter d'afficher un aperçu si le fichier existe
    for p in Path('.').rglob('*player_data_wta.csv'):
        try:
            print(f"--- HEAD of {p} ---")
            os.system(f"head -n 6 {p} || true")
            print("--- file command ---")
            os.system(f"file {p} || true")
            print("--- hexdump first 200 bytes (detect LFS pointer / BOM) ---")
            os.system(f"xxd -l 200 {p} || true")
        except Exception:
            pass

# Appelé plus tard **après** l'import config (qui définit players_path)
# Si players_path n'existe pas -> crash proprement avec diagnostic
if not players_path.exists():
    print("ERROR: players_path NOT FOUND at:", players_path)
    debug_find_csv()
    raise SystemExit("ERROR: players CSV absent in CI workspace — aborting to avoid creating template.")
else:
    print("OK: players_path exists:", players_path, "size:", players_path.stat().st_size)
    # show a quick head to be safe
    os.system(f"head -n 6 {players_path} || true")




print("DEBUG: cwd =", os.getcwd())
print("DEBUG: players_path =", str(players_path.resolve()))
print("DEBUG: output_path =", str(output_path.resolve()))
print("DEBUG: rankings_dir =", str(rankings_dir.resolve()))

# Ensure directories exist
os.makedirs(rankings_dir, exist_ok=True)
DATA_DIR = players_path.parent

# --- 1) Scrape rankings for the requested dates ---


today = datetime.date.today()

# Option: forcer depuis env var SCRAPE_DATE="YYYY-MM-DD"
scrape_date_env = os.getenv("SCRAPE_DATE")
if scrape_date_env:
    try:
        start_date = datetime.datetime.strptime(scrape_date_env, "%Y-%m-%d").date()
    except Exception:
        raise SystemExit("SCRAPE_DATE mal formattée, utiliser YYYY-MM-DD")
else:
    # calculer le lundi de la semaine courante (weekday(): Monday=0)
    # Si today is Monday -> use today, else go back to last Monday
    days_since_monday = today.weekday()  # 0..6
    start_date = today - datetime.timedelta(days=days_since_monday)

# si tu veux scraper seulement ce lundi (une date) :
end_date = start_date

specific_dates = [start_date + datetime.timedelta(weeks=i) for i in range((end_date - start_date).days // 7 + 1)]
print(f"Will scrape rankings for: {specific_dates}")

scrape_data(specific_dates, rankings_dir)

# --- 2) Update player base from rankings (unchanged) ---
players_df = load_players(players_path)
ranks_df = load_rankings(rankings_dir)

new_ids = find_new_ids(players_df, ranks_df)
if new_ids:
    new_players_df = summarize_new_players(ranks_df, new_ids, list(players_df.columns))
    players_df = pd.concat([players_df, new_players_df], ignore_index=True)
    print(f"Added {len(new_ids)} new players.")
else:
    print("No new players to add.")

players_df = update_last_appearances(players_df, ranks_df)
save_players(players_df, str(players_path))
print(f"Player data refreshed and written to → {output_path}")

# --- 3) Decide whether to run full refresh or only on new players ---
today = datetime.date.today()
monthly_window = 1 <= today.day <= 7

# helper paths for temporary per-new-player runs
tmp_in = DATA_DIR / "tmp_new_players_input.csv"
tmp_out = DATA_DIR / "tmp_new_players_output.csv"
summary_tmp = DATA_DIR / "tmp_overwrite_changes.csv"

# Create the requests/session before scraping
session = make_retry_session(
    total_retries=3,
    backoff_factor=0.5,
    status_forcelist=[500,502,503,504]
)

if monthly_window:
    print("=== Monthly full refresh window (day 1..7) — running full wiki + IOC enrich on all active players ===")
    # Run full enrichment on the master CSV (same behavior as before)
    enrich_csv(
        session=session,
        input_csv=str(players_path),
        output_csv=str(output_path),
        summary_csv=str(DATA_DIR / "overwrite_changes.csv"),
        rankings_dir=str(rankings_dir),
        start_index=begin_index_wiki,
        end_index=end_index_wiki,
        overwrite=overwrite_wiki,
        min_first_date=min_first_date
    )

    with requests.Session() as sess:
        enrich_country_codes(
            session=sess,
            input_csv=str(players_path),
            output_csv=str(output_path),
            start_index=begin_index_ioc,
            end_index=end_index_ioc,
            overwrite=overwrite_ioc
        )

    print("Monthly full refresh done.")

else:
    # Normal case: run wiki & IOC only for new players
    if not new_ids:
        print("No new players -> skipping wiki and IOC enrichment for this run.")
    else:
        print(f"=== Running wiki + IOC enrichment ONLY for {len(new_ids)} new players ===")

        # Build tmp input CSV containing only the rows for new_ids
        # Ensure player_id is int for matching
        # ensure numeric player_id
        players_df['player_id'] = pd.to_numeric(players_df['player_id'], errors='coerce').astype('Int64')

        # build subset for new players
        mask = players_df['player_id'].isin(new_ids)
        df_new_subset = players_df.loc[mask].copy()

        # --- IMPORTANT: keep only players that are "active" in the latest rankings ---
        # ranks_df est déjà chargé plus haut (load_rankings). On calcule les player_id apparus
        # à la date de classement la plus récente et on filtre df_new_subset sur cette liste.
        try:
            latest_date = ranks_df['date'].max()
            active_ids = set(ranks_df.loc[ranks_df['date'] == latest_date, 'player_id'].astype(int))
            print(f"DEBUG: latest ranking date = {latest_date}, active ids = {len(active_ids)}")
            # filter the new-subset to only active players (avoid scraping retirees / historical players)
            before = len(df_new_subset)
            df_new_subset = df_new_subset[df_new_subset['player_id'].isin(active_ids)].copy()
            after = len(df_new_subset)
            print(f"DEBUG: filtered new-subset by active ids: {before} -> {after} rows")
        except Exception as e:
            # if something goes wrong reading ranks_df, continue but warn
            print("WARNING: could not filter new-subset by active ids:", e)


        # If there are no rows (safety)
        if df_new_subset.shape[0] == 0:
            print("No matching rows found for new_ids -> skipping enrich.")
        else:
            # write tmp input and call enrich_csv on it
            df_new_subset.to_csv(tmp_in, index=False)
            print(f"Temporary input for enrich_csv written -> {tmp_in} ({len(df_new_subset)} rows)")

            # Run enrichment on the tmp file. We set start_index/end_index to defaults so enrich_csv
            # will compute active_idxs relative to the temp file (that's OK — it will only attempt active ones)
            enrich_csv(
                session=session,
                input_csv=str(tmp_in),
                output_csv=str(tmp_out),
                summary_csv=str(summary_tmp),
                rankings_dir=str(rankings_dir),
                start_index=0,
                end_index=None,
                overwrite=overwrite_wiki,
                min_first_date=min_first_date
            )

            # Read enriched tmp_out and merge back into master
            if tmp_out.exists():
                enriched = pd.read_csv(tmp_out, keep_default_na=False, parse_dates=['birth_date','first_appearance','last_appearance'], infer_datetime_format=True)
                # ensure numeric player_id for merging
                enriched['player_id'] = pd.to_numeric(enriched['player_id'], errors='coerce').astype(int)

                # Columns that enrich_csv is expected to populate (safe list)
                target_cols = ['height_inches','height_cm','plays','birth_date','birthplace']
                # for each enriched row, update master players_df accordingly (don't overwrite unexpected columns)
                updated = 0
                for _, r in enriched.iterrows():
                    pid = int(r['player_id'])
                    idxs = players_df.index[players_df['player_id'] == pid].tolist()
                    if not idxs:
                        continue
                    idx = idxs[0]
                    for c in target_cols:
                        if c in enriched.columns:
                            new_val = r[c]
                            # Only set if non-empty OR master is empty (same policy as enrich_csv)
                            # We'll mimic "never replace non-blank with blank"
                            old_val = players_df.at[idx, c] if c in players_df.columns else ""
                            if (str(new_val).strip() != "" ) or (str(old_val).strip() == ""):
                                players_df.at[idx, c] = new_val
                                updated += 1

                # Save merged master
                save_players(players_df, str(players_path))
                print(f"Merged enriched data for {len(enriched)} rows into master; approx {updated} fields updated. Master saved -> {output_path}")

                # Clean tmp files (optional)
                try:
                    tmp_in.unlink(missing_ok=True)
                    tmp_out.unlink(missing_ok=True)
                    summary_tmp.unlink(missing_ok=True)
                except Exception:
                    pass
            else:
                print("Warning: expected tmp output not produced by enrich_csv:", tmp_out)

        # Now run IOC enrichment (add_ioc_to_player). We'll reuse the same mechanism:
        # create tmp_in again from updated master rows for the same new_ids and call enrich_country_codes on it
        # then merge back the 'represented_country' (or other IOC-related columns) into master.

        # Recreate tmp_in from the current master (so we include updates from enrich_csv)
        df_new_subset = players_df[players_df['player_id'].isin(new_ids)].copy()
        if df_new_subset.shape[0] == 0:
            print("No new players found for IOC enrichment -> skipping.")
        else:
            df_new_subset.to_csv(tmp_in, index=False)
            tmp_ioc_out = DATA_DIR / "tmp_new_players_ioc_output.csv"
            with requests.Session() as sess:
                enrich_country_codes(
                    session=sess,
                    input_csv=str(tmp_in),
                    output_csv=str(tmp_ioc_out),
                    start_index=0,
                    end_index=None,
                    overwrite=overwrite_ioc
                )

            if tmp_ioc_out.exists():
                ioc_enriched = pd.read_csv(tmp_ioc_out, keep_default_na=False)
                ioc_enriched['player_id'] = pd.to_numeric(ioc_enriched['player_id'], errors='coerce').astype(int)
                # Columns to merge (typical: 'represented_country' but depends on your function)
                ioc_cols = [c for c in ['represented_country'] if c in ioc_enriched.columns]
                ioc_updated = 0
                for _, r in ioc_enriched.iterrows():
                    pid = int(r['player_id'])
                    idxs = players_df.index[players_df['player_id'] == pid].tolist()
                    if not idxs:
                        continue
                    idx = idxs[0]
                    for c in ioc_cols:
                        new_val = r[c]
                        old_val = players_df.at[idx, c] if c in players_df.columns else ""
                        if (str(new_val).strip() != "") or (str(old_val).strip() == ""):
                            players_df.at[idx, c] = new_val
                            ioc_updated += 1

                save_players(players_df, str(players_path))
                print(f"Merged IOC enrichment for {len(ioc_enriched)} rows into master; approx {ioc_updated} fields updated. Master saved -> {output_path}")

                # clean tmp files
                try:
                    tmp_in.unlink(missing_ok=True)
                    tmp_ioc_out.unlink(missing_ok=True)
                except Exception:
                    pass
            else:
                print("Warning: expected tmp IOC output not produced:", tmp_ioc_out)

print("All done.")
