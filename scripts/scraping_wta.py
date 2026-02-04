# scraping_wta.py
import time
import requests
import pandas as pd
from datetime import datetime
import os
import re

MATCH_ID_PREFIX = "LS"

def _fmt_date_from_timestamp(ts):
    """Convertit '2026-01-11T05:50:10.863+00:00' -> 'YYYY-MM-DD' ou None."""
    if not ts:
        return None
    try:
        t = pd.to_datetime(ts, utc=True, errors="coerce")
        if pd.isna(t):
            return None
        return t.strftime("%Y-%m-%d")
    except Exception:
        return None

def _safe_get(d, *keys, default=None):
    for k in keys:
        if isinstance(d, dict) and k in d:
            return d[k]
    return default

def _format_set(a, b, tb=None):
    if a is None or a == "" or b is None or b == "":
        return None
    a_s = str(a).strip()
    b_s = str(b).strip()
    if tb is None or tb == "" :
        return f"{a_s}-{b_s}"
    tb_s = str(tb).strip()
    if tb_s:
        return f"{a_s}-{b_s}({tb_s})"
    return f"{a_s}-{b_s}"

def _int_from_str(s):
    """Retourne int(s) si possible sinon None (ignore parenthèses)."""
    if s is None:
        return None
    m = re.search(r"(\d+)", str(s))
    return int(m.group(1)) if m else None

def _determine_winner_from_sets(score_data):
    """
    Compte les sets gagnés par A et B d'après ScoreSet{n}A/ScoreSet{n}B.
    Retourne tuple (winner_flag, winner_by_sets, loser_by_sets) where
    winner_flag is 'A' or 'B' or None if undetermined.
    """
    a_wins = 0
    b_wins = 0
    for n in range(1, 6):
        a_raw = score_data.get(f"ScoreSet{n}A", "")
        b_raw = score_data.get(f"ScoreSet{n}B", "")
        if a_raw is None or b_raw is None:
            continue
        a_val = _int_from_str(a_raw)
        b_val = _int_from_str(b_raw)
        if a_val is None or b_val is None:
            continue
        if a_val > b_val:
            a_wins += 1
        elif b_val > a_val:
            b_wins += 1
        # égal improbable dans set final (tie-break gère), ignore égalité
    if a_wins == b_wins == 0:
        return (None, a_wins, b_wins)
    return ('A', a_wins, b_wins) if a_wins > b_wins else ('B', a_wins, b_wins)

# Fonction de capture des données pour un match
def scrape_and_merge_match_data(tournament_id, year, match_id):
    try:
        BASE_URL_SCORE = f"https://api.wtatennis.com/tennis/tournaments/{tournament_id}/{year}/matches/{match_id}/score"
        BASE_URL_STATS = f"https://api.wtatennis.com/tennis/tournaments/{tournament_id}/{year}/matches/{match_id}/stats"

        # Score
        score_response = requests.get(BASE_URL_SCORE, timeout=20)
        if score_response.status_code != 200 or not score_response.text:
            return None
        try:
            score_json = score_response.json()
        except Exception:
            return None
        score_data = score_json[0] if isinstance(score_json, list) and len(score_json) > 0 else (score_json if isinstance(score_json, dict) else None)
        if not score_data:
            return None

        # Stats
        stats_response = requests.get(BASE_URL_STATS, timeout=20)
        stats_data = []
        if stats_response.status_code == 200 and stats_response.text:
            try:
                stats_json = stats_response.json()
                if isinstance(stats_json, list):
                    stats_data = stats_json
            except Exception:
                stats_data = []

        # Tournament block
        tournament_obj = score_data.get("Tournament", {}) if isinstance(score_data, dict) else {}
        tournament_group = tournament_obj.get("tournamentGroup", {}) if isinstance(tournament_obj, dict) else {}
        indoor_outdoor = tournament_obj.get("inOutdoor") if isinstance(tournament_obj, dict) else None

        # Players
        player_a = f"{_safe_get(score_data,'PlayerNameFirstA','') or ''} {_safe_get(score_data,'PlayerNameLastA','') or ''}".strip()
        player_b = f"{_safe_get(score_data,'PlayerNameFirstB','') or ''} {_safe_get(score_data,'PlayerNameLastB','') or ''}".strip()

        # Build set strings
        set1 = _format_set(score_data.get("ScoreSet1A"), score_data.get("ScoreSet1B"), score_data.get("ScoreTbSet1") or score_data.get("ScoreTb1"))
        set2 = _format_set(score_data.get("ScoreSet2A"), score_data.get("ScoreSet2B"), score_data.get("ScoreTbSet2") or score_data.get("ScoreTb2"))
        set3 = _format_set(score_data.get("ScoreSet3A"), score_data.get("ScoreSet3B"), score_data.get("ScoreTbSet3") or score_data.get("ScoreTb3"))

        # Winner determination by sets (primary), fallback to Winner flag (if numeric) or ResultString parse (last resort)
        winner_flag = None
        a_wins = b_wins = 0
        wf, a_wins, b_wins = _determine_winner_from_sets(score_data)
        winner_flag = wf

        # If undetermined from sets, try numeric Winner field heuristics
        if winner_flag is None:
            w_raw = score_data.get("Winner")
            # heuristics: if Winner == "1" -> A, "2" -> B (but feeds may vary); prefer sets if possible
            if w_raw is not None:
                try:
                    if str(w_raw).strip() == "1":
                        winner_flag = 'A'
                    elif str(w_raw).strip() == "2":
                        winner_flag = 'B'
                except Exception:
                    winner_flag = None

        # Fallback: try to parse ResultString like "A. Sabalenka d B. Kostyuk 6-4,6-3"
        if winner_flag is None:
            res = score_data.get("ResultString") or score_data.get("ScoreString")
            if isinstance(res, str) and " d " in res:
                # try to extract left of " d " as winner name
                try:
                    left = res.split(" d ")[0]
                    left_clean = re.sub(r"^\[.*?\]", "", left).strip()  # remove [1] tags
                    # compare with player names: if left starts with last/first, choose accordingly
                    if player_a and left_clean and (player_a.split()[-1] in left_clean or player_a.split()[0] in left_clean):
                        winner_flag = 'A'
                    elif player_b and left_clean and (player_b.split()[-1] in left_clean or player_b.split()[0] in left_clean):
                        winner_flag = 'B'
                except Exception:
                    pass

        # Assign winner/loser names and seeds
        if winner_flag == 'A':
            winner_name = player_a
            loser_name = player_b
            winner_seed = score_data.get("SeedA") or None
            loser_seed = score_data.get("SeedB") or None
        elif winner_flag == 'B':
            winner_name = player_b
            loser_name = player_a
            winner_seed = score_data.get("SeedB") or None
            loser_seed = score_data.get("SeedA") or None
        else:
            # unknown -> set to None so later pipeline can decide
            winner_name = None
            loser_name = None
            winner_seed = score_data.get("SeedA") or score_data.get("SeedB") or None
            loser_seed = None

        # Build main record
        match_info = {
            "event_id": _safe_get(score_data, "EventID"),
            "event_year": _safe_get(score_data, "EventYear"),
            "match_id": _safe_get(score_data, "MatchID"),
            "player_a": player_a,
            "player_b": player_b,
            "country_a": _safe_get(score_data, "PlayerCountryA"),
            "country_b": _safe_get(score_data, "PlayerCountryB"),
            "seed_a": _safe_get(score_data, "SeedA"),
            "seed_b": _safe_get(score_data, "SeedB"),
            "winner_flag_raw": _safe_get(score_data, "Winner"),
            "winner_player_name": winner_name,
            "loser_player_name": loser_name,
            "winner_seed": winner_seed,
            "loser_seed": loser_seed,
            "round": _safe_get(score_data, "RoundID"),
            "num_sets": _safe_get(score_data, "NumSets"),
            "score_string": _safe_get(score_data, "ScoreString") or _safe_get(score_data, "ResultString"),
            "set1_score": set1,
            "set2_score": set2,
            "set3_score": set3,
            "match_timestamp": _safe_get(score_data, "MatchTimeStamp"),
            "match_date": _fmt_date_from_timestamp(_safe_get(score_data, "MatchTimeStamp")),
            "match_time_total": _safe_get(score_data, "MatchTimeTotal"),
            "surface": _safe_get(tournament_obj, "surface"),
            "tournament_name": _safe_get(tournament_group, "name") or _safe_get(tournament_obj, "title"),
            "tournament_title": _safe_get(tournament_obj, "title"),
            "level": _safe_get(tournament_group, "level") or _safe_get(tournament_obj, "level"),
            "start_date": _safe_get(tournament_obj, "startDate"),
            "end_date": _safe_get(tournament_obj, "endDate"),
            "singles_draw_size": _safe_get(tournament_obj, "singlesDrawSize"),
            "doubles_draw_size": _safe_get(tournament_obj, "doublesDrawSize"),
            "prize_money": _safe_get(tournament_obj, "prizeMoney"),
            "prize_money_currency": _safe_get(tournament_obj, "prizeMoneyCurrency"),
            "city": _safe_get(tournament_obj, "city"),
            "country": _safe_get(tournament_obj, "country"),
            "liveScoringId": _safe_get(tournament_obj, "liveScoringId"),
            "venue_id": _safe_get(score_data, "Venue", "id"),
            "venue_name": _safe_get(score_data, "Venue", "name"),
            "indoor_outdoor": indoor_outdoor
        }

        # Stats processing: produce columns like acesa_set1, acesb_set2, etc.
        stats_processed = {}
        if stats_data:
            for stats_entry in stats_data:
                setnum = stats_entry.get("setnum") or stats_entry.get("setNum") or stats_entry.get("set")
                # normalize setnum: if 0 -> total, if >=1 -> set{setnum}
                try:
                    setnum_int = int(setnum)
                except Exception:
                    continue
                for key, value in stats_entry.items():
                    if key in {"setnum", "setNum", "eventid", "eventyear", "matchid", "set"}:
                        continue
                    # map setnum 0 -> tot_{key}, otherwise -> {key}_set{n}
                    if setnum_int == 0:
                        col = f"{key}_tot"
                    else:
                        col = f"{key}_set{setnum_int}"
                    # avoid collisions
                    if col in stats_processed:
                        idx = 1
                        newcol = f"{col}_{idx}"
                        while newcol in stats_processed:
                            idx += 1
                            newcol = f"{col}_{idx}"
                        stats_processed[newcol] = value
                    else:
                        stats_processed[col] = value
        else:
            # keep a few standard stat cols with None so downstream schemas are stable
            stats_processed.update({
                "aces_set1": None, "aces_set2": None, "aces_set3": None,
                "dblflt_set1": None, "dblflt_set2": None, "dblflt_set3": None
            })

        match_info.update(stats_processed)
        df = pd.DataFrame([match_info])
        return df

    except Exception as e:
        print(f"Error scraping match {match_id} in {tournament_id}/{year}: {e}")
        return None

def calculate_match_id_range(num_players):
    return range(1, int(num_players + num_players / 3) + 1)

def process_matches(tournament_player_counts, year):
    output_folder = "data_wta"
    os.makedirs(output_folder, exist_ok=True)

    all_matches = []
    valid_tournament_ids = []

    for tournament_id, num_players in tournament_player_counts.items():
        print(f"Processing tournament {tournament_id} with {num_players} players...")
        match_count = 0
        missing_matches = []

        match_id_range = calculate_match_id_range(num_players)

        for match_id_num in match_id_range:
            match_id = f"{MATCH_ID_PREFIX}{str(match_id_num).zfill(3)}"
            match_df = scrape_and_merge_match_data(tournament_id, year, match_id)

            if match_df is not None:
                all_matches.append(match_df)
                match_count += 1
            else:
                missing_matches.append(match_id)

        retry_count = 0
        while match_count < max(0, num_players - 1) and retry_count < 25:
            print(f"Retrying missing matches for tournament {tournament_id} (attempt {retry_count + 1})...")
            retry_count += 1
            for match_id in missing_matches[:]:
                match_df = scrape_and_merge_match_data(tournament_id, year, match_id)
                if match_df is not None:
                    all_matches.append(match_df)
                    match_count += 1
                    missing_matches.remove(match_id)
            time.sleep(2)

        if match_count >= 1:
            valid_tournament_ids.append(tournament_id)
        print(f"Completed tournament {tournament_id} with {match_count} matches (attempted {len(match_id_range)} ids).")

    if all_matches:
        final_df = pd.concat(all_matches, ignore_index=True, sort=False)
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        outpath = os.path.join(output_folder, f"wtatennis_data_{timestamp}.csv")
        final_df.to_csv(outpath, index=False)
        print(f"Data saved to '{outpath}'.")
        valid_ids_path = os.path.join(output_folder, f"valid_tournament_ids_{timestamp}.txt")
        with open(valid_ids_path, "w") as file:
            for tournament_id in valid_tournament_ids:
                file.write(f"{tournament_id}\n")
        print(f"Valid tournament IDs saved to '{valid_ids_path}'.")
        return final_df
    else:
        print("No data to save. Check tournament IDs and parameters.")
        return pd.DataFrame()
