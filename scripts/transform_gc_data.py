import pandas as pd

# Function to determine the winner and loser from home/away set strings
def determine_winner_loser_home_away(row):
    def parse_set(s):
        """Return (home, away) ints if parsable, else (None, None)."""
        try:
            if s is None:
                return (None, None)
            s = str(s).strip()
            if not s:
                return (None, None)
            # Accept forms like "6-3" or "7-6(5)"
            m = s.split("(")[0]
            parts = m.split("-")
            if len(parts) >= 2:
                h = int(parts[0])
                a = int(parts[1])
                return (h, a)
        except Exception:
            pass
        return (None, None)

    # Collect the three set strings in the expected keys (robust fallback)
    set_keys = [f"Set {i} Score" for i in range(1, 4)]
    set_scores = [row.get(k, None) for k in set_keys]

    home_wins = 0
    away_wins = 0
    valid_sets = 0
    for s in set_scores:
        h, a = parse_set(s)
        if h is None or a is None:
            continue
        valid_sets += 1
        if h > a:
            home_wins += 1
        elif a > h:
            away_wins += 1

    if home_wins > away_wins:
        return "Home", "Away"
    elif away_wins > home_wins:
        return "Away", "Home"
    else:
        # no clear winner from sets
        return None, None


# Function to reorganize row data and preserve IDs
def reorganize_row_home_away(row, winner, loser):
    new_row = {}

    # General match information (normalize to snake_case keys)
    general_cols = [
        "Tournament Name", "Tournament Title", "Level", "Year", "Start Date",
        "End Date", "Surface", "Indoor/Outdoor", "City", "Country",
        "Singles Draw Size", "Doubles Draw Size", "Prize Money",
        "Prize Money Currency", "Match ID", "Date", "Round Name", "Stage Type",
        "Stage Phase", "Stage Start Date", "Stage End Date", "Group Name",
        "Best Of", "Start Time Confirmed"
    ]
    for col in general_cols:
        new_key = col.lower().replace(" ", "_")
        new_row[new_key] = row.get(col, None)

    # Helper to fetch id/country/seed with multiple possible key names (robust)
    def get_any_id(r, keys):
        for k in keys:
            if k in r and r.get(k) is not None and str(r.get(k)).strip() != "":
                return r.get(k)
        return None

    # copy raw Player ID fields (if present in original row) - preserve original keys and lowercase variants
    raw_pid_a = get_any_id(row, ["PlayerIDA", "PlayerIdA", "playerida", "player_id_a"])
    raw_pid_b = get_any_id(row, ["PlayerIDB", "PlayerIdB", "playeridb", "player_id_b"])

    # Always keep the raw ids too (helps downstream)
    new_row["playerida_raw"] = raw_pid_a
    new_row["playeridb_raw"] = raw_pid_b
    # also add original-style names so existing code doesn't break
    new_row["PlayerIDA"] = raw_pid_a
    new_row["PlayerIDB"] = raw_pid_b

    # Mapping columns for winner/loser (base field suffixes expected in original rows)
    player_cols = {
        "player_name": "Player",
        "country": "Country",
        "seed": "Seed",
        "bracket_number": "Bracket Number",
    }

    for col_new, col_base in player_cols.items():
        # row keys look like "Home Player" / "Away Player"
        new_row[f"winner_{col_new}"] = row.get(f"{winner} {col_base}", None) if winner else None
        new_row[f"loser_{col_new}"] = row.get(f"{loser} {col_base}", None) if loser else None

    # Also copy raw home/away fields in case downstream expects them
    for base in ("Player", "Country", "Seed"):
        new_row[f"home_{base.lower()}"] = row.get(f"Home {base}", None)
        new_row[f"away_{base.lower()}"] = row.get(f"Away {base}", None)

    # Set winner/loser player ids derived from raw ids and winner/loser labels
    if winner == "Home":
        new_row["winner_player_id"] = raw_pid_a
        new_row["loser_player_id"] = raw_pid_b
    elif winner == "Away":
        new_row["winner_player_id"] = raw_pid_b
        new_row["loser_player_id"] = raw_pid_a
    else:
        new_row["winner_player_id"] = None
        new_row["loser_player_id"] = None

    # Add statistics for each set (and other stats) - keep same names as original when available
    stat_cols = [
        "aces", "backhand_errors", "backhand_unforced_errors", "backhand_winners",
        "breakpoints_won", "double_faults", "drop_shot_unforced_errors",
        "drop_shot_winners", "first_serve_points_won", "first_serve_successful",
        "forehand_errors", "forehand_unforced_errors", "forehand_winners",
        "games_won", "groundstroke_errors", "groundstroke_unforced_errors",
        "groundstroke_winners", "lob_unforced_errors", "lob_winners",
        "max_games_in_a_row", "max_points_in_a_row", "overhead_stroke_errors",
        "overhead_stroke_unforced_errors", "overhead_stroke_winners",
        "points_won", "points_won_from_last_10", "return_errors",
        "return_winners", "second_serve_points_won", "second_serve_successful",
        "service_games_won", "service_points_lost", "service_points_won",
        "tiebreaks_won", "total_breakpoints", "volley_unforced_errors",
        "volley_winners"
    ]

    for stat in stat_cols:
        new_row[f"winner_{stat}"] = row.get(f"{winner} {stat}", None) if winner else None
        new_row[f"loser_{stat}"] = row.get(f"{loser} {stat}", None) if loser else None

    # Handle scores (preserve exact "Set i Score" values if present)
    for i in range(1, 4):
        key = f"Set {i} Score"
        # Keep both snake_case and original keys
        new_row[f"set{i}_score"] = row.get(key, None)
        new_row[key] = row.get(key, None)

    # Preserve other helpful raw fields if present
    for raw_key in ["ScoreString", "ResultString", "Winner", "RoundID", "MatchTimeStamp", "Venue", "CourtID"]:
        if raw_key in row:
            new_row[raw_key] = row.get(raw_key)

    return new_row


# Transform the dataframe
def transform_home_away_data(data):
    """
    Expecting a DataFrame 'data' with rows produced by scraping_gc_matches.fetch_tournament_data().
    This function will produce a new DataFrame that:
      - preserves PlayerIDA/PlayerIDB raw fields,
      - adds winner_player_id / loser_player_id,
      - preserves per-set scores and many stats.
    """
    if data is None or data.empty:
        return pd.DataFrame()

    transformed_rows = []

    for index, row in data.iterrows():
        # treat the row as a dict for get() behavior
        r = row.to_dict()

        winner, loser = determine_winner_loser_home_away(r)

        if winner and loser:
            transformed_row = reorganize_row_home_away(r, winner, loser)
            transformed_rows.append(transformed_row)
        else:
            # If winner/loser not determined from set parsing, still attempt to keep IDs + fallback name mapping
            # We can still output a row so it won't get dropped silently
            fallback = {}
            # copy raw PlayerIDA/PlayerIDB if present
            pid_a = r.get("PlayerIDA") or r.get("playerida") or r.get("PlayerIdA")
            pid_b = r.get("PlayerIDB") or r.get("playeridb") or r.get("PlayerIdB")
            fallback["PlayerIDA"] = pid_a
            fallback["PlayerIDB"] = pid_b
            # try to map names if possible
            home_name = r.get("Home Player") or r.get("PlayerNameA") or None
            away_name = r.get("Away Player") or r.get("PlayerNameB") or None
            fallback["winner_player_id"] = None
            fallback["loser_player_id"] = None
            fallback["winner_player_name"] = None
            fallback["loser_player_name"] = None
            # if a Winner field exists, try to use it
            w_raw = r.get("Winner") or r.get("winner_flag") or None
            if w_raw is not None:
                wr = str(w_raw).strip()
                if wr in ("1", "A", "a"):
                    fallback["winner_player_name"] = home_name
                    fallback["loser_player_name"] = away_name
                    fallback["winner_player_id"] = pid_a
                    fallback["loser_player_id"] = pid_b
                elif wr in ("2", "B", "b"):
                    fallback["winner_player_name"] = away_name
                    fallback["loser_player_name"] = home_name
                    fallback["winner_player_id"] = pid_b
                    fallback["loser_player_id"] = pid_a
                else:
                    # if winner field equals an id string, map accordingly
                    if pid_a and wr == str(pid_a):
                        fallback["winner_player_id"] = pid_a
                        fallback["loser_player_id"] = pid_b
                        fallback["winner_player_name"] = home_name
                        fallback["loser_player_name"] = away_name
                    elif pid_b and wr == str(pid_b):
                        fallback["winner_player_id"] = pid_b
                        fallback["loser_player_id"] = pid_a
                        fallback["winner_player_name"] = away_name
                        fallback["loser_player_name"] = home_name

            # copy some set scores if exist
            for i in range(1,4):
                fallback[f"set{i}_score"] = r.get(f"Set {i} Score", None)
                fallback[f"Set {i} Score"] = r.get(f"Set {i} Score", None)

            # preserve ScoreString/ResultString
            for k in ("ScoreString", "ResultString", "Score", "score_string"):
                if k in r:
                    fallback[k] = r.get(k)

            transformed_rows.append(fallback)

    transformed_df = pd.DataFrame(transformed_rows)

    # Ensure key columns exist so downstream code doesn't KeyError / drop important rows
    for col in ["winner_player_id", "loser_player_id", "PlayerIDA", "PlayerIDB", "playerida_raw", "playeridb_raw"]:
        if col not in transformed_df.columns:
            transformed_df[col] = None

    # Save debug CSV if you want to inspect quickly (keep or remove as you like)
    try:
        transformed_df.to_csv('transformed_test.csv', index=False)
    except Exception:
        pass

    return transformed_df
