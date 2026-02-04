#!/usr/bin/env python3
# main.py - pipeline qui peut lancer pour plusieurs années
# Écrit uniquement les CSV finaux par tournoi dans matches/wta_matches/
from datetime import datetime
import os
import pandas as pd
import numpy as np
import re
import sys
import argparse
import shutil

# Import de tes modules existants (doivent exister et fonctionner)
from scraping_gc_matches import fetch_tournament_data
from transform_gc_data import transform_home_away_data
from scraping_wta import process_matches

# --- Configuration par défaut ---
YEAR = 2026
# Exemple de dictionnaire : ajuste/ajoute d'autres années si nécessaire
tournament_player_counts_2026 = {
    800: [48, "2024-12-29", "2025-01-05", 0],
    901: [128, "2024-12-29", "2025-01-05", 1],
}

# Répertoire final unique pour tous les CSV finaux
OUT_DIR = os.path.join("matches", "wta_matches")
# Emplacement potentiellement utilisé par process_matches(); on nettoie après appel
POTENTIAL_TEMP_DIR = "data_wta"

# ---------------- utilitaires ----------------

def ensure_out_dir():
    """Crée OUT_DIR si nécessaire."""
    try:
        os.makedirs(OUT_DIR, exist_ok=True)
    except Exception as e:
        print(f"Erreur création dossier de sortie {OUT_DIR}: {e}")
        raise

def cleanup_potential_temp_dir(verbose=True):
    """
    Supprime tout contenu laissé par process_matches ou autres dans POTENTIAL_TEMP_DIR.
    On supprime fichiers et sous-dossiers. Si le dossier n'existe pas, rien à faire.
    """
    try:
        if not os.path.isdir(POTENTIAL_TEMP_DIR):
            return
        for entry in os.listdir(POTENTIAL_TEMP_DIR):
            path = os.path.join(POTENTIAL_TEMP_DIR, entry)
            try:
                if os.path.isfile(path) or os.path.islink(path):
                    os.remove(path)
                    if verbose:
                        print(f"[CLEANUP] Removed file {path}")
                elif os.path.isdir(path):
                    shutil.rmtree(path)
                    if verbose:
                        print(f"[CLEANUP] Removed dir {path}")
            except Exception as e:
                if verbose:
                    print(f"[CLEANUP] Could not remove {path}: {e}")
    except Exception as e:
        if verbose:
            print(f"[CLEANUP] Error cleaning {POTENTIAL_TEMP_DIR}: {e}")

def get_last_scraped_date(file_path="last_scraped_date.txt"):
    """Lit la date du dernier scrape si disponible; sinon retourne 1900-01-01."""
    try:
        with open(file_path, "r") as f:
            return datetime.strptime(f.read().strip(), "%Y-%m-%d")
    except Exception:
        return datetime(1900,1,1)

def tournaments_to_scrape(tournament_player_counts, last_scraped_date, today_date):
    """Retourne la liste (tid, is_gc) à scraper selon dates."""
    out=[]
    for tid, details in tournament_player_counts.items():
        try:
            start = datetime.strptime(details[1], "%Y-%m-%d")
            end = datetime.strptime(details[2], "%Y-%m-%d")
            is_gc = details[3] == 1
            if start <= today_date and end >= last_scraped_date:
                out.append((tid, is_gc))
        except Exception:
            continue
    return out

def parse_date_to_iso(x):
    """Normalize divers types de date en YYYY-MM-DD ou None."""
    if pd.isna(x) or x is None:
        return None
    if isinstance(x, (datetime, pd.Timestamp)):
        return x.strftime("%Y-%m-%d")
    try:
        t = pd.to_datetime(x, utc=True, errors="coerce")
        if pd.isna(t):
            t = pd.to_datetime(x, errors="coerce")
        if pd.isna(t):
            return None
        return t.strftime("%Y-%m-%d")
    except Exception:
        return None

def safe_int(x):
    try:
        if x is None or (isinstance(x,float) and np.isnan(x)):
            return None
        return int(float(x))
    except Exception:
        return None

# Score parsing helpers (repris de ton code, robustes)
def parse_score_string(s):
    if not s:
        return []
    s = str(s)
    parts = [p.strip() for p in s.split(",") if p.strip()]
    out = []
    for p in parts[:5]:
        m = re.match(r"^\s*(\d+)\s*-\s*(\d+)\s*(?:\(\s*(\d+)\s*\))?\s*$", p)
        if m:
            a, b, tb = m.group(1), m.group(2), m.group(3)
            if tb:
                out.append(f"{a}-{b}({tb})")
            else:
                out.append(f"{a}-{b}")
        else:
            digits = re.findall(r"\d+", p)
            if len(digits) >= 2:
                if len(digits) >= 3:
                    out.append(f"{digits[0]}-{digits[1]}({digits[2]})")
                else:
                    out.append(f"{digits[0]}-{digits[1]}")
            else:
                continue
    return out

def extract_sets_from_row(r):
    if any(k in r for k in ("Set 1 Score", "Set 2 Score", "Set 3 Score")):
        s1 = r.get("Set 1 Score") or None
        s2 = r.get("Set 2 Score") or None
        s3 = r.get("Set 3 Score") or None
        return (s1, s2, s3)
    if any(f"ScoreSet{n}A" in r or f"ScoreSet{n}B" in r for n in range(1,6)):
        sets = []
        for n in range(1,6):
            a = r.get(f"ScoreSet{n}A") or r.get(f"scorea_set{n-1}") or ""
            b = r.get(f"ScoreSet{n}B") or r.get(f"scoreb_set{n-1}") or ""
            tb = r.get(f"ScoreTbSet{n}") or ""
            if (a is None or a == "") and (b is None or b == ""):
                continue
            if a is None or a == "" or b is None or b == "":
                continue
            a = str(a).strip(); b = str(b).strip(); tb = str(tb).strip()
            if tb:
                sets.append(f"{a}-{b}({tb})")
            else:
                sets.append(f"{a}-{b}")
        s1 = sets[0] if len(sets) > 0 else None
        s2 = sets[1] if len(sets) > 1 else None
        s3 = sets[2] if len(sets) > 2 else None
        return (s1, s2, s3)
    for k in ("ScoreString", "Score", "score_string", "ResultString", "scoreString"):
        if k in r and r.get(k):
            parsed = parse_score_string(r.get(k))
            s1 = parsed[0] if len(parsed) > 0 else None
            s2 = parsed[1] if len(parsed) > 1 else None
            s3 = parsed[2] if len(parsed) > 2 else None
            return (s1, s2, s3)
    return (None, None, None)

# ---- CORE mapping functions (largement reprises de ton code) ----
CORE_COLS = [
    "tourney_id","tourney_year","tourney_name","level","start_date","end_date",
    "surface","city","country","singles_draw_size","prize_money","prize_money_currency",
    "match_id","date","round","winner","loser","winner_country","loser_country",
    "winner_seed","loser_seed","set1_score","set2_score","set3_score","indoor_outdoor",
    # --- new canonical player id fields ---
    "player_id_winner","player_id_loser"
]


def winner_from_resultstring(result_string, nameA, nameB):
    if not result_string or not isinstance(result_string, str):
        return None
    s = result_string
    s = re.sub(r"\[.*?\]", "", s)
    s = s.strip()
    delim = re.search(r"\s+d\s+|\s+def\s+|def\.|def\s+", s, flags=re.IGNORECASE)
    if delim:
        left = s[:delim.start()].strip().lower()
    else:
        left = re.split(r"\d{1,2}-\d{1,2}", s)[0].strip().lower()

    def name_matches_left(fullname):
        if not fullname:
            return False
        tokens = [t.lower() for t in str(fullname).split() if t]
        if not tokens:
            return False
        last = tokens[-1]
        first = tokens[0]
        if last and last in left:
            return True
        if first and len(first) > 0:
            initial = first[0].lower()
            if re.search(r"\b" + re.escape(initial) + r"\b", left) or re.search(r"\b" + re.escape(initial) + r"\.", left):
                if last and last in left:
                    return True
        if " ".join(tokens) in left:
            return True
        return False

    try:
        if name_matches_left(nameA):
            return 'A'
        if name_matches_left(nameB):
            return 'B'
    except Exception:
        return None
    return None

def map_core_from_gc_row(r, tournament_id, year_str):
    import re
    def get_any(d, keys):
        for k in keys:
            if k in d:
                v = d.get(k)
                if v is None:
                    continue
                if isinstance(v, str) and v.strip() == "":
                    continue
                return v
        return None
    def norm(v):
        if v is None:
            return None
        s = str(v).strip()
        return s if s != "" else None
    def parse_pair_local(s):
        if not s:
            return (None, None)
        s = str(s)
        m = re.match(r"\s*(\d+)\s*-\s*(\d+)\s*(?:\(\s*(\d+)\s*\))?\s*$", s)
        if m:
            return int(m.group(1)), int(m.group(2))
        digits = re.findall(r"\d+", s)
        if len(digits) >= 2:
            return int(digits[0]), int(digits[1])
        return (None, None)
    def count_wins_local(pairs):
        a = b = 0
        for x, y in pairs:
            if x is None or y is None:
                continue
            if x > y:
                a += 1
            elif y > x:
                b += 1
        return a, b

    pA_name = get_any(r, ["Home Player", "PlayerNameA", "PlayerNameFirstA", "PlayerNameLastA"])
    if not pA_name:
        pA_name = ((str(r.get("PlayerNameFirstA", "")).strip() + " " + str(r.get("PlayerNameLastA", "")).strip()).strip()) or None
    pB_name = get_any(r, ["Away Player", "PlayerNameB", "PlayerNameFirstB", "PlayerNameLastB"])
    if not pB_name:
        pB_name = ((str(r.get("PlayerNameFirstB", "")).strip() + " " + str(r.get("PlayerNameLastB", "")).strip()).strip()) or None

    sets = []
    pairs = []
    tbs = []

    any_score_present = False
    for n in range(1, 6):
        for k in (f"ScoreSet{n}A", f"ScoreSet{n}B"):
            if k in r:
                any_score_present = True
                break
        if any_score_present:
            break

    if any_score_present:
        for n in range(1, 6):
            a = None; b = None; tb = None
            for k in [f"ScoreSet{n}A", f"ScoreSet{n}a", f"score_set{n}a", f"scorea_set{n-1}", f"scorea_set{n}"]:
                if k in r:
                    a = norm(r.get(k))
                    if a is not None:
                        break
            for k in [f"ScoreSet{n}B", f"ScoreSet{n}b", f"score_set{n}b", f"scoreb_set{n-1}", f"scoreb_set{n}"]:
                if k in r:
                    b = norm(r.get(k))
                    if b is not None:
                        break
            for k in [f"ScoreTbSet{n}", f"ScoreTb{n}", f"ScoreTbSet{n}"]:
                if k in r:
                    tb = norm(r.get(k))
                    if tb is not None:
                        break
            if (a is None or a == "") and (b is None or b == ""):
                continue
            if a is None or b is None:
                continue
            s = f"{a}-{b}" if not tb else f"{a}-{b}({tb})"
            sets.append(s)
            pairs.append(parse_pair_local(s))
            tbs.append(tb)
    else:
        s1 = get_any(r, ["Set 1 Score", "Set1Score", "set1_score"])
        s2 = get_any(r, ["Set 2 Score", "Set2Score", "set2_score"])
        s3 = get_any(r, ["Set 3 Score", "Set3Score", "set3_score"])
        for s in (s1, s2, s3):
            if s:
                s_norm = norm(s)
                sets.append(s_norm)
                pairs.append(parse_pair_local(s_norm))
        if not sets:
            ss = get_any(r, ["ScoreString", "Score", "score_string", "ResultString", "scoreString"])
            if ss:
                parts = [p.strip() for p in str(ss).split(",") if p.strip()]
                for p in parts[:5]:
                    sets.append(p)
                    pairs.append(parse_pair_local(p))

    winner_seed = safe_int(r.get("winner_seed") or r.get("Home Seed") or r.get("SeedA") or r.get("SeedA"))
    loser_seed = safe_int(r.get("loser_seed") or r.get("Away Seed") or r.get("SeedB") or r.get("SeedB"))
    winner_country = get_any(r, ["winner_country", "Home Country", "PlayerCountryA", "PlayerCountryA2"]) or None
    loser_country = get_any(r, ["loser_country", "Away Country", "PlayerCountryB", "PlayerCountryB2"]) or None

    a_wins, b_wins = count_wins_local(pairs) if pairs else (0,0)
    winner_name = None
    loser_name = None
    match_winner_label = None
    if a_wins > b_wins:
        winner_name = pA_name; loser_name = pB_name; match_winner_label = 'A'
    elif b_wins > a_wins:
        winner_name = pB_name; loser_name = pA_name; match_winner_label = 'B'
    else:
        w_raw = r.get("Winner")
        if w_raw is not None:
            wr = str(w_raw).strip()
            if wr in ("1", "A", "a"):
                winner_name = pA_name; loser_name = pB_name; match_winner_label = 'A'
            elif wr in ("2", "B", "b"):
                winner_name = pB_name; loser_name = pA_name; match_winner_label = 'B'
            else:
                pidA = str(r.get("PlayerIDA")) if r.get("PlayerIDA") is not None else None
                pidB = str(r.get("PlayerIDB")) if r.get("PlayerIDB") is not None else None
                if pidA and wr == pidA:
                    winner_name = pA_name; loser_name = pB_name; match_winner_label = 'A'
                elif pidB and wr == pidB:
                    winner_name = pB_name; loser_name = pA_name; match_winner_label = 'B'
        if match_winner_label is None:
            res = get_any(r, ["ResultString", "ScoreString", "Score"])
            if res and isinstance(res, str):
                try:
                    parsed = winner_from_resultstring(res, pA_name, pB_name)
                except Exception:
                    parsed = None
                if parsed == 'A':
                    winner_name = pA_name; loser_name = pB_name; match_winner_label = 'A'
                elif parsed == 'B':
                    winner_name = pB_name; loser_name = pA_name; match_winner_label = 'B'

    display_sets = []
    for idx, s in enumerate(sets):
        a_val, b_val = pairs[idx] if idx < len(pairs) else (None, None)
        tb_m = re.search(r"\((\d+)\)", str(s)) if s else None
        tb = tb_m.group(1) if tb_m else None
        if match_winner_label in ('A', 'B') and (a_val is not None and b_val is not None):
            if match_winner_label == 'A':
                ds = f"{a_val}-{b_val}"
            else:
                ds = f"{b_val}-{a_val}"
            if tb:
                ds = f"{ds}({tb})"
            display_sets.append(ds)
            continue
        if s:
            display_sets.append(re.sub(r"\s+", "", s))
        else:
            display_sets.append(None)

    set1 = display_sets[0] if len(display_sets) > 0 else None
    set2 = display_sets[1] if len(display_sets) > 1 else None
    set3 = display_sets[2] if len(display_sets) > 2 else None

    if not winner_name:
        winner_name = r.get("winner_player_name") or None
    if not loser_name:
        loser_name = r.get("loser_player_name") or None

    indoor = get_any(r, ["Indoor/Outdoor", "inOutdoor", "IndoorOutdoor", "indoor_outdoor"])

        # --- after winner/loser resolution in map_core_from_gc_row ---
        # --- robust retrieval of raw A/B player ids and mapping to winner/loser ids ---
    import re

    def find_pid(d, base_names):
        """Cherche une valeur plausible de PlayerID en testant plusieurs clés et fallback numeric heuristique."""
        # 1) exact keys / preferred order
        for key in base_names:
            if key in d and d.get(key) not in (None, ""):
                return str(d.get(key)).strip()
        # 2) lowercase variants (ex: transform_home_away_data a pu changer la casse)
        lower_bases = [bk.lower() for bk in base_names]
        for k in d.keys():
            try:
                if k.lower() in lower_bases and d.get(k) not in (None, ""):
                    return str(d.get(k)).strip()
            except Exception:
                continue
        # 3) fallback : chercher une valeur numérique plausible (4-7 chiffres) dans champs contenant 'player'|'id'
        for k, v in d.items():
            try:
                s = str(v).strip()
            except Exception:
                continue
            if re.fullmatch(r"\d{4,7}", s):
                if 'player' in k.lower() or 'id' in k.lower() or 'playerid' in k.lower():
                    return s
        return None

    pidA = find_pid(r, ["PlayerIDA", "PlayerIdA", "playerida", "PlayerAId", "PlayerIDA1"])
    pidB = find_pid(r, ["PlayerIDB", "PlayerIdB", "playeridb", "PlayerBId", "PlayerIDB1"])

    # canonical player_id_winner / player_id_loser (plusieurs filets de sécurité)
    player_id_winner = None
    player_id_loser = None

    # 1) si on a déjà déterminé match_winner_label via les sets -> utiliser directement
    try:
        if match_winner_label == 'A':
            player_id_winner = pidA
            player_id_loser = pidB
        elif match_winner_label == 'B':
            player_id_winner = pidB
            player_id_loser = pidA
    except Exception:
        player_id_winner = player_id_loser = None

    # 2) si pas résolu, regarder le champ Winner (valeurs communes : "1"/"2"/"A"/"B" ou un id)
    if player_id_winner is None:
        w_raw = r.get("Winner") or r.get("winner") or r.get("winner_flag_raw")
        if w_raw is not None:
            wr = str(w_raw).strip()
            if wr in ("1", "A", "a"):
                player_id_winner = pidA; player_id_loser = pidB
            elif wr in ("2", "B", "b"):
                player_id_winner = pidB; player_id_loser = pidA
            else:
                # si Winner contient directement l'ID du joueur
                if pidA and wr == str(pidA):
                    player_id_winner = pidA; player_id_loser = pidB
                elif pidB and wr == str(pidB):
                    player_id_winner = pidB; player_id_loser = pidA

    # 3) si toujours None, comparer winner_player_name / ResultString (texte) avec pA_name / pB_name
    if player_id_winner is None:
        wtxt = (r.get("winner_player_name") or r.get("Winner") or r.get("ResultString") or "").strip()
        try:
            if wtxt and pA_name and wtxt.lower() in pA_name.lower():
                player_id_winner = pidA; player_id_loser = pidB
            elif wtxt and pB_name and wtxt.lower() in pB_name.lower():
                player_id_winner = pidB; player_id_loser = pidA
        except Exception:
            pass

    # 4) dernier recours : si on a les deux pid mais aucune info sur vainqueur,
    #    on laisse les player_id_winner/loser à None (mieux que donner le mauvais id).
    #    Optionnel : si tu veux forcer, commenter la section ci-dessous pour assigner par défaut A->winner.
    # if player_id_winner is None and pidA and pidB:
    #     player_id_winner = pidA; player_id_loser = pidB

    # normalisation finale : convertir "" en None
    if player_id_winner == "":
        player_id_winner = None
    if player_id_loser == "":
        player_id_loser = None



    return {
        "tourney_id": str(tournament_id) if tournament_id is not None else str(r.get("Tournament ID") or r.get("tourney_id") or ""),
        "tourney_year": year_str,
        "tourney_name": r.get("tournament_name") or r.get("tournamentTitle") or r.get("Tournament Name") or r.get("tourney_name"),
        "level": r.get("level") or r.get("Level"),
        "start_date": parse_date_to_iso(r.get("start_date") or r.get("startDate")),
        "end_date": parse_date_to_iso(r.get("end_date") or r.get("endDate")),
        "surface": r.get("surface"),
        "city": r.get("city"),
        "country": r.get("country"),
        "singles_draw_size": r.get("singles_draw_size") or r.get("singlesDrawSize"),
        "prize_money": r.get("prize_money") or r.get("prizeMoney"),
        "prize_money_currency": r.get("prize_money_currency") or r.get("prizeMoneyCurrency"),
        "match_id": r.get("match_id") or r.get("Match ID") or r.get("MatchID") or r.get("MatchId"),
        "date": parse_date_to_iso(r.get("date") or r.get("start_time") or r.get("MatchTimeStamp")),
        "round": r.get("round_name") or r.get("Round Name") or r.get("round"),
        "winner": winner_name or r.get("winner_player_name") or r.get("Home Player") or r.get("winner") or r.get("HomePlayer"),
        "loser": loser_name or r.get("loser_player_name") or r.get("Away Player") or r.get("loser") or r.get("AwayPlayer"),
        "winner_country": winner_country,
        "loser_country": loser_country,
        "winner_seed": winner_seed,
        "loser_seed": loser_seed,
        "set1_score": set1,
        "set2_score": set2,
        "set3_score": set3,
        "indoor_outdoor": indoor,
        "player_id_winner": player_id_winner,
        "player_id_loser": player_id_loser
    }

def map_core_from_non_gc_row(r):
    import re
    def parse_pair(s):
        if not s:
            return (None, None)
        m = re.match(r"^\s*(\d+)\s*-\s*(\d+)", str(s))
        if m:
            return int(m.group(1)), int(m.group(2))
        digits = re.findall(r"\d+", str(s))
        if len(digits) >= 2:
            return int(digits[0]), int(digits[1])
        return (None, None)
    def count_wins(pairs):
        a = b = 0
        for x, y in pairs:
            if x is None or y is None:
                continue
            if x > y:
                a += 1
            elif y > x:
                b += 1
        return a, b
    def winner_from_resultstring_local(result_string, nameA, nameB):
        if not result_string or not isinstance(result_string, str):
            return None
        s = re.sub(r"\[.*?\]", "", result_string).strip().lower()
        delim = re.search(r"\s+d\s+|\s+def\s+|def\.|def\s+", s, flags=re.IGNORECASE)
        if delim:
            left = s[:delim.start()].strip().lower()
        else:
            left = re.split(r"\d{1,2}-\d{1,2}", s)[0].strip().lower()
        def nm_matches(full):
            if not full:
                return False
            toks = [t.lower() for t in str(full).split() if t]
            if not toks:
                return False
            last = toks[-1]; first = toks[0]
            if last and last in left:
                return True
            if first and len(first) and (first[0].lower() in left) and last and last in left:
                return True
            if " ".join(toks) in left:
                return True
            return False
        try:
            if nm_matches(r.get("player_a") or r.get("PlayerNameA")):
                return 'A'
            if nm_matches(r.get("player_b") or r.get("PlayerNameB")):
                return 'B'
        except Exception:
            return None
        return None

    pairs = []
    tbs = []
    raw_A = []
    raw_B = []

    if any(f"ScoreSet{n}A" in r or f"ScoreSet{n}B" in r for n in range(1, 6)):
        for n in range(1, 6):
            a_raw = r.get(f"ScoreSet{n}A")
            b_raw = r.get(f"ScoreSet{n}B")
            tb_raw = r.get(f"ScoreTbSet{n}") or r.get(f"ScoreTb{n}") or ""
            if (a_raw is None or a_raw == "") and (b_raw is None or b_raw == ""):
                continue
            if a_raw is None or a_raw == "" or b_raw is None or b_raw == "":
                continue
            a_s = str(a_raw).strip(); b_s = str(b_raw).strip()
            raw_A.append(a_s); raw_B.append(b_s)
            try:
                a_i = int(re.search(r"(\d+)", a_s).group(1))
            except Exception:
                a_i = None
            try:
                b_i = int(re.search(r"(\d+)", b_s).group(1))
            except Exception:
                b_i = None
            pairs.append((a_i, b_i))
            tbs.append(str(tb_raw).strip() if tb_raw and re.search(r"\d", str(tb_raw)) else None)
    else:
        s1 = r.get("set1_score") or r.get("Set 1 Score") or None
        s2 = r.get("set2_score") or r.get("Set 2 Score") or None
        s3 = r.get("set3_score") or r.get("Set 3 Score") or None
        any_set_fields = False
        for s in (s1, s2, s3):
            if s:
                any_set_fields = True
                pairs.append(parse_pair(s))
                tbs.append(re.search(r"\((\d+)\)", str(s)).group(1) if re.search(r"\((\d+)\)", str(s)) else None)
                raw_A.append(None); raw_B.append(None)
        if not any_set_fields:
            ss = r.get("ScoreString") or r.get("ResultString") or r.get("score_string")
            if ss:
                parsed = parse_score_string(ss)
                for p in parsed:
                    pairs.append(parse_pair(p))
                    tbs.append(re.search(r"\((\d+)\)", str(p)).group(1) if re.search(r"\((\d+)\)", str(p)) else None)
                    raw_A.append(None); raw_B.append(None)

    a_wins, b_wins = count_wins(pairs) if pairs else (0,0)
    match_winner = None
    if a_wins > b_wins:
        match_winner = 'A'
    elif b_wins > a_wins:
        match_winner = 'B'
    else:
        w_raw = r.get("winner_flag_raw") or r.get("Winner")
        if w_raw is not None:
            wr = str(w_raw).strip()
            if wr in ("1","A","a"):
                match_winner = 'A'
            elif wr in ("2","B","b"):
                match_winner = 'B'
            else:
                pidA = str(r.get("PlayerIDA")) if r.get("PlayerIDA") is not None else None
                pidB = str(r.get("PlayerIDB")) if r.get("PlayerIDB") is not None else None
                if pidA and wr == pidA:
                    match_winner = 'A'
                elif pidB and wr == pidB:
                    match_winner = 'B'
        if match_winner is None:
            parsed = winner_from_resultstring_local(r.get("ResultString") or r.get("ScoreString") or "", r.get("player_a") or r.get("PlayerNameA"), r.get("player_b") or r.get("PlayerNameB"))
            if parsed in ('A','B'):
                match_winner = parsed

    display_sets = []
    for idx, (a_i, b_i) in enumerate(pairs):
        tb = tbs[idx] if idx < len(tbs) else None
        a_raw_val = raw_A[idx] if idx < len(raw_A) else None
        b_raw_val = raw_B[idx] if idx < len(raw_B) else None

        if a_i is not None and b_i is not None and match_winner in ('A','B'):
            if match_winner == 'A':
                ds = f"{a_i}-{b_i}"
            else:
                ds = f"{b_i}-{a_i}"
            if tb:
                ds = f"{ds}({tb})"
            display_sets.append(ds)
            continue

        if a_raw_val is not None and b_raw_val is not None and match_winner in ('A','B'):
            if match_winner == 'A':
                ds = f"{a_raw_val}-{b_raw_val}"
            else:
                ds = f"{b_raw_val}-{a_raw_val}"
            if tb:
                ds = f"{ds}({tb})"
            display_sets.append(ds)
            continue

        if a_i is not None and b_i is not None:
            ds = f"{a_i}-{b_i}"
            if tb:
                ds = f"{ds}({tb})"
            display_sets.append(ds)
            continue

        display_sets.append(None)

    set1 = display_sets[0] if len(display_sets) > 0 else None
    set2 = display_sets[1] if len(display_sets) > 1 else None
    set3 = display_sets[2] if len(display_sets) > 2 else None

    player_a = r.get("player_a") or (str(r.get("PlayerNameFirstA","")).strip() + " " + str(r.get("PlayerNameLastA","")).strip()).strip()
    player_b = r.get("player_b") or (str(r.get("PlayerNameFirstB","")).strip() + " " + str(r.get("PlayerNameLastB","")).strip()).strip()

    winner_name = None; loser_name = None
    if match_winner == 'A':
        winner_name = player_a; loser_name = player_b
    elif match_winner == 'B':
        winner_name = player_b; loser_name = player_a
    else:
        winner_name = r.get("winner_player_name") or None
        loser_name = r.get("loser_player_name") or None

    if winner_name and player_a and winner_name == player_a:
        winner_country = r.get("country_a") or r.get("PlayerCountryA")
        loser_country = r.get("country_b") or r.get("PlayerCountryB")
    elif winner_name and player_b and winner_name == player_b:
        winner_country = r.get("country_b") or r.get("PlayerCountryB")
        loser_country = r.get("country_a") or r.get("PlayerCountryA")
    else:
        winner_country = r.get("winner_country") or r.get("country_a") or r.get("country_b")
        loser_country = r.get("loser_country") or r.get("country_b") or r.get("country_a")

    winner_seed = safe_int(r.get("winner_seed") or r.get("seed_a") or r.get("SeedA"))
    loser_seed = safe_int(r.get("loser_seed") or r.get("seed_b") or r.get("SeedB"))
    date_from_ts = parse_date_to_iso(r.get("match_timestamp") or r.get("MatchTimeStamp") or r.get("match_date"))
    indoor = r.get("indoor_outdoor") or r.get("in_indoor_outdoor") or None

        # try various keys for raw player ids
    pidA = r.get("PlayerIDA") or r.get("playerida") or r.get("player_a_id") or r.get("player_a_id_raw")
    pidB = r.get("PlayerIDB") or r.get("playeridb") or r.get("player_b_id") or r.get("player_b_id_raw")

    player_id_winner = None
    player_id_loser = None
    try:
        if match_winner == 'A':
            player_id_winner = pidA; player_id_loser = pidB
        elif match_winner == 'B':
            player_id_winner = pidB; player_id_loser = pidA
        else:
            # If winner not determined, attempt to see if winner_name matches player_a/b and use pid accordingly
            if winner_name and player_a and winner_name == player_a:
                player_id_winner = pidA; player_id_loser = pidB
            elif winner_name and player_b and winner_name == player_b:
                player_id_winner = pidB; player_id_loser = pidA
    except Exception:
        player_id_winner = player_id_loser = None


    return {
        "tourney_id": str(r.get("event_id") or r.get("tourney_id") or ""),
        "tourney_year": str(r.get("event_year") or r.get("tourney_year") or ""),
        "tourney_name": r.get("tournament_name") or r.get("tournament_title") or r.get("tourney_name"),
        "level": r.get("level"),
        "start_date": parse_date_to_iso(r.get("start_date")),
        "end_date": parse_date_to_iso(r.get("end_date")),
        "surface": r.get("surface"),
        "city": r.get("city"),
        "country": r.get("country"),
        "singles_draw_size": r.get("singles_draw_size"),
        "prize_money": r.get("prize_money"),
        "prize_money_currency": r.get("prize_money_currency"),
        "match_id": r.get("match_id") or r.get("MatchID"),
        "date": date_from_ts,
        "round": r.get("round"),
        "winner": winner_name,
        "loser": loser_name,
        "winner_country": winner_country,
        "loser_country": loser_country,
        "winner_seed": winner_seed,
        "loser_seed": loser_seed,
        "set1_score": set1,
        "set2_score": set2,
        "set3_score": set3,
        "indoor_outdoor": indoor,
        "player_id_winner": player_id_winner,
        "player_id_loser": player_id_loser
    }

def normalize_preserve_all(df, is_gc=False, tournament_id=None, year_str=None):
    """Normalise chaque ligne en conservant toutes les colonnes originales + colonnes CORE."""
    if df is None:
        return pd.DataFrame()
    if df.empty:
        return pd.DataFrame(columns=CORE_COLS)
    rows = []
    for _, row in df.iterrows():
        orig = row.to_dict()
        try:
            core = map_core_from_gc_row(orig, tournament_id, year_str) if is_gc else map_core_from_non_gc_row(orig)
        except Exception:
            core = {}
        merged = dict(orig)
        merged.update(core)
        rows.append(merged)
    result = pd.DataFrame(rows)
    for c in CORE_COLS:
        if c not in result.columns:
            result[c] = None
    others = [c for c in result.columns if c not in CORE_COLS]
    result = result[CORE_COLS + others]
    return result

def explicit_merge_gc_non_gc(gc_df, non_gc_df, key="match_id"):
    """
    Pour revue interne seulement : retourne le DataFrame merged, mais N'ECRIT PAS de fichier.
    (Ne sauvegarde rien sur le disque.)
    """
    if gc_df is None:
        gc_df = pd.DataFrame()
    if non_gc_df is None:
        non_gc_df = pd.DataFrame()
    if key not in gc_df.columns:
        gc_df[key] = None
    if key not in non_gc_df.columns:
        non_gc_df[key] = None

    merged = pd.merge(gc_df, non_gc_df, how="outer", on=key, suffixes=("_gc", "_non_gc"))

    if "indoor_outdoor_gc" in merged.columns or "indoor_outdoor_non_gc" in merged.columns:
        merged["indoor_outdoor_merged"] = merged.get("indoor_outdoor_gc").fillna(merged.get("indoor_outdoor_non_gc"))

    cols_to_remove = ["doubles_draw_size","round_name","stage_type","stage_phase","stage_start_date","stage_end_date","group_name","best_of","start_time_confirmed"]
    for c in cols_to_remove:
        for suf in ("", "_gc", "_non_gc"):
            if (c + suf) in merged.columns:
                merged.drop(columns=[c + suf], inplace=True)

    # Ne rien sauvegarder sur disque ici (contrainte utilisateur).
    return merged

# --------- Sélection robuste par tid ----------
def select_rows_for_tid(df, tid):
    """
    Recherche robuste des lignes appartenant au tournoi `tid` :
      - teste colonnes probables (tourney_id, event_id, tournament_id, tournament_name, tournament_title)
      - fallback égalité stricte stringifiée
      - fallback recherche mot-entier (\b<ID>\b) dans toutes les cellules
    """
    if df is None or df.empty:
        return pd.DataFrame(columns=df.columns if df is not None else CORE_COLS)
    tid_str = str(tid)
    candidates = pd.DataFrame()

    # colonnes prioritaires
    priority_names = set(["tourney_id","event_id","event","tournament_id","tournament","tournament_name","tournament_title","eventid","event_id"])
    priority_cols = [c for c in df.columns if c.lower() in priority_names]
    for col in priority_cols:
        try:
            sel = df[df[col].fillna("").astype(str).str.strip() == tid_str]
            if not sel.empty:
                candidates = pd.concat([candidates, sel], ignore_index=True, sort=False)
        except Exception:
            continue

    # fallback égalité stricte sur toutes les colonnes
    if candidates.empty:
        try:
            mask = df.apply(lambda row: any(str(v).strip() == tid_str for v in row.values if pd.notna(v)), axis=1)
            candidates = df[mask]
        except Exception:
            candidates = pd.DataFrame()

    # fallback regex mot-entier
    if candidates.empty:
        try:
            regex = re.compile(rf"\b{re.escape(tid_str)}\b")
            mask2 = df.apply(lambda row: any(bool(regex.search(str(v))) for v in row.values if pd.notna(v)), axis=1)
            candidates = df[mask2]
        except Exception:
            candidates = pd.DataFrame()

    if not candidates.empty:
        candidates = candidates.drop_duplicates().reset_index(drop=True)
    return candidates

# --------- MAIN (par année) ----------
def main(year=None, tournament_player_counts=None, verbose=True):
    """
    Exécute la pipeline pour une année.
    -> Ecrit uniquement : matches/wta_matches/wta_<tid>_<year>.csv
    -> Ne laisse aucun autre fichier sur disque (nettoie POTENTIAL_TEMP_DIR si nécessaire).
    Retourne dict {tid: dataframe} pour usage interne/tests.
    """
    year_str = str(year or YEAR)
    tpc = tournament_player_counts or tournament_player_counts_2026

    ensure_out_dir()

    today = datetime.now()
    last_scraped_file = f"last_scraped_date_{year_str}.txt"
    last_scraped = get_last_scraped_date(file_path=last_scraped_file)
    to_scrape = tournaments_to_scrape(tpc, last_scraped, today)
    if verbose:
        print(f"\n=== YEAR {year_str} | Tournaments à scraper: {to_scrape}")

    # --- 1) GC fetch (récupère DataFrames en mémoire, n'écrit rien) ---
    gc_collected = []
    for tid, is_gc in to_scrape:
        if not is_gc:
            continue
        if verbose:
            print(f"[GC] fetch_tournament_data(year={year_str}, tid={tid}) ...")
        try:
            # fetch_tournament_data doit retourner un DataFrame (ou None)
            df_gc = fetch_tournament_data(year_str, tid, verbose=True)
        except TypeError:
            df_gc = fetch_tournament_data(year_str, tid)
        except Exception as e:
            print(f"[GC] fetch exception for {tid}: {e}")
            df_gc = pd.DataFrame()

        if df_gc is None:
            df_gc = pd.DataFrame()

        # tenter transformation si disponible
        try:
            df_gc = transform_home_away_data(df_gc)
        except Exception:
            # si transform échoue, on continue avec df_gc tel quel
            pass

        gc_collected.append((tid, df_gc))

    # --- 2) non-GC fetch via process_matches (peut créer des fichiers temporaires) ---
    non_gc_map = {}
    for tid, is_gc in to_scrape:
        if not is_gc:
            details = tpc.get(tid)
            draw = details[0] if details else None
            non_gc_map[tid] = draw

    non_gc_raw = pd.DataFrame()
    if non_gc_map:
        if verbose:
            print(f"[non-GC] calling process_matches for {len(non_gc_map)} tournaments ...")
        try:
            non_gc_raw = process_matches(non_gc_map, year_str)
            if non_gc_raw is None:
                non_gc_raw = pd.DataFrame()
        except Exception as e:
            print(f"[non-GC] process_matches exception: {e}")
            non_gc_raw = pd.DataFrame()
        finally:
            # cleanup immédiat de tout fichier que process_matches aurait laissé
            cleanup_potential_temp_dir(verbose=verbose)

    # DEBUG summary
    total_gc_rows = 0
    for tid, df in gc_collected:
        n = 0 if df is None else len(df)
        if verbose:
            print(f"[DEBUG] GC tid={tid} | rows={n} | sample cols={(list(df.columns)[:10] if (df is not None and len(df.columns)>0) else [])}")
        total_gc_rows += n
    if verbose:
        print(f"[DEBUG] total GC rows (year {year_str}): {total_gc_rows}")
        print(f"[DEBUG] non-GC raw shape: {non_gc_raw.shape if non_gc_raw is not None else (0,0)}")
        if non_gc_raw is not None:
            print(f"[DEBUG] non-GC sample cols: {list(non_gc_raw.columns)[:40]}")

    # --- Normalize en mémoire (sans écrire) ---
    normalized_gc_list = []
    for tid, df in gc_collected:
        norm = normalize_preserve_all(df, is_gc=True, tournament_id=tid, year_str=year_str)
        normalized_gc_list.append(norm)
        if verbose:
            print(f"[DEBUG] normalized GC tid={tid} -> {norm.shape}")

    gc_all = pd.concat(normalized_gc_list, ignore_index=True, sort=False) if normalized_gc_list else pd.DataFrame(columns=CORE_COLS)
    normalized_non_gc = normalize_preserve_all(non_gc_raw, is_gc=False)

    if verbose:
        print(f"[DEBUG] gc_all.shape = {gc_all.shape}")
        print(f"[DEBUG] normalized_non_gc.shape = {normalized_non_gc.shape}")

    # Ensure indoor_outdoor exists
    for df in (gc_all, normalized_non_gc):
        if df is not None and 'indoor_outdoor' not in df.columns:
            df['indoor_outdoor'] = None

    # Combined internal (ne sera pas sauvegardé)
    combined = pd.concat([gc_all, normalized_non_gc], axis=0, ignore_index=True, sort=False) if (not gc_all.empty or not normalized_non_gc.empty) else pd.DataFrame(columns=CORE_COLS)

    # Force indoor_outdoor = 'O' pour GC rows (détection via level ou tourney_id)
    expected_gc_ids = [str(tid) for tid, is_gc in to_scrape if is_gc]
    if 'level' in combined.columns:
        lvl_series = combined['level'].fillna('').astype(str).str.upper()
    else:
        lvl_series = pd.Series([''] * len(combined))
    cond_level = lvl_series.isin(['GC', 'GRAND SLAM'])
    if 'tourney_id' in combined.columns:
        tid_series = combined['tourney_id'].fillna('').astype(str)
    else:
        tid_series = pd.Series([''] * len(combined))
    cond_tid = tid_series.isin(expected_gc_ids)
    cond_gc = cond_level | cond_tid
    if 'indoor_outdoor' not in combined.columns:
        combined['indoor_outdoor'] = None
    try:
        combined.loc[cond_gc, 'indoor_outdoor'] = 'O'
    except Exception:
        for i in range(len(combined)):
            try:
                if (str(combined.at[i, 'level']).upper() in ('GC', 'GRAND SLAM')) or (str(combined.at[i].get('tourney_id','')) in expected_gc_ids):
                    combined.at[i, 'indoor_outdoor'] = 'O'
            except Exception:
                continue

    # Nettoyage colonnes variants indoor/outdoor et colonnes globales non désirées
    cols_to_drop_indoor_variants = [c for c in combined.columns if re.search(r'(?i)(indoor|outdoor)', c) and c != 'indoor_outdoor']
    if cols_to_drop_indoor_variants:
        combined.drop(columns=cols_to_drop_indoor_variants, inplace=True)
    cols_to_remove_global = ["doubles_draw_size","round_name","stage_type","stage_phase","stage_start_date","stage_end_date","group_name","best_of","start_time_confirmed"]
    for c in cols_to_remove_global:
        if c in combined.columns:
            combined.drop(columns=[c], inplace=True)

    # explicit merge pour revue en mémoire uniquement (ne sauvegarde pas)
    merged_for_review = explicit_merge_gc_non_gc(gc_all, normalized_non_gc, key="match_id")
    # enlever colonnes non désirées si présentes (toujours en mémoire)
    for c in cols_to_remove_global:
        for suf in ("", "_gc", "_non_gc"):
            col = c + suf
            if col in merged_for_review.columns:
                merged_for_review.drop(columns=[col], inplace=True)
    cols_to_drop_merge = [c for c in merged_for_review.columns if re.search(r'(?i)(indoor|outdoor)', c) and c not in ('indoor_outdoor', 'indoor_outdoor_merged')]
    if cols_to_drop_merge:
        merged_for_review.drop(columns=cols_to_drop_merge, inplace=True)
    if 'indoor_outdoor_merged' in merged_for_review.columns:
        if 'indoor_outdoor' not in merged_for_review.columns:
            merged_for_review['indoor_outdoor'] = None
        merged_for_review['indoor_outdoor'] = merged_for_review['indoor_outdoor_merged'].fillna(merged_for_review.get('indoor_outdoor'))
        merged_for_review.drop(columns=['indoor_outdoor_merged'], inplace=True)
    cols_to_drop_merge2 = [c for c in merged_for_review.columns if re.search(r'(?i)(indoor|outdoor)', c) and c != 'indoor_outdoor']
    if cols_to_drop_merge2:
        merged_for_review.drop(columns=cols_to_drop_merge2, inplace=True)

    # Standardisation sommaire des noms de rounds si présent
    if 'level' not in combined.columns and 'Level' in combined.columns:
        combined['level'] = combined['Level']
    if 'level' in combined.columns:
        combined['level'] = combined['level'].replace('Grand Slam','GC')
    if 'tourney_id' in combined.columns and 'round' in combined.columns:
        try:
            def std_round(row):
                try:
                    r = row['round']
                    mapping = {'final':'F','F':'F','semifinal':'SF','S':'SF','quarterfinal':'QF','Q':'QF','round_of_16':'R16','16':'R16','round_of_32':'R32','32':'R32','round_of_64':'R64','64':'R64'}
                    if r in mapping:
                        return mapping[r]
                    return r
                except Exception:
                    return row.get('round')
            combined['round'] = combined.apply(std_round, axis=1)
        except Exception:
            pass

    # ------------------- ENREGISTREMENT FINAL PAR TOURNOI -------------------
    per_tournament_results = {}
    tids_to_write = [tid for tid, _ in to_scrape]
    for tid in tids_to_write:
        tid_str = str(tid)
        frames = []
        # sélection robuste dans GC normalisé
        try:
            sel_gc = select_rows_for_tid(gc_all, tid)
            if not sel_gc.empty:
                frames.append(sel_gc)
        except Exception:
            pass
        # sélection robuste dans non-GC normalisé
        try:
            sel_non = select_rows_for_tid(normalized_non_gc, tid)
            if not sel_non.empty:
                frames.append(sel_non)
        except Exception:
            pass

        if frames:
            df_tid = pd.concat(frames, ignore_index=True, sort=False)
        else:
            # si aucune ligne trouvée, on crée un DataFrame vide avec colonnes CORE_COLS
            df_tid = pd.DataFrame(columns=CORE_COLS)

        # nettoyage final colonnes indésirables si présentes
        for c in cols_to_remove_global:
            if c in df_tid.columns:
                df_tid.drop(columns=[c], inplace=True)

        # ecriture UNIQUE sur disque : le CSV final par tournoi
        outfile = os.path.join(OUT_DIR, f"wta_{tid_str}_{year_str}.csv")
        try:
            df_tid.to_csv(outfile, index=False)
            if verbose:
                print(f"[OUT] Sauvé fichier final tournoi {tid_str} -> {outfile} ({len(df_tid)} rows)")
        except Exception as e:
            print(f"Erreur sauvegarde fichier tournoi {tid_str}: {e}")

        per_tournament_results[tid] = df_tid

    # suppression finale au cas où (redondant mais sûr) : effacer tout contenu temporaire éventuel
    cleanup_potential_temp_dir(verbose=verbose)

    # Debug GC manquants
    expected_gc_ids = [tid for tid, is_gc in to_scrape if is_gc]
    fetched_gc_ids = [tid for tid, df in gc_collected if (df is not None and len(df)>0)]
    missing_gcs = [tid for tid in expected_gc_ids if tid not in fetched_gc_ids]
    if missing_gcs:
        print(f"ATTENTION : les tournois GC suivants ont été appelés mais n'ont pas renvoyé de lignes: {missing_gcs}")
    else:
        if verbose:
            print(f"Année {year_str} : Toutes les GC attendues ont renvoyé au moins une ligne.")

    return per_tournament_results

# --------- Multi-year runner ----------
def run_years(years=None, tpc_map=None, verbose=True):
    # autoscanning des variables tournament_player_counts_<YYYY> dans globals()
    auto_map = {}
    g = globals()
    for name, val in g.items():
        m = re.match(r"^tournament_player_counts_(\d{4})$", name)
        if m and isinstance(val, dict):
            y = int(m.group(1))
            auto_map[y] = val

    final_map = {}
    if tpc_map:
        for k, v in tpc_map.items():
            final_map[int(k)] = v
    final_map.update(auto_map)

    if years:
        years_list = [int(y) for y in years]
    else:
        years_list = sorted(final_map.keys())

    if not years_list:
        print("Aucune année trouvée pour exécution (pas de dicts tournament_player_counts_<YYYY>).")
        return {}

    results = {}
    for y in years_list:
        print(f"\n\n>>> Lancement pipeline pour l'année {y} ...")
        tpc = final_map.get(int(y))
        if tpc is None:
            print(f"  -> Aucun dictionnaire tournament_player_counts_{y} trouvé — saut de l'année.")
            continue
        try:
            per_tid = main(year=y, tournament_player_counts=tpc, verbose=verbose)
            results[y] = per_tid
        except Exception as e:
            print(f"Erreur en traitant l'année {y}: {e}")
            continue
    return results

# --------- CLI ----------
def parse_args_and_run():
    parser = argparse.ArgumentParser(description="Lancer pipeline tennis pour une ou plusieurs années.")
    parser.add_argument("--years", help="Liste d'années séparées par des virgules, ex: 2024,2025", default=None)
    parser.add_argument("--all", help="Exécuter pour toutes les variables tournament_player_counts_<YYYY> trouvées", action="store_true")
    parser.add_argument("--verbose", help="Verbose output", action="store_true")
    args = parser.parse_args()

    years = None
    if args.years:
        years = [y.strip() for y in args.years.split(",") if y.strip()]

    if not args.all and not years:
        years = [str(YEAR)]

    if args.all:
        run_years(years=None, tpc_map=None, verbose=args.verbose)
    else:
        run_years(years=years, tpc_map=None, verbose=args.verbose)

if __name__ == "__main__":
    parse_args_and_run()
