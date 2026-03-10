#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Scraper ATP multi-année avec reprise (skip des raw JSONs déjà existants).
Adaptation : sélection automatique des tournois à scraper à partir d'un
fichier JSON (structure "TournamentDates" fournie par l'API/site),
sélection effectuée si (end_date + days_after_end) == today (Europe/Paris).
- conserve fetch_with_playwright EXACTEMENT comme fourni.
- run_scrape_with_retries inclut la logique de reprise (vérifie debug_jsons/raw_...).
- run_scrape_multi_year permet de lancer plusieurs années en une seule exécution.

Sortie : appelle run_scrape_multi_year avec { year: { tourn_id: [draw_size, start_iso, end_iso, is_gs] } }
"""

# ----------------- TA FONCTION (COPIÉE EXACTEMENT) -----------------
from playwright.sync_api import sync_playwright
import json
from textwrap import shorten

URL = "https://www.atptour.com/-/Hawkeye/MatchStats/Complete/2026/339/MS001"

def fetch_with_playwright(headless: bool = True):
    with sync_playwright() as pw:
        browser = pw.chromium.launch(headless=headless)  # headless=False affiche le navigateur
        context = browser.new_context(locale="fr-FR")
        page = context.new_page()

        # Optionnel: intercepter la requête réseau et récupérer la réponse exacte
        # Nous allons simplement faire une navigation directe et récupérer la réponse de la requête
        print("Ouverture de la page ...")
        response = page.goto(URL, wait_until="networkidle", timeout=30000)
        if response is None:
            print("Aucune réponse réseau pour la navigation (timeout ou bloqué).")
            browser.close()
            return None

        status = response.status
        print("Status:", status)
        body = response.text()
        # Le endpoint renvoie normalement directement un JSON textuel
        # Tenter de parser
        try:
            data = json.loads(body)
            with open("match_stats_playwright.json", "w", encoding="utf-8") as f:
                json.dump(data, f, indent=2, ensure_ascii=False)
            print("JSON récupéré et sauvegardé dans match_stats_playwright.json")
            browser.close()
            return data
        except Exception as e:
            print("Impossible de parser le corps en JSON (taille={}, début={})".format(
                len(body), shorten(body, width=800)))
            browser.close()
            return None

# ----------------- FIN de la copie exacte -----------------

# ---------- Imports & helpers ----------
import os, time, re, argparse, sys
import pandas as pd
from datetime import datetime, timedelta, date
from pathlib import Path
from typing import Dict, Any, Tuple, Optional
try:
    from zoneinfo import ZoneInfo
except Exception:
    ZoneInfo = None

BASE_URL_TEMPLATE = "https://www.atptour.com/-/Hawkeye/MatchStats/Complete/{year}/{tournament_id}/{match_code}"

def calculate_match_id_range(num_players):
    return range(1, int(num_players + num_players / 3) + 1)

def _safe(d, *keys, default=None):
    for k in keys:
        if isinstance(d, dict) and k in d:
            return d[k]
    return default

def _norm_str(x):
    if x is None:
        return None
    s = str(x).strip()
    return s if s != "" else None

def _int_or_none(x):
    try:
        if x is None:
            return None
        return int(x)
    except Exception:
        m = re.search(r"(\d+)", str(x))
        return int(m.group(1)) if m else None

def _build_set_string(a_score, b_score, a_tb=None, b_tb=None):
    if a_score is None or b_score is None:
        return None
    a_s = str(a_score).strip()
    b_s = str(b_score).strip()
    if a_s == "" or b_s == "":
        return None
    tb = None
    if a_tb is not None and str(a_tb).strip() != "":
        tb = str(a_tb).strip()
    elif b_tb is not None and str(b_tb).strip() != "":
        tb = str(b_tb).strip()
    base = f"{a_s}-{b_s}"
    if tb is not None:
        m = re.search(r"(\d+)", tb)
        if m:
            return f"{base}({m.group(1)})"
    return base

def _flatten_stats_for_set(stats_obj):
    out = {}
    if not isinstance(stats_obj, dict):
        return out
    for k, v in stats_obj.items():
        if isinstance(v, dict):
            if 'Number' in v and v['Number'] is not None:
                out[k] = v['Number']
            elif 'Percent' in v and v['Percent'] is not None:
                out[k + "_percent"] = v['Percent']
                if 'Dividend' in v:
                    out[k + "_dividend"] = v.get('Dividend')
                if 'Divisor' in v:
                    out[k + "_divisor"] = v.get('Divisor')
            elif 'Dividend' in v and v['Dividend'] is not None:
                out[k + "_dividend"] = v['Dividend']
            else:
                found = False
                for subk, subv in v.items():
                    if isinstance(subv, (int, float)) and subv is not None:
                        out[f"{k}_{subk}"] = subv
                        found = True
                if not found:
                    out[k] = None
        else:
            out[k] = v
    return out

# ---------- parsing function (inchangé) ----------
def parse_atp_match_json(j: Dict[str, Any], year=None, tournament_id=None, verbose=False):
    """
    Parse JSON and produce a flat dict. Statistic keys are generated as:
      - <stat>_tot_winner / <stat>_tot_loser  (total match stats)
      - <stat>_set{n}_winner / <stat>_set{n}_loser (per-set stats)
      - tiebreak_set{n}_winner/loser, settime_set{n}_winner/loser etc.
    The determination of which side (playerteam1/playerteam2) maps to winner/loser
    is done using the winner flag inferred from the JSON (WinningPlayerId / Winner)
    or by counting set scores (fallback).
    """
    if j is None or not isinstance(j, dict):
        return None
    out = {}
    tour = _safe(j, "Tournament") or {}
    out["event_id"] = _norm_str(_safe(tour, "EventId", "EventID"))
    out["event_year"] = _int_or_none(_safe(tour, "EventYear", "Year"))
    out["tourney_name"] = _safe(tour, "EventDisplayName") or _safe(tour, "TournamentName")
    out["level"] = _safe(tour, "EventType") or None
    out["start_date"] = _norm_str(_safe(tour, "StartDate"))
    out["end_date"] = _norm_str(_safe(tour, "EndDate"))
    out["surface"] = _safe(tour, "Court") or _safe(tour, "CourtName")
    out["singles_draw_size"] = _safe(tour, "Singles")
    out["prize_money"] = _safe(tour, "PrizeMoney")
    out["prize_money_currency"] = _safe(tour, "CurrencySymbol")

    match = _safe(j, "Match") or {}
    out["match_id"] = _norm_str(_safe(match, "MatchId", "MatchID"))
    out["round"] = _safe(match, "RoundName") or (_safe(match, "Round") or {}).get("ShortName") or (_safe(match, "Round") or {}).get("LongName")
    out["match_time_total"] = _safe(match, "MatchTimeTotal")
    out["match_message"] = _safe(match, "Message") or _safe(match, "ExtendedMessage")
    out["match_status"] = _safe(match, "MatchStatus") or _safe(match, "Status")
    out["num_sets"] = _int_or_none(_safe(match, "NumberOfSets") or _safe(match, "NumberOfSets"))
    out["winner_flag_raw"] = _norm_str(_safe(match, "WinningPlayerId") or _safe(match, "Winner"))

    pt1 = _safe(match, "PlayerTeam1") or _safe(match, "PlayerTeam") or {}
    pt2 = _safe(match, "PlayerTeam2") or _safe(match, "OpponentTeam") or {}
    p1 = _safe(pt1, "Player") or {}
    p2 = _safe(pt2, "Player") or {}

    p1_first = _safe(pt1, "PlayerFirstName") or _safe(p1, "PlayerFirstName") or _safe(p1, "PlayerFirstNameFull")
    p1_last = _safe(pt1, "PlayerLastName") or _safe(p1, "PlayerLastName") or _safe(p1, "PlayerLastName")
    p2_first = _safe(pt2, "PlayerFirstName") or _safe(p2, "PlayerFirstName") or _safe(p2, "PlayerFirstNameFull")
    p2_last = _safe(pt2, "PlayerLastName") or _safe(p2, "PlayerLastName") or _safe(p2, "PlayerLastName")

    player_a = (_norm_str(p1_first) + " " + _norm_str(p1_last)).strip() if (p1_first or p1_last) else (_safe(p1, "PlayerFirstNameFull") or _safe(p1, "PlayerName") or None)
    player_b = (_norm_str(p2_first) + " " + _norm_str(p2_last)).strip() if (p2_first or p2_last) else (_safe(p2, "PlayerFirstNameFull") or _safe(p2, "PlayerName") or None)

    out["player_a"] = player_a
    out["player_b"] = player_b
    out["player_a_id"] = _norm_str(_safe(p1, "PlayerId") or _safe(pt1, "PlayerId"))
    out["player_b_id"] = _norm_str(_safe(p2, "PlayerId") or _safe(pt2, "PlayerId"))
    out["country_a"] = _safe(p1, "PlayerCountry") or _safe(p1, "PlayerCountryCode") or _safe(pt1, "PlayerCountryCode")
    out["country_b"] = _safe(p2, "PlayerCountry") or _safe(p2, "PlayerCountryCode") or _safe(pt2, "PlayerCountryCode")
    out["seed_a"] = _safe(pt1, "SeedPlayerTeam") or _safe(pt1, "Seed")
    out["seed_b"] = _safe(pt2, "SeedPlayerTeam") or _safe(pt2, "Seed")

    # collect per-set scores and tiebreaks
    sets_a = {}; sets_b = {}; tb_a = {}; tb_b = {}
    for s in _safe(pt1, "Sets", []) or []:
        sn = _int_or_none(_safe(s, "SetNumber"))
        if sn is None:
            continue
        sets_a[sn] = _norm_str(_safe(s, "SetScore"))
        tb_a[sn] = _safe(s, "TieBreakScore")
    for s in _safe(pt2, "Sets", []) or []:
        sn = _int_or_none(_safe(s, "SetNumber"))
        if sn is None:
            continue
        sets_b[sn] = _norm_str(_safe(s, "SetScore"))
        tb_b[sn] = _safe(s, "TieBreakScore")

    set_nums = sorted([n for n in set(list(sets_a.keys()) + list(sets_b.keys())) if n != 0])
    if not set_nums and out.get("num_sets"):
        set_nums = list(range(1, int(out["num_sets"]) + 1))
    all_scores = {}
    for i in range(1, 6):
        a_sc = sets_a.get(i)
        b_sc = sets_b.get(i)
        a_tb = tb_a.get(i)
        b_tb = tb_b.get(i)
        sstr = _build_set_string(a_sc, b_sc, a_tb=a_tb, b_tb=b_tb)
        all_scores[f"set{i}_score"] = sstr

    if _safe(match, "Message") or _safe(match, "ExtendedMessage"):
        out["score_string"] = _norm_str(_safe(match, "ExtendedMessage") or _safe(match, "Message"))
    else:
        joined = ",".join([all_scores[f"set{i}_score"] for i in range(1,6) if all_scores[f"set{i}_score"]])
        out["score_string"] = joined if joined else None

    # Determine winner label A/B (playerteam1/playerteam2) using WinningPlayerId or scored sets fallback
    winner_raw = out.get("winner_flag_raw")
    winner_label = None
    if winner_raw:
        pid_a = out.get("player_a_id")
        pid_b = out.get("player_b_id")
        if pid_a and str(winner_raw).strip() == str(pid_a).strip():
            winner_label = 'A'
        elif pid_b and str(winner_raw).strip() == str(pid_b).strip():
            winner_label = 'B'
        else:
            wr = str(winner_raw).strip().lower()
            if pid_a and wr.endswith(str(pid_a).strip().lower()):
                winner_label = 'A'
            elif pid_b and wr.endswith(str(pid_b).strip().lower()):
                winner_label = 'B'
            else:
                if pid_a and pid_a.lower() in wr:
                    winner_label = 'A'
                elif pid_b and pid_b.lower() in wr:
                    winner_label = 'B'

    if winner_label is None:
        a_wins = b_wins = 0
        for i in range(1, 6):
            s = all_scores.get(f"set{i}_score")
            if not s:
                continue
            m = re.match(r"^\s*(\d+)\s*-\s*(\d+)", s)
            if not m:
                continue
            a_v = int(m.group(1)); b_v = int(m.group(2))
            if a_v > b_v:
                a_wins += 1
            elif b_v > a_v:
                b_wins += 1
        if a_wins > b_wins:
            winner_label = 'A'
        elif b_wins > a_wins:
            winner_label = 'B'
        else:
            winner_label = None

    # Populate winner/loser metadata
    if winner_label == 'A':
        out["winner_player_name"] = out["player_a"]
        out["loser_player_name"] = out["player_b"]
        out["winner_seed"] = out.get("seed_a")
        out["loser_seed"] = out.get("seed_b")
        out["winner_country"] = out.get("country_a")
        out["loser_country"] = out.get("country_b")
    elif winner_label == 'B':
        out["winner_player_name"] = out["player_b"]
        out["loser_player_name"] = out["player_a"]
        out["winner_seed"] = out.get("seed_b")
        out["loser_seed"] = out.get("seed_a")
        out["winner_country"] = out.get("country_b")
        out["loser_country"] = out.get("country_a")
    else:
        out["winner_player_name"] = None
        out["loser_player_name"] = None
        out["winner_seed"] = None
        out["loser_seed"] = None
        out["winner_country"] = None
        out["loser_country"] = None

    # Put set scores in out
    out["set1_score"] = all_scores["set1_score"]
    out["set2_score"] = all_scores["set2_score"]
    out["set3_score"] = all_scores["set3_score"]
    out["set4_score"] = all_scores["set4_score"]
    out["set5_score"] = all_scores["set5_score"]
    out["match_date"] = _norm_str(_safe(match, "MatchTime") or _safe(match, "MatchTimeStamp") or _safe(tour, "StartDate"))

    # ---------- Build stats and map _a/_b -> _winner/_loser ----------
    stats_cols = {}

    def _find_set_stats(playerteam, setnum):
        if not playerteam:
            return None
        sets = _safe(playerteam, "SetScores") or _safe(playerteam, "Sets") or []
        for s in sets:
            sn = _int_or_none(_safe(s, "SetNumber"))
            if sn == setnum:
                return _safe(s, "Stats") or None
        return None

    observed_setnums = set([n for n in set_nums]) if set_nums else set(range(1, (out.get("num_sets") or 3) + 1))

    # Helper to decide which side maps to winner/loser
    def side_to_label(side_a: bool):
        # side_a True -> this is playerteam1 side
        if winner_label == 'A':
            return "winner" if side_a else "loser"
        elif winner_label == 'B':
            return "loser" if side_a else "winner"
        else:
            # unknown winner: fallback to 'a'/'b' suffix to avoid data loss
            return "a" if side_a else "b"

    for sn in sorted(observed_setnums):
        stats1 = _find_set_stats(pt1, sn)
        stats2 = _find_set_stats(pt2, sn)
        for block_name in ("ServiceStats", "ReturnStats", "PointStats"):
            b1 = _safe(stats1, block_name) if stats1 else None
            b2 = _safe(stats2, block_name) if stats2 else None
            flat1 = _flatten_stats_for_set(b1)
            flat2 = _flatten_stats_for_set(b2)
            # playerteam1 -> side_a True
            side_label1 = side_to_label(True)
            side_label2 = side_to_label(False)
            for k, v in flat1.items():
                key = f"{k.lower()}_set{sn}_{side_label1}"
                stats_cols[key] = v
            for k, v in flat2.items():
                key = f"{k.lower()}_set{sn}_{side_label2}"
                stats_cols[key] = v

        # tiebreaks and set times (map same way)
        sets1 = _safe(pt1, "SetScores") or _safe(pt1, "Sets") or []
        sets2 = _safe(pt2, "SetScores") or _safe(pt2, "Sets") or []
        s1 = next((s for s in sets1 if _int_or_none(_safe(s, "SetNumber")) == sn), None)
        s2 = next((s for s in sets2 if _int_or_none(_safe(s, "SetNumber")) == sn), None)
        if s1:
            tb = _safe(s1, "TieBreakScore")
            if tb not in (None, ""):
                stats_cols[f"tiebreak_set{sn}_{side_to_label(True)}"] = tb
            time_s = _safe(s1, "Time")
            if time_s:
                stats_cols[f"settime_set{sn}_{side_to_label(True)}"] = time_s
        if s2:
            tb = _safe(s2, "TieBreakScore")
            if tb not in (None, ""):
                stats_cols[f"tiebreak_set{sn}_{side_to_label(False)}"] = tb
            time_s = _safe(s2, "Time")
            if time_s:
                stats_cols[f"settime_set{sn}_{side_to_label(False)}"] = time_s

    # Totals (SetNumber == 0) -> flatten and map to winner/loser
    totals1 = _find_set_stats(pt1, 0)
    totals2 = _find_set_stats(pt2, 0)
    if totals1 or totals2:
        flat1 = _flatten_stats_for_set(_safe(totals1, "ServiceStats") or {})
        flat2 = _flatten_stats_for_set(_safe(totals2, "ServiceStats") or {})
        for k, v in flat1.items():
            key = f"{k.lower()}_tot_{side_to_label(True)}"
            stats_cols[key] = v
        for k, v in flat2.items():
            key = f"{k.lower()}_tot_{side_to_label(False)}"
            stats_cols[key] = v
        pflat1 = _flatten_stats_for_set(_safe(totals1, "PointStats") or {})
        pflat2 = _flatten_stats_for_set(_safe(totals2, "PointStats") or {})
        for k, v in pflat1.items():
            key = f"{k.lower()}_tot_{side_to_label(True)}"
            stats_cols[key] = v
        for k, v in pflat2.items():
            key = f"{k.lower()}_tot_{side_to_label(False)}"
            stats_cols[key] = v

    # Merge stats into out
    out.update(stats_cols)
    out["winner_flag"] = winner_label
    return out

# ---------- Helper: check existing debug json ----------
def is_debug_json_valid(debug_dir: Path, tourn_id: int, year: int, match_code: str, verbose: bool = False):
    path = debug_dir / f"raw_{tourn_id}_{year}_{match_code}.json"
    if not path.exists():
        return False, None
    try:
        txt = path.read_text(encoding="utf-8")
        if not txt or txt.strip() == "":
            if verbose:
                print(f"    [resume-check] {path} vide -> re-fetch")
            return False, None
        j = json.loads(txt)
    except Exception as e:
        if verbose:
            print(f"    [resume-check] impossible de parser {path} ({e}) -> re-fetch")
        return False, None

    if isinstance(j, dict) and ("error" in j or (j.get("url") and j.get("error"))):
        if verbose:
            print(f"    [resume-check] {path} contient une erreur -> re-fetch")
        return False, None

    if isinstance(j, dict) and ("Tournament" in j or "Match" in j):
        if verbose:
            print(f"    [resume-check] {path} semble valide -> reuse")
        return True, j

    if isinstance(j, dict) and len(j) > 0:
        if verbose:
            print(f"    [resume-check] {path} non-vide -> reuse (heuristique)")
        return True, j

    if verbose:
        print(f"    [resume-check] {path} non reconnu -> re-fetch")
    return False, None

# ---------- Orchestration avec reprise (inchangée) ----------
def run_scrape_with_retries(
    tournament_player_counts,
    year,
    out_folder="data_atp",
    headless=False,
    verbose=True,
    max_attempts_per_match=10,
    wait_between_rounds=5.0,
    wait_between_retries=4.0
):
    out_folder = Path(out_folder)
    out_folder.mkdir(parents=True, exist_ok=True)
    debug_dir = out_folder / "debug_jsons"
    debug_dir.mkdir(exist_ok=True)
    logs_dir = out_folder / "logs"
    logs_dir.mkdir(exist_ok=True)

    failed_summary = []

    for tourn_id, meta in tournament_player_counts.items():
        num_players = int(meta[0]) if isinstance(meta, (list, tuple)) else int(meta)
        required_matches = max(0, num_players - 1)
        candidate_codes = [f"MS{n:03d}" for n in range(1, required_matches + 1)]

        if verbose:
            print(f"\n=== TOURNOI {tourn_id} ({year}) : attendu {required_matches} matchs -> codes {candidate_codes[0]}..{candidate_codes[-1] if candidate_codes else 'N/A'} ===")

        rows = []
        collected_codes = set()
        per_match_attempts = {code: 0 for code in candidate_codes}
        total_fetches = 0

        # PHASE 1 : passe initiale (skip si raw exists valide)
        if verbose:
            print("[phase 1] passe initiale (avec reprise si debug_json existant).")
        for match_code in candidate_codes:
            ok, j = is_debug_json_valid(debug_dir, tourn_id, year, match_code, verbose=verbose)
            if ok:
                parsed = parse_atp_match_json(j, year=year, tournament_id=tourn_id, verbose=verbose)
                if parsed:
                    rows.append(parsed)
                    collected_codes.add(match_code)
                    if verbose:
                        print(f"  [resume] reused {match_code} -> collected {len(collected_codes)}/{required_matches}")
                    continue
                else:
                    if verbose:
                        print(f"  [resume] fichier existant mais parse failed -> refetch {match_code}")

            globals()['URL'] = BASE_URL_TEMPLATE.format(year=year, tournament_id=tourn_id, match_code=match_code)
            per_match_attempts[match_code] += 1
            total_fetches += 1
            if verbose:
                print(f"  [phase1] fetch #{total_fetches} -> {match_code} (attempt {per_match_attempts[match_code]}/{max_attempts_per_match})")

            try:
                data = fetch_with_playwright(headless=headless)
            except Exception as e:
                data = None
                if verbose:
                    print(f"    [error] fetch_with_playwright raised: {e}")

            safe_name = f"{tourn_id}_{year}_{match_code}"
            raw_path = debug_dir / f"raw_{safe_name}.json"
            try:
                if data is None:
                    raw_path.write_text(json.dumps({"error": "no json returned", "url": globals().get('URL')}), encoding="utf-8")
                else:
                    raw_path.write_text(json.dumps(data, indent=2, ensure_ascii=False), encoding="utf-8")
            except Exception as e:
                if verbose:
                    print(f"    [warn] couldn't write debug file: {e}")

            if data:
                parsed = parse_atp_match_json(data, year=year, tournament_id=tourn_id, verbose=verbose)
                if parsed:
                    rows.append(parsed)
                    collected_codes.add(match_code)
                    if verbose:
                        print(f"    parsed OK -> collected {len(collected_codes)}/{required_matches}")
                else:
                    if verbose:
                        print("    parse returned None (structure inattendue).")
            else:
                if verbose:
                    print("    aucun JSON obtenu pour ce match (voir debug).")

            time.sleep(0.4)

        # PHASE 2 : retries ciblés sur manquants (round-robin), avec re-check fichiers raw
        if len(collected_codes) < required_matches:
            missing = [c for c in candidate_codes if c not in collected_codes]
            if verbose:
                print(f"[phase 2] retries ciblés sur {len(missing)} manquants: {missing}. max_attempts_per_match={max_attempts_per_match}")

            round_no = 0
            while missing:
                round_no += 1
                if verbose:
                    print(f"  [retry-round {round_no}] remaining missing={len(missing)}")
                progressed = False
                for match_code in list(missing):
                    ok, j = is_debug_json_valid(debug_dir, tourn_id, year, match_code, verbose=verbose)
                    if ok:
                        parsed = parse_atp_match_json(j, year=year, tournament_id=tourn_id, verbose=verbose)
                        if parsed:
                            rows.append(parsed)
                            collected_codes.add(match_code)
                            missing.remove(match_code)
                            progressed = True
                            if verbose:
                                print(f"    [resume] reused during retry -> {match_code}")
                            continue
                        else:
                            if verbose:
                                print(f"    [resume] fichier existant mais parse failed -> refetch {match_code}")

                    if per_match_attempts[match_code] >= max_attempts_per_match:
                        if verbose:
                            print(f"    [skip] {match_code} reached max attempts ({per_match_attempts[match_code]})")
                        missing.remove(match_code)
                        continue

                    globals()['URL'] = BASE_URL_TEMPLATE.format(year=year, tournament_id=tourn_id, match_code=match_code)
                    per_match_attempts[match_code] += 1
                    total_fetches += 1
                    if verbose:
                        print(f"    [retry] fetch #{total_fetches} -> {match_code} (attempt {per_match_attempts[match_code]}/{max_attempts_per_match})")

                    try:
                        data = fetch_with_playwright(headless=headless)
                    except Exception as e:
                        data = None
                        if verbose:
                            print(f"      [error] fetch_with_playwright raised: {e}")

                    safe_name = f"{tourn_id}_{year}_{match_code}"
                    raw_path = debug_dir / f"raw_{safe_name}.json"
                    try:
                        if data is None:
                            raw_path.write_text(json.dumps({"error": "no json returned", "url": globals().get('URL'), "attempt": per_match_attempts[match_code]}), encoding="utf-8")
                        else:
                            raw_path.write_text(json.dumps(data, indent=2, ensure_ascii=False), encoding="utf-8")
                    except Exception as e:
                        if verbose:
                            print(f"      [warn] couldn't write debug file: {e}")

                    if data:
                        parsed = parse_atp_match_json(data, year=year, tournament_id=tourn_id, verbose=verbose)
                        if parsed:
                            rows.append(parsed)
                            collected_codes.add(match_code)
                            missing.remove(match_code)
                            progressed = True
                            if verbose:
                                print(f"      parsed OK -> collected {len(collected_codes)}/{required_matches}")
                        else:
                            if verbose:
                                print("      parse returned None (structure inattendue).")
                    else:
                        if verbose:
                            print("      aucun JSON obtenu pour ce match (voir debug).")

                    time.sleep(wait_between_retries)

                if not progressed:
                    any_left = any(per_match_attempts[c] < max_attempts_per_match for c in missing)
                    if not any_left:
                        if verbose:
                            print("  [stop] aucun progrès possible: tous les manquants ont atteint max_attempts_per_match.")
                        break
                    if verbose:
                        print("  [info] fin de round sans progrès mais certains ont encore des tentatives disponibles -> nouvelle passe.")
                    time.sleep(wait_between_rounds)

        # Sauvegarde CSV et logs
        if rows:
            df = pd.DataFrame(rows)
            ts = datetime.now().strftime("%Y%m%d_%H%M%S")
            outpath = out_folder / f"atp_{tourn_id}_{year}_{ts}.csv"
            df.to_csv(outpath, index=False)
            if verbose:
                print(f"[proc] sauvegardé -> {outpath} ({len(df)} lignes, {len(df.columns)} colonnes)")
        else:
            if verbose:
                print(f"[proc] aucune ligne recueillie pour tournoi {tourn_id}.")

        obtained = len(rows)
        if obtained < required_matches:
            missing_after = [c for c in candidate_codes if c not in collected_codes]
            log_data = {
                "tournament_id": tourn_id,
                "year": year,
                "expected_matches": required_matches,
                "obtained_matches": obtained,
                "total_fetches": total_fetches,
                "per_match_attempts_sample": {k: per_match_attempts.get(k,0) for k in candidate_codes[:50]},
                "missing_match_codes_sample": missing_after[:50],
                "timestamp": datetime.now().isoformat()
            }
            log_path = logs_dir / f"failed_{tourn_id}_{year}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
            try:
                log_path.write_text(json.dumps(log_data, indent=2, ensure_ascii=False), encoding="utf-8")
            except Exception as e:
                if verbose:
                    print(f"[warn] writing log failed: {e}")
            failed_summary.append(log_data)
            if verbose:
                print(f"[log] tournoi {tourn_id} partial: attendu={required_matches}, obtenu={obtained}. Log: {log_path}")

    # master summary
    if failed_summary:
        summary_rows = []
        for item in failed_summary:
            summary_rows.append({
                "tournament_id": item["tournament_id"],
                "year": item["year"],
                "expected_matches": item["expected_matches"],
                "obtained_matches": item["obtained_matches"],
                "total_fetches": item.get("total_fetches", "")
            })
        dfsum = pd.DataFrame(summary_rows)
        sum_path = logs_dir / f"failed_tournaments_summary_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        try:
            dfsum.to_csv(sum_path, index=False)
            if verbose:
                print(f"[summary] failed tournaments summary saved -> {sum_path}")
        except Exception as e:
            if verbose:
                print(f"[warn] could not save summary CSV: {e}")

    return

# ---------- Runner multi-année (inchangé) ----------
def run_scrape_multi_year(
    tournaments_by_year: Dict[int, Dict[int, Any]],
    out_folder="data_atp",
    headless=False,
    verbose=True,
    max_attempts_per_match=10,
    wait_between_rounds=5.0,
    wait_between_retries=4.0,
    pause_between_years=5.0
):
    """
    tournaments_by_year: { year_int: tournament_player_counts_dict }
    Itère les années triées et appelle run_scrape_with_retries pour chacune.
    """
    if not isinstance(tournaments_by_year, dict):
        raise ValueError("tournaments_by_year doit être un dict { year: tournament_dict }.")

    # vérifier forme basique
    years = sorted(tournaments_by_year.keys())
    if verbose:
        print(f"[multi-year] lancement pour années: {years}")

    for idx, y in enumerate(years):
        if verbose:
            print(f"\n>>> Début scraping pour l'année {y} ({idx+1}/{len(years)})")
        run_scrape_with_retries(
            tournaments_by_year[y],
            year=y,
            out_folder=out_folder,
            headless=headless,
            verbose=verbose,
            max_attempts_per_match=max_attempts_per_match,
            wait_between_rounds=wait_between_rounds,
            wait_between_retries=wait_between_retries
        )
        if idx < len(years) - 1:
            if verbose:
                print(f"[info] pause de {pause_between_years}s avant l'année suivante...")
            time.sleep(pause_between_years)

# ---------- NEW: parsing of Tournament JSON and date utils ----------
_MONTHS = {
    # english
    'january':1,'february':2,'march':3,'april':4,'may':5,'june':6,'july':7,'august':8,'september':9,'october':10,'november':11,'december':12,
    # french variants (just in case)
    'janvier':1,'fevrier':2,'février':2,'mars':3,'avril':4,'mai':5,'juin':6,'juillet':7,'aout':8,'août':8,'septembre':9,'octobre':10,'novembre':11,'decembre':12,'décembre':12
}

def _parse_day_month_part(part: str, default_month: Optional[int]=None) -> Tuple[Optional[int], Optional[int]]:
    """
    Parse '30 March' or '30' or 'March' returning (day, month)
    """
    if not part or not part.strip():
        return None, default_month
    p = part.strip()
    # try to find day
    mday = re.search(r"(\d{1,2})", p)
    day = int(mday.group(1)) if mday else None
    # find month word
    mmon = re.search(r"([A-Za-zéèêàùûôïçÉÈÊÀÙÛÔÏÇ]+)", p)
    month = None
    if mmon:
        mn = mmon.group(1).lower()
        month = _MONTHS.get(mn)
    if month is None:
        month = default_month
    return day, month

def parse_formatted_date(formatted_date: str) -> Tuple[Optional[str], Optional[str], Optional[int]]:
    """
    Parse strings like:
      - "12 - 17 January, 2026" -> ("2026-01-12", "2026-01-17", 2026)
      - "30 March - 5 April, 2026" -> ("2026-03-30", "2026-04-05", 2026)
      - "4 - 11 January, 2026"
      - "January, 2026" -> (None, None, 2026)
    Returns (start_iso, end_iso, year) where start_iso/end_iso are YYYY-MM-DD or None.
    """
    if not formatted_date or not formatted_date.strip():
        return None, None, None
    s = formatted_date.strip()
    # split year by comma
    parts = [p.strip() for p in s.rsplit(",", 1)]
    if len(parts) == 2:
        left, year_part = parts[0], parts[1]
    else:
        left = parts[0]; year_part = ""
    # try parse year
    year = None
    m = re.search(r"(\d{4})", year_part)
    if m:
        year = int(m.group(1))
    else:
        # maybe year present at end of left part (rare)
        m2 = re.search(r"(\d{4})", left)
        if m2:
            year = int(m2.group(1))
            # remove year from left
            left = re.sub(r"\d{4}", "", left).strip()

    # if left contains '-' -> two parts
    if '-' in left:
        a,b = [p.strip() for p in left.split('-',1)]
        # determine months: if a includes month -> use its month else default to b's month
        # parse b to get month
        day_b, month_b = _parse_day_month_part(b, default_month=None)
        # parse a using month_b as default
        day_a, month_a = _parse_day_month_part(a, default_month=month_b)
        # if month_a still None, use month_b
        if month_a is None:
            month_a = month_b
        # build ISO strings if possible
        start_iso = None
        end_iso = None
        try:
            if year and day_a and month_a:
                start_iso = date(year, month_a, day_a).isoformat()
        except Exception:
            start_iso = None
        try:
            if year and day_b and month_b:
                end_iso = date(year, month_b, day_b).isoformat()
        except Exception:
            end_iso = None
        return start_iso, end_iso, year
    else:
        # no dash: could be "11 January" or "January" or just "January 2026"
        day, month = _parse_day_month_part(left, default_month=None)
        start_iso = None; end_iso = None
        if year and day and month:
            start_iso = date(year, month, day).isoformat()
            end_iso = start_iso
        else:
            # if only month + year, returns None for dates but keep year
            start_iso = None; end_iso = None
        return start_iso, end_iso, year

def _today_in_paris() -> date:
    if ZoneInfo is not None:
        try:
            tz = ZoneInfo("Europe/Paris")
            now = datetime.now(tz)
            return now.date()
        except Exception:
            pass
    # fallback to naive local date
    return datetime.now().date()

def build_tournaments_by_year_from_json(tournaments_json_path: str, days_after_end: int = 1, verbose: bool = True, today_override: Optional[date]=None) -> Dict[int, Dict[int, Any]]:
    """
    Read the tournaments JSON (structure provided) and return a dict:
      { year: { tourn_id_int: [singles_draw_size, start_iso, end_iso, is_gs_int] } }
    Only includes tournaments where end_date + days_after_end == today_paris (or today_override).
    If a tournament has no parsable end_date, it is skipped.
    """
    p = Path(tournaments_json_path)
    if not p.exists():
        raise FileNotFoundError(f"{tournaments_json_path} not found")
    raw = json.loads(p.read_text(encoding="utf-8"))
    out: Dict[int, Dict[int, Any]] = {}
    today = today_override if today_override is not None else _today_in_paris()
    if verbose:
        print(f"[selector] today (Europe/Paris) = {today.isoformat()} -- days_after_end={days_after_end}")

    tdates = raw.get("TournamentDates") or []
    for month_block in tdates:
        tournaments = month_block.get("Tournaments") or []
        for t in tournaments:
            try:
                tid_raw = t.get("Id") or t.get("ID") or t.get("IdTournament")
                if tid_raw is None:
                    continue
                tid = int(str(tid_raw))
            except Exception:
                continue
            formatted = t.get("FormattedDate") or t.get("Formatted") or ""
            start_iso, end_iso, year = parse_formatted_date(formatted)
            if not year:
                # try fallback: maybe Year field present
                try:
                    year_fallback = int(t.get("Year")) if t.get("Year") else None
                    if year_fallback:
                        year = year_fallback
                except Exception:
                    pass
            if not end_iso:
                if verbose:
                    print(f"  [skip] tourn {tid} no parsable end date (FormattedDate='{formatted}')")
                continue
            # convert end_iso to date
            try:
                end_dt = datetime.fromisoformat(end_iso).date()
            except Exception:
                if verbose:
                    print(f"  [skip] tourn {tid} end date invalid iso '{end_iso}'")
                continue
            trigger_date = end_dt + timedelta(days=days_after_end)
            if trigger_date == today:
                sgl = t.get("SglDrawSize") or t.get("SglDraw") or t.get("Sgl")
                try:
                    sgl_n = int(sgl) if sgl is not None else 0
                except Exception:
                    sgl_n = 0
                typ = t.get("Type") or ""
                is_gs = 1 if str(typ).strip().upper() == "GS" else 0
                if year is None:
                    # try to extract from end_iso
                    try:
                        year = datetime.fromisoformat(end_iso).year
                    except Exception:
                        year = datetime.now().year
                if year not in out:
                    out[year] = {}
                out[year][tid] = [sgl_n, start_iso, end_iso, is_gs]
                if verbose:
                    print(f"  [select] tourn {tid} year={year} end={end_iso} draw={sgl_n} gs={is_gs}")
            else:
                if verbose:
                    if False:
                        print(f"    debug: tourn {tid} trigger_date={trigger_date.isoformat()} (today={today.isoformat()})")
    return out

# ---------- CLI / main ----------
def _build_arg_parser():
    p = argparse.ArgumentParser(description="Sélection + lancement du scraper ATP based on tournaments JSON.")
    p.add_argument("--tournaments-json", "-j", type=str, default="docs/atp_tournaments_2026.json", help="Path to tournaments JSON (structure with TournamentDates).")
    p.add_argument("--days-after-end", "-d", type=int, default=1, help="Days after tournament end to trigger scraping (default 1).")
    p.add_argument("--out-folder", "-o", type=str, default="data_atp", help="Output folder for scraped CSVs/debug.")
    p.add_argument("--headless", action="store_true", help="Run browsers in headless mode (default False).")
    p.add_argument("--max-attempts", type=int, default=10, help="max_attempts_per_match passed to run_scrape_with_retries.")
    p.add_argument("--wait-between-retries", type=float, default=4.0, help="wait_between_retries")
    p.add_argument("--wait-between-rounds", type=float, default=5.0, help="wait_between_rounds")
    p.add_argument("--pause-between-years", type=float, default=5.0, help="pause between years when running multi-year")
    p.add_argument("--today", type=str, default=None, help="Override 'today' (YYYY-MM-DD) for testing.")
    p.add_argument("--verbose", "-v", action="store_true", help="Verbose output")
    return p

def main(argv=None):
    parser = _build_arg_parser()
    args = parser.parse_args(argv)

    if args.today:
        try:
            today_override = datetime.fromisoformat(args.today).date()
        except Exception:
            print(f"Invalid --today value: {args.today}", file=sys.stderr)
            sys.exit(2)
    else:
        today_override = None

    try:
        tournaments_by_year = build_tournaments_by_year_from_json(
            args.tournaments_json,
            days_after_end=args.days_after_end,
            verbose=args.verbose,
            today_override=today_override
        )
    except FileNotFoundError as e:
        print(f"ERROR: {e}", file=sys.stderr)
        sys.exit(1)

    if not tournaments_by_year:
        if args.verbose:
            print("[main] Aucun tournoi sélectionné pour aujourd'hui. Rien à faire.")
        return

    if args.verbose:
        print("[main] tournaments_by_year prepared for scraping:")
        for y, d in tournaments_by_year.items():
            print(f"  year {y}: {len(d)} tournaments -> ids: {sorted(d.keys())}")

    # call runner
    run_scrape_multi_year(
        tournaments_by_year=tournaments_by_year,
        out_folder=args.out_folder,
        headless=args.headless,
        verbose=args.verbose,
        max_attempts_per_match=args.max_attempts,
        wait_between_rounds=args.wait_between_rounds,
        wait_between_retries=args.wait_between_retries,
        pause_between_years=args.pause_between_years
    )

if __name__ == "__main__":
    main()