#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
WTA scraper autonome.

Entrées CLI:
  --tournament-id 902
  --year 2026
  --num-players 32
  --mode first-round   # ou full

Logique:
  - mode first-round : on ne cherche que les matchs du premier tour
  - mode full : on cherche tous les matchs de LS001 à LS(num_players-1)
  - on effectue plusieurs passes sur les matchs manquants
  - en mode first-round, on s'arrête dès qu'on a récupéré le nombre
    de vrais matchs attendus au premier tour, puis on complète les BYE
  - les BYE sont uniquement gérés sur le premier tour:
      LS[start] .. LS[end]
    avec:
      start = (plus petite puissance de 2 >= num_players) / 2
      end   = (plus petite puissance de 2 >= num_players) - 1
  - les BYE sont extraits du HTML:
      https://www.wtatennis.com/tournaments/{tourney_id}/draws/{YEAR}/draws

Sortie:
  data_wta_test/wta_902_2026_temporary.json
"""

from __future__ import annotations

import argparse
import json
import re
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import requests
from bs4 import BeautifulSoup


MATCH_ID_PREFIX = "LS"
BASE_SCORE_URL = "https://api.wtatennis.com/tennis/tournaments/{tournament_id}/{year}/matches/{match_id}/score"
DRAW_URL = "https://www.wtatennis.com/tournaments/{tournament_id}/draws/{year}/draws"


# -------------------------
# Helpers génériques
# -------------------------
def _safe_get(d: Any, *keys, default=None):
    for k in keys:
        if isinstance(d, dict) and k in d:
            return d[k]
    return default


def _norm_str(x):
    if x is None:
        return None
    s = str(x).strip()
    return s if s else None


def _int_from_str(s):
    if s is None:
        return None
    m = re.search(r"(\d+)", str(s))
    return int(m.group(1)) if m else None


def _safe_int(x):
    try:
        if x is None:
            return None
        if isinstance(x, str) and x.strip() == "":
            return None
        return int(str(x).strip())
    except Exception:
        m = re.search(r"(\d+)", str(x))
        return int(m.group(1)) if m else None


def _format_set(a, b, tb=None):
    if a is None or a == "" or b is None or b == "":
        return None
    a_s = str(a).strip()
    b_s = str(b).strip()
    if tb is None or tb == "":
        return f"{a_s}-{b_s}"
    tb_s = str(tb).strip()
    if tb_s:
        return f"{a_s}-{b_s}({tb_s})"
    return f"{a_s}-{b_s}"


def _ensure_dir(path: Path):
    path.mkdir(parents=True, exist_ok=True)


def _match_code_num(match_id: str) -> int:
    m = re.search(r"(\d+)$", str(match_id))
    return int(m.group(1)) if m else 10**9


def next_power_of_two(n: int) -> int:
    if n <= 1:
        return 1
    return 1 << (n - 1).bit_length()


def first_round_bounds(num_players: int) -> Tuple[int, int]:
    """
    Premier tour du tableau principal:
      start = (plus petite puissance de 2 >= num_players) / 2
      end   = (plus petite puissance de 2 >= num_players) - 1
    """
    bracket_size = next_power_of_two(num_players)
    start = bracket_size // 2
    end = bracket_size - 1
    return start, end


def expected_first_round_real_matches(num_players: int) -> int:
    """
    Nombre de vrais matchs attendus au premier tour.
    Exemple:
      num_players=32  -> 16
      num_players=96  -> 32
    """
    bracket_size = next_power_of_two(num_players)
    return max(0, num_players - bracket_size // 2)


def expected_first_round_bye_slots(num_players: int) -> int:
    """
    Nombre de BYE au premier tour.
    Exemple:
      num_players=32  -> 0
      num_players=96  -> 32
    """
    bracket_size = next_power_of_two(num_players)
    return max(0, bracket_size - num_players)


def build_full_candidates(num_players: int) -> List[str]:
    """
    Mode full:
      LS001 .. LS(num_players-1)
    """
    end = max(1, num_players - 1)
    return [f"{MATCH_ID_PREFIX}{n:03d}" for n in range(1, end + 1)]


def build_first_round_candidates(num_players: int) -> List[str]:
    """
    Premier tour uniquement:
      LS(start) .. LS(end)
    """
    start, end = first_round_bounds(num_players)
    return [f"{MATCH_ID_PREFIX}{n:03d}" for n in range(start, end + 1)]


def build_candidate_codes(num_players: int, mode: str) -> List[str]:
    if mode == "first-round":
        return build_first_round_candidates(num_players)
    return build_full_candidates(num_players)


def build_first_round_codes(num_players: int) -> List[str]:
    return build_first_round_candidates(num_players)


# -------------------------
# Parsing du score WTA
# -------------------------
def _determine_winner_from_sets(score_data):
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

    if a_wins == b_wins == 0:
        return None, a_wins, b_wins
    return ("A", a_wins, b_wins) if a_wins > b_wins else ("B", a_wins, b_wins)


def _extract_player_fields(score_data: dict):
    player_a = f"{_safe_get(score_data, 'PlayerNameFirstA', '') or ''} {_safe_get(score_data, 'PlayerNameLastA', '') or ''}".strip()
    player_b = f"{_safe_get(score_data, 'PlayerNameFirstB', '') or ''} {_safe_get(score_data, 'PlayerNameLastB', '') or ''}".strip()

    pid_a = _safe_get(score_data, "PlayerIDA", "PlayerIdA", "playerida", "playerid_a")
    pid_b = _safe_get(score_data, "PlayerIDB", "PlayerIdB", "playeridb", "playerid_b")

    country_a = _safe_get(score_data, "PlayerCountryA", "PlayerCountryCodeA", "CountryA")
    country_b = _safe_get(score_data, "PlayerCountryB", "PlayerCountryCodeB", "CountryB")

    return (
        _norm_str(player_a),
        _norm_str(player_b),
        _norm_str(pid_a),
        _norm_str(pid_b),
        _norm_str(country_a),
        _norm_str(country_b),
    )


def _extract_score_string(score_data: dict):
    set1 = _format_set(score_data.get("ScoreSet1A"), score_data.get("ScoreSet1B"), score_data.get("ScoreTbSet1") or score_data.get("ScoreTb1"))
    set2 = _format_set(score_data.get("ScoreSet2A"), score_data.get("ScoreSet2B"), score_data.get("ScoreTbSet2") or score_data.get("ScoreTb2"))
    set3 = _format_set(score_data.get("ScoreSet3A"), score_data.get("ScoreSet3B"), score_data.get("ScoreTbSet3") or score_data.get("ScoreTb3"))
    set4 = _format_set(score_data.get("ScoreSet4A"), score_data.get("ScoreSet4B"), score_data.get("ScoreTbSet4") or score_data.get("ScoreTb4"))
    set5 = _format_set(score_data.get("ScoreSet5A"), score_data.get("ScoreSet5B"), score_data.get("ScoreTbSet5") or score_data.get("ScoreTb5"))

    score_string = _safe_get(score_data, "ScoreString") or _safe_get(score_data, "ResultString") or _safe_get(score_data, "score_string")
    if not score_string:
        parts = [s for s in (set1, set2, set3, set4, set5) if s]
        score_string = ",".join(parts) if parts else None

    return score_string


def parse_wta_match_summary(score_json: Any, tournament_id: int, year: int, match_id: str) -> Optional[dict]:
    """
    Retourne une entrée dès qu'on a les deux joueuses (noms/ids/pays),
    même si le match n'est pas terminé et que le vainqueur est inconnu.
    """
    if score_json is None:
        return None

    if isinstance(score_json, list):
        score_data = score_json[0] if score_json else None
    elif isinstance(score_json, dict):
        score_data = score_json
    else:
        score_data = None

    if not isinstance(score_data, dict):
        return None

    event_id = _safe_get(score_data, "EventID", "EventId", "eventId") or tournament_id
    event_year = _safe_get(score_data, "EventYear", "eventYear") or year

    player_a, player_b, pid_a, pid_b, country_a, country_b = _extract_player_fields(score_data)
    score_string = _extract_score_string(score_data)

    if not ((player_a or pid_a or country_a) and (player_b or pid_b or country_b)):
        return None

    winner_flag = None

    wf, _, _ = _determine_winner_from_sets(score_data)
    if wf is not None:
        winner_flag = wf

    if winner_flag is None:
        w_raw = _safe_get(score_data, "Winner", "winner", "WinnerId", "WinningPlayerId")
        if w_raw is not None:
            w_str = str(w_raw).strip()
            if w_str in ("1", "A", "a"):
                winner_flag = "A"
            elif w_str in ("2", "B", "b"):
                winner_flag = "B"
            elif pid_a and w_str == str(pid_a):
                winner_flag = "A"
            elif pid_b and w_str == str(pid_b):
                winner_flag = "B"

    if winner_flag is None:
        res = score_data.get("ResultString") or score_data.get("ScoreString")
        if isinstance(res, str):
            left = re.split(r"\d{1,2}-\d{1,2}", res)[0].strip()
            left = re.sub(r"^\[.*?\]", "", left).strip()
            pa = (player_a or "").lower()
            pb = (player_b or "").lower()
            ll = left.lower()
            if pa and (pa.split()[-1] in ll or pa.split()[0] in ll):
                winner_flag = "A"
            elif pb and (pb.split()[-1] in ll or pb.split()[0] in ll):
                winner_flag = "B"

    if winner_flag == "A":
        winner_name = player_a
        loser_name = player_b
        winner_id = pid_a
        loser_id = pid_b
        winner_country = country_a
        loser_country = country_b
    elif winner_flag == "B":
        winner_name = player_b
        loser_name = player_a
        winner_id = pid_b
        loser_id = pid_a
        winner_country = country_b
        loser_country = country_a
    else:
        winner_name = player_a
        loser_name = player_b
        winner_id = pid_a
        loser_id = pid_b
        winner_country = country_a
        loser_country = country_b

    return {
        "event_id": event_id,
        "event_year": event_year,
        "match_id": match_id,
        "winner_player_name": winner_name,
        "winner_player_id": winner_id,
        "loser_player_name": loser_name,
        "loser_player_id": loser_id,
        "winner_country": winner_country,
        "loser_wountry": loser_country,
        "score_string": score_string,
    }


# -------------------------
# HTML draws -> BYE
# -------------------------
def fetch_draws_html(session: requests.Session, tournament_id: int, year: int, timeout: int = 25) -> Optional[str]:
    url = DRAW_URL.format(tournament_id=tournament_id, year=year)
    resp = session.get(url, timeout=timeout)
    if resp.status_code != 200 or not resp.text:
        return None
    return resp.text


def _slug_to_name(slug: str) -> Optional[str]:
    if not slug:
        return None
    slug = slug.strip("/").split("/")[-1]
    parts = [p for p in slug.split("-") if p]
    if not parts:
        return None
    return " ".join(p[:1].upper() + p[1:] for p in parts)


def _extract_player_from_row(row) -> Optional[dict]:
    link = row.select_one("a.match-table__player--link")
    if not link:
        return None

    player_id = link.get("data-player-id")
    player_id = str(player_id).strip() if player_id is not None else None

    title = link.get("title") or link.get("aria-label")
    name_node = link.select_one(".match-table__player-fullname")
    display_name = name_node.get_text(" ", strip=True) if name_node else None

    href = link.get("href") or ""
    slug_name = _slug_to_name(href)

    player_name = title or display_name or slug_name

    if display_name and re.match(r"^[A-Z]\.\s+", display_name) and slug_name:
        player_name = slug_name

    country = None
    flag_img = link.select_one("img.flag__img[alt]")
    if flag_img and flag_img.get("alt"):
        country = flag_img.get("alt").strip().upper()

    if not country:
        flag_div = link.select_one(".flag")
        if flag_div:
            for cls in flag_div.get("class", []):
                m = re.match(r"flag--([A-Za-z]{2,3})", cls)
                if m:
                    country = m.group(1).upper()
                    break

    return {
        "player_name": _norm_str(player_name),
        "player_id": _norm_str(player_id),
        "country": _norm_str(country),
    }


def parse_bye_matches_from_draws_html(html: str) -> List[dict]:
    """
    Parcourt les tables du draw dans l'ordre du HTML.
    À chaque table contenant un BYE, on récupère le joueur adverse.
    L'ordre de sortie suit donc l'ordre d'apparition dans le HTML.
    """
    if not html:
        return []

    soup = BeautifulSoup(html, "html.parser")
    bye_entries: List[dict] = []

    for table in soup.select("table.match-table"):
        rows = table.select("tr.match-table__row")
        if not rows:
            continue

        player_info = None
        has_bye = False

        for row in rows:
            if row.select_one(".match-table__player-name--bye"):
                has_bye = True
                continue

            info = _extract_player_from_row(row)
            if info and info.get("player_name"):
                player_info = info

        if has_bye and player_info:
            bye_entries.append(
                {
                    "winner_player_name": player_info.get("player_name"),
                    "winner_player_id": player_info.get("player_id"),
                    "winner_country": player_info.get("country"),
                    "loser_player_name": "BYE",
                    "loser_player_id": None,
                    "loser_wountry": None,
                    "score_string": None,
                }
            )

    return bye_entries


# -------------------------
# Fetch + reprise
# -------------------------
def fetch_match_json(session: requests.Session, tournament_id: int, year: int, match_id: str, timeout: int = 20) -> Optional[Any]:
    url = BASE_SCORE_URL.format(tournament_id=tournament_id, year=year, match_id=match_id)
    resp = session.get(url, timeout=timeout)
    if resp.status_code != 200 or not resp.text:
        return None
    try:
        return resp.json()
    except Exception:
        return None


def is_debug_json_valid(debug_dir: Path, tourn_id: int, year: int, match_id: str, verbose: bool = False):
    path = debug_dir / f"raw_{tourn_id}_{year}_{match_id}.json"
    if not path.exists():
        return False, None

    try:
        txt = path.read_text(encoding="utf-8")
        if not txt.strip():
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

    if isinstance(j, dict) and (("Tournament" in j) or ("ScoreString" in j) or ("ResultString" in j) or ("MatchID" in j) or ("MatchId" in j)):
        if verbose:
            print(f"    [resume-check] {path} semble valide -> reuse")
        return True, j

    if isinstance(j, list) and len(j) > 0:
        if verbose:
            print(f"    [resume-check] {path} liste non vide -> reuse")
        return True, j

    if isinstance(j, dict) and len(j) > 0:
        if verbose:
            print(f"    [resume-check] {path} non-vide -> reuse (heuristique)")
        return True, j

    if verbose:
        print(f"    [resume-check] {path} non reconnu -> re-fetch")
    return False, None


def _make_bye_row(match_id: str, bye_info: dict, tournament_id: int, year: int) -> dict:
    return {
        "event_id": tournament_id,
        "event_year": year,
        "match_id": match_id,
        "winner_player_name": bye_info.get("winner_player_name"),
        "winner_player_id": bye_info.get("winner_player_id"),
        "loser_player_name": "BYE",
        "loser_player_id": None,
        "winner_country": bye_info.get("winner_country"),
        "loser_wountry": None,
        "score_string": None,
    }


def run_scrape_wta_tournament(
    tournament_id: int,
    year: int,
    num_players: int,
    out_folder: str = "data_wta",
    mode: str = "full",
    verbose: bool = True,
    max_attempts_per_match: int = 5,
    wait_between_rounds: float = 2.0,
    wait_between_retries: float = 1.0,
) -> Optional[Path]:
    out_folder = Path(out_folder)
    _ensure_dir(out_folder)

    debug_dir = out_folder / "debug_jsons"
    _ensure_dir(debug_dir)

    logs_dir = out_folder / "logs"
    _ensure_dir(logs_dir)

    session = requests.Session()
    session.headers.update(
        {
            "User-Agent": "Mozilla/5.0 (compatible; WTA-Scraper/1.0)",
            "Accept": "application/json,text/plain,*/*,text/html;q=0.9",
        }
    )

    candidate_codes = build_candidate_codes(int(num_players), mode)
    first_round_codes = build_first_round_codes(int(num_players))
    fr_start, fr_end = first_round_bounds(num_players)
    first_round_real_target = expected_first_round_real_matches(num_players)
    first_round_bye_target = expected_first_round_bye_slots(num_players)

    if verbose:
        print(f"=== TOURNOI {tournament_id} ({year}) ===")
        print(f"[config] num_players={num_players}")
        print(f"[config] mode={mode}")
        if candidate_codes:
            print(f"[config] candidate LS range: {candidate_codes[0]}..{candidate_codes[-1]}")
        print(f"[config] first-round fill range: LS{fr_start:03d}..LS{fr_end:03d}")
        print(f"[config] first-round real-match target: {first_round_real_target}")
        print(f"[config] first-round BYE target: {first_round_bye_target}")
        print(f"[config] max_attempts_per_match={max_attempts_per_match}")

    real_rows_by_code: Dict[str, dict] = {}
    attempts: Dict[str, int] = {code: 0 for code in candidate_codes}
    total_fetches = 0

    if verbose:
        print("[phase 1] passes successives sur les matchs candidats.")

    pending = list(candidate_codes)
    stop_early = False

    for round_idx in range(1, max_attempts_per_match + 1):
        if not pending or stop_early:
            break

        if verbose:
            print(f"[round {round_idx}/{max_attempts_per_match}] {len(pending)} matchs encore manquants")

        next_pending = []

        for match_id in pending:
            if match_id in real_rows_by_code:
                continue

            if mode == "first-round" and len(real_rows_by_code) >= first_round_real_target:
                stop_early = True
                break

            ok, j = is_debug_json_valid(debug_dir, tournament_id, year, match_id, verbose=verbose)
            if ok:
                parsed = parse_wta_match_summary(j, tournament_id, year, match_id)
                if parsed:
                    real_rows_by_code[match_id] = parsed
                    if verbose:
                        print(f"  [resume] {match_id} -> OK")
                    if mode == "first-round" and len(real_rows_by_code) >= first_round_real_target:
                        stop_early = True
                        break
                    continue

            if attempts.get(match_id, 0) >= max_attempts_per_match:
                if verbose:
                    print(f"  [skip] {match_id} a atteint le max de retries")
                continue

            attempts[match_id] = attempts.get(match_id, 0) + 1
            total_fetches += 1

            if verbose:
                print(f"  [fetch] #{total_fetches} -> {match_id} (tentative {attempts[match_id]}/{max_attempts_per_match})")

            try:
                data = fetch_match_json(session, tournament_id, year, match_id)
            except Exception as e:
                data = None
                if verbose:
                    print(f"    [error] fetch failed: {e}")

            raw_path = debug_dir / f"raw_{tournament_id}_{year}_{match_id}.json"
            try:
                if data is None:
                    raw_path.write_text(
                        json.dumps(
                            {
                                "error": "no json returned",
                                "url": BASE_SCORE_URL.format(tournament_id=tournament_id, year=year, match_id=match_id),
                                "attempt": attempts[match_id],
                            },
                            ensure_ascii=False,
                        ),
                        encoding="utf-8",
                    )
                else:
                    raw_path.write_text(json.dumps(data, indent=2, ensure_ascii=False), encoding="utf-8")
            except Exception as e:
                if verbose:
                    print(f"    [warn] couldn't write debug file: {e}")

            parsed = parse_wta_match_summary(data, tournament_id, year, match_id) if data else None
            if parsed:
                real_rows_by_code[match_id] = parsed
                if verbose:
                    print(f"    parsed OK -> {len(real_rows_by_code)} matchs réels collectés")

                if mode == "first-round" and len(real_rows_by_code) >= first_round_real_target:
                    stop_early = True
                    break
            else:
                next_pending.append(match_id)
                if verbose:
                    print("    encore manquant")

            time.sleep(wait_between_retries)

        pending = next_pending

        if pending and round_idx < max_attempts_per_match and not stop_early:
            time.sleep(wait_between_rounds)

    # -------------------------
    # Complétion BYE du premier tour uniquement
    # -------------------------
    if verbose:
        print("[phase 2] récupération des BYE depuis le HTML des draws.")

    bye_rows = []
    try:
        draws_html = fetch_draws_html(session, tournament_id, year)
        if draws_html:
            bye_rows = parse_bye_matches_from_draws_html(draws_html)
            if verbose:
                print(f"  [draws] {len(bye_rows)} matchs BYE extraits du HTML")
        else:
            if verbose:
                print("  [draws] aucun HTML récupéré")
    except Exception as e:
        if verbose:
            print(f"  [draws] erreur lors du fetch/parse HTML: {e}")

    missing_first_round = [code for code in first_round_codes if code not in real_rows_by_code]

    if verbose:
        print(f"  [draws] {len(missing_first_round)} codes du premier tour manquent encore dans le JSON")

    for match_id, bye_info in zip(missing_first_round, bye_rows):
        real_rows_by_code[match_id] = _make_bye_row(match_id, bye_info, tournament_id, year)
        if verbose:
            print(f"    [bye] {match_id} complété avec {bye_info.get('winner_player_name')}")

    if len(bye_rows) < len(missing_first_round) and verbose:
        print(f"  [warn] BYE trouvés ({len(bye_rows)}) < codes manquants ({len(missing_first_round)})")

    # -------------------------
    # Final JSON
    # -------------------------
    rows_sorted = sorted(real_rows_by_code.values(), key=lambda x: _match_code_num(x.get("match_id") or ""))

    event_id = None
    event_year = year
    for r in rows_sorted:
        if r.get("event_id") is not None:
            event_id = r.get("event_id")
            break
    if not event_id:
        event_id = tournament_id

    out_data = {
        "event_id": event_id,
        "event_year": event_year,
        "matches": [
            {
                "match_id": r.get("match_id"),
                "winner_player_name": r.get("winner_player_name"),
                "winner_player_id": r.get("winner_player_id"),
                "loser_player_name": r.get("loser_player_name"),
                "loser_player_id": r.get("loser_player_id"),
                "winner_country": r.get("winner_country"),
                "loser_wountry": r.get("loser_wountry"),
                "score_string": r.get("score_string"),
            }
            for r in rows_sorted
        ],
    }

    outpath = out_folder / f"wta_{tournament_id}_{year}_temporary.json"
    outpath.write_text(json.dumps(out_data, indent=2, ensure_ascii=False), encoding="utf-8")

    if verbose:
        print(f"[proc] sauvegardé -> {outpath} ({len(rows_sorted)} matches)")

    expected_count = len(candidate_codes)
    if len(rows_sorted) < expected_count:
        missing_after = [c for c in candidate_codes if c not in real_rows_by_code]
        log_data = {
            "tournament_id": tournament_id,
            "year": year,
            "mode": mode,
            "expected_matches": expected_count,
            "obtained_matches": len(rows_sorted),
            "total_fetches": total_fetches,
            "missing_match_codes_sample": missing_after[:50],
            "timestamp": datetime.now().isoformat(),
        }
        log_path = logs_dir / f"failed_{tournament_id}_{year}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        try:
            log_path.write_text(json.dumps(log_data, indent=2, ensure_ascii=False), encoding="utf-8")
            if verbose:
                print(f"[log] tournoi partiel -> {log_path}")
        except Exception as e:
            if verbose:
                print(f"[warn] impossible d'écrire le log: {e}")

    return outpath


# -------------------------
# CLI
# -------------------------
def _build_arg_parser():
    p = argparse.ArgumentParser(description="Scraper WTA autonome -> JSON résumé par tournoi.")
    p.add_argument("--tournament-id", type=int, required=True, help="ID du tournoi WTA.")
    p.add_argument("--year", type=int, required=True, help="Année du tournoi.")
    p.add_argument("--num-players", type=int, required=True, help="Nombre de joueurs dans le tableau.")
    p.add_argument("--out-folder", "--out-dir", dest="out_folder", type=str, default="data_wta", help="Dossier de sortie.")
    p.add_argument(
        "--mode",
        choices=["full", "first-round"],
        default="full",
        help="full = tous les matchs de LS001 à LS(num_players-1) ; first-round = uniquement le premier tour",
    )
    p.add_argument("--max-attempts", type=int, default=5, help="Nombre max de passes/retries par match.")
    p.add_argument("--wait-between-retries", type=float, default=1.0, help="Pause entre deux retries.")
    p.add_argument("--wait-between-rounds", type=float, default=2.0, help="Pause entre deux passes complètes.")
    p.add_argument("--verbose", "-v", action="store_true", help="Mode verbeux.")
    return p


def main(argv=None):
    parser = _build_arg_parser()
    args = parser.parse_args(argv)

    outpath = run_scrape_wta_tournament(
        tournament_id=args.tournament_id,
        year=args.year,
        num_players=args.num_players,
        out_folder=args.out_folder,
        mode=args.mode,
        verbose=args.verbose,
        max_attempts_per_match=args.max_attempts,
        wait_between_rounds=args.wait_between_rounds,
        wait_between_retries=args.wait_between_retries,
    )

    if outpath is None:
        sys.exit(1)


if __name__ == "__main__":
    main()