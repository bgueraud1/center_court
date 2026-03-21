#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
ATP scraper autonome avec deux sources possibles:
- score: page MatchStats complete
- draw: page draws (utile quand le match n'est pas terminé)

Sortie par tournoi:
  atp_ID_YEAR_temporary.json

Contenu:
{
  "event_id": ...,
  "event_year": ...,
  "matches": [
    {
      "match_id": "MS016",
      "winner_player_name": ...,
      "winner_player_id": ...,
      "loser_player_name": ...,
      "loser_player_id": ...,
      "winner_country": ...,
      "loser_wountry": ...,
      "score_string": ...
    }
  ]
}

CLI exemples:
  python3 scripts/scraping_atp_temporary.py --tournament 902:2026:30:miami-open-presented-by-itau --match-source auto --out-folder data_atp_test --verbose
  python3 scripts/scraping_atp_temporary.py --tournament 902:2026:30:miami-open-presented-by-itau --match-source draw --out-folder data_atp_test --verbose
  python3 scripts/scraping_atp_temporary.py --tournament 902:2026:30:miami-open-presented-by-itau --tournament 339:2026:128:indian-wells-open --match-source score --out-folder data_atp_test --verbose
"""

from __future__ import annotations

import argparse
import json
import os
import re
import sys
import time
import unicodedata
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from playwright.sync_api import sync_playwright

try:
    from bs4 import BeautifulSoup
except Exception as e:
    raise RuntimeError("BeautifulSoup (bs4) est requis pour parser les pages draw ATP.") from e


CREATED_FILES: List[str] = []

SCORE_URL_TEMPLATE = "https://www.atptour.com/-/Hawkeye/MatchStats/Complete/{year}/{tournament_id}/{match_code}"
DRAW_URL_TEMPLATES = [
    "https://www.atptour.com/en/scores/current-challenger/{slug}/{tournament_id}/draws",
    "https://www.atptour.com/en/scores/current/{slug}/{tournament_id}/draws",
    "https://www.atptour.com/en/scores/archives/{slug}/{tournament_id}/draws",
]


# ---------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------

def _safe(d, *keys, default=None):
    for k in keys:
        if isinstance(d, dict) and k in d:
            return d[k]
    return default

def _all_match_info(num_players: int) -> Tuple[int, List[str]]:
    """
    En mode score, on veut aller jusqu'à la borne du draw:
      MS001 ... MS{draw_size - 1}
    Exemple: 28 joueurs -> draw_size=32 -> MS001..MS031
    """
    draw_size = _next_power_of_two(num_players)
    total_matches = max(draw_size - 1, 0)
    match_codes = [f"MS{n:03d}" for n in range(1, total_matches + 1)]
    return total_matches, match_codes


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


def _next_power_of_two(n: int) -> int:
    n = int(n)
    if n <= 1:
        return 1
    return 1 << (n - 1).bit_length()


def _normalize_key(x: Optional[str]) -> str:
    if not x:
        return ""
    s = unicodedata.normalize("NFKD", str(x))
    s = "".join(ch for ch in s if not unicodedata.combining(ch))
    s = re.sub(r"[^A-Za-z0-9]+", "", s).lower()
    return s


def _slugify(text: str) -> str:
    if not text:
        return text
    s = unicodedata.normalize("NFKD", str(text))
    s = s.encode("ascii", "ignore").decode("ascii")
    s = s.lower().strip()
    s = re.sub(r"[^a-z0-9]+", "-", s).strip("-")
    return s


def _ensure_dir(path: Path):
    path.mkdir(parents=True, exist_ok=True)


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


def _looks_like_actual_score(s: Optional[str]) -> bool:
    if not s:
        return False
    return bool(re.search(r"\d+\s*-\s*\d+", s))


def _parse_cli_tournament_spec(spec: str) -> Dict[str, Any]:
    """
    Format:
      TOURNAMENT_ID:YEAR:NUM_PLAYERS:SLUG
    Exemple:
      902:2026:30:miami-open-presented-by-itau
    """
    parts = [p.strip() for p in str(spec).split(":")]
    if len(parts) < 4:
        raise ValueError(
            f"Spec invalide: {spec!r}. Attendu: TOURNAMENT_ID:YEAR:NUM_PLAYERS:SLUG"
        )
    tournament_id = int(parts[0])
    year = int(parts[1])
    num_players = int(parts[2])
    slug = ":".join(parts[3:]).strip()
    if not slug:
        raise ValueError(f"Slug manquant dans la spec {spec!r}")
    return {
        "tournament_id": tournament_id,
        "year": year,
        "num_players": num_players,
        "slug": slug,
    }


def _first_round_info(num_players: int) -> Tuple[int, int, List[str]]:
    """
    Premier tour du draw:
      draw_size = puissance de 2 supérieure ou égale à num_players
      first_round_slots = draw_size / 2
      match codes = MS{first_round_slots}..MS{draw_size-1}
    Exemple: 30 joueurs -> draw_size=32 -> first_round_slots=16 -> MS016..MS031
    """
    draw_size = _next_power_of_two(num_players)
    first_round_slots = draw_size // 2
    match_codes = [f"MS{n:03d}" for n in range(first_round_slots, draw_size)]
    return draw_size, first_round_slots, match_codes


def _score_match_info(num_players: int) -> Tuple[int, List[str]]:
    """
    Tous les matchs d'un tournoi:
      MS001..MS{num_players - 1}
    """
    total_matches = max(int(num_players) - 1, 0)
    match_codes = [f"MS{n:03d}" for n in range(1, total_matches + 1)]
    return total_matches, match_codes


# ---------------------------------------------------------------------
# Fetch générique Playwright
# ---------------------------------------------------------------------

def fetch_url_with_playwright(url: str, headless: bool = True, expect_html: bool = False) -> Optional[str]:
    """
    Retourne le texte JSON ou le HTML, selon expect_html.
    """
    with sync_playwright() as pw:
        browser = pw.chromium.launch(headless=headless)
        context = browser.new_context(locale="en-US")
        page = context.new_page()
        try:
            if expect_html:
                page.goto(url, wait_until="domcontentloaded", timeout=30000)
                try:
                    page.wait_for_selector("select[id^='player-draw-select'], div.draw-content", timeout=15000)
                except Exception:
                    pass
                body = page.content()
                browser.close()
                return body
            else:
                response = page.goto(url, wait_until="networkidle", timeout=30000)
                if response is None:
                    browser.close()
                    return None
                body = response.text()
                browser.close()
                return body
        except Exception:
            try:
                browser.close()
            except Exception:
                pass
            return None


# ---------------------------------------------------------------------
# Score page parsing (ATP MatchStats)
# ---------------------------------------------------------------------

def _extract_player_team(team: Any) -> Dict[str, Any]:
    team = team or {}
    player = _safe(team, "Player") or {}

    first = _safe(team, "PlayerFirstName") or _safe(player, "PlayerFirstName") or _safe(player, "PlayerFirstNameFull")
    last = _safe(team, "PlayerLastName") or _safe(player, "PlayerLastName")
    name = None
    if first or last:
        name = (_norm_str(first) + " " + _norm_str(last)).strip()
    else:
        name = _safe(player, "PlayerFirstNameFull") or _safe(player, "PlayerName") or _safe(team, "PlayerName")

    pid = _safe(player, "PlayerId") or _safe(team, "PlayerId")
    country = (
        _safe(player, "PlayerCountry")
        or _safe(player, "PlayerCountryCode")
        or _safe(team, "PlayerCountryCode")
        or _safe(team, "PlayerCountry")
    )

    return {
        "name": _norm_str(name),
        "player_id": _norm_str(pid),
        "country": _norm_str(country),
        "sets": _safe(team, "Sets", default=[]) or [],
    }


def parse_atp_score_json_summary(j: Dict[str, Any], year=None, tournament_id=None, match_code=None, verbose=False):
    """
    Parse l'endpoint score. Retourne les champs finaux + quelques champs internes.
    Si le vainqueur n'est pas connu, on conserve quand même la paire A/B.
    """
    if j is None:
        return None
    if not isinstance(j, dict):
        return None

    tour = _safe(j, "Tournament") or {}
    match = _safe(j, "Match") or {}
    if not isinstance(match, dict):
        return None

    pt1 = _safe(match, "PlayerTeam1") or _safe(match, "PlayerTeam") or {}
    pt2 = _safe(match, "PlayerTeam2") or _safe(match, "OpponentTeam") or {}
    a = _extract_player_team(pt1)
    b = _extract_player_team(pt2)

    if not (a["name"] or a["player_id"] or a["country"]):
        return None
    if not (b["name"] or b["player_id"] or b["country"]):
        return None

    sets_a = {}
    sets_b = {}
    tb_a = {}
    tb_b = {}
    for s in a["sets"]:
        sn = _int_or_none(_safe(s, "SetNumber"))
        if sn is None:
            continue
        sets_a[sn] = _norm_str(_safe(s, "SetScore"))
        tb_a[sn] = _safe(s, "TieBreakScore")
    for s in b["sets"]:
        sn = _int_or_none(_safe(s, "SetNumber"))
        if sn is None:
            continue
        sets_b[sn] = _norm_str(_safe(s, "SetScore"))
        tb_b[sn] = _safe(s, "TieBreakScore")

    all_scores = {}
    for i in range(1, 6):
        all_scores[f"set{i}_score"] = _build_set_string(
            sets_a.get(i), sets_b.get(i), a_tb=tb_a.get(i), b_tb=tb_b.get(i)
        )

    score_string = _safe(match, "Message") or _safe(match, "ExtendedMessage") or _safe(match, "ScoreString")
    if not score_string:
        joined = ",".join([all_scores[f"set{i}_score"] for i in range(1, 6) if all_scores[f"set{i}_score"]])
        score_string = joined if joined else None
    score_string = _norm_str(score_string)

    winner_label = None
    winner_raw = _norm_str(_safe(match, "WinningPlayerId") or _safe(match, "Winner"))
    if winner_raw:
        if a["player_id"] and winner_raw == a["player_id"]:
            winner_label = "A"
        elif b["player_id"] and winner_raw == b["player_id"]:
            winner_label = "B"
        else:
            wr = winner_raw.lower()
            if a["player_id"] and a["player_id"].lower() in wr:
                winner_label = "A"
            elif b["player_id"] and b["player_id"].lower() in wr:
                winner_label = "B"

    if winner_label is None:
        a_wins = b_wins = 0
        for i in range(1, 6):
            s = all_scores.get(f"set{i}_score")
            if not s:
                continue
            m = re.match(r"^\s*(\d+)\s*-\s*(\d+)", s)
            if not m:
                continue
            a_v = int(m.group(1))
            b_v = int(m.group(2))
            if a_v > b_v:
                a_wins += 1
            elif b_v > a_v:
                b_wins += 1
        if a_wins > b_wins:
            winner_label = "A"
        elif b_wins > a_wins:
            winner_label = "B"

    if winner_label == "A":
        winner_name = a["name"]
        loser_name = b["name"]
        winner_id = a["player_id"]
        loser_id = b["player_id"]
        winner_country = a["country"]
        loser_country = b["country"]
    elif winner_label == "B":
        winner_name = b["name"]
        loser_name = a["name"]
        winner_id = b["player_id"]
        loser_id = a["player_id"]
        winner_country = b["country"]
        loser_country = a["country"]
    else:
        # Si le match n'est pas terminé, on garde la paire A/B.
        winner_name = a["name"]
        loser_name = b["name"]
        winner_id = a["player_id"]
        loser_id = b["player_id"]
        winner_country = a["country"]
        loser_country = b["country"]

    event_id = _safe(tour, "EventId", "EventID") or tournament_id
    event_year = _safe(tour, "EventYear", "Year") or year
    match_id = _safe(match, "MatchId", "MatchID") or match_code
    match_state = _norm_str(_safe(match, "MatchState", "Status", "MatchStatus"))

    return {
        "event_id": _norm_str(event_id),
        "event_year": _int_or_none(event_year) or year,
        "match_id": _norm_str(match_id),
        "winner_player_name": winner_name,
        "winner_player_id": winner_id,
        "loser_player_name": loser_name,
        "loser_player_id": loser_id,
        "winner_country": winner_country,
        "loser_wountry": loser_country,
        "score_string": score_string,
        "_match_state": match_state,
        "_score_like": _looks_like_actual_score(score_string),
        "_source": "score",
    }


# ---------------------------------------------------------------------
# BYE inference from score tree
# ---------------------------------------------------------------------

def _row_side_players(row: Dict[str, Any]) -> List[Dict[str, Any]]:
    return [
        {
            "name": row.get("winner_player_name"),
            "player_id": row.get("winner_player_id"),
            "country": row.get("winner_country"),
        },
        {
            "name": row.get("loser_player_name"),
            "player_id": row.get("loser_player_id"),
            "country": row.get("loser_wountry"),
        },
    ]


def _player_same(a: Dict[str, Any], b: Dict[str, Any]) -> bool:
    a_pid = _norm_str(a.get("player_id"))
    b_pid = _norm_str(b.get("player_id"))
    if a_pid and b_pid and a_pid.upper() == b_pid.upper():
        return True
    a_name = _normalize_key(a.get("name"))
    b_name = _normalize_key(b.get("name"))
    if a_name and b_name and a_name == b_name:
        return True
    return False


def _find_unique_child_player(child_row: Dict[str, Any], sibling_row: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    child_players = _row_side_players(child_row)
    sibling_players = _row_side_players(sibling_row)

    common_child = None
    for cp in child_players:
        if any(_player_same(cp, sp) for sp in sibling_players):
            common_child = cp
            break

    if common_child is None:
        return None

    unique_child = None
    for cp in child_players:
        if not any(_player_same(cp, sp) for sp in sibling_players):
            unique_child = cp
            break

    return unique_child


def infer_bye_rows_from_score_rows(
    rows: List[Dict[str, Any]],
    num_players: int,
    tournament_id: int,
    year: int,
    verbose: bool = False,
) -> List[Dict[str, Any]]:
    """
    Ajoute les matchs BYE du premier tour manquants à partir de l'arbre des matchs déjà récupérés.

    Méthode:
      - on repère les MS du premier tour absents
      - pour un MS manquant, on regarde son match enfant
      - on compare le match enfant au match frère présent
      - le joueur du match enfant absent du frère est le joueur qui a gagné le BYE
    """
    if not rows:
        return rows

    draw_size = _next_power_of_two(num_players)
    first_round_slots = draw_size // 2
    first_round_codes = [f"MS{n:03d}" for n in range(first_round_slots, draw_size)]

    rows_by_code = {}
    for r in rows:
        mid = _norm_str(r.get("match_id"))
        if mid:
            rows_by_code[mid] = r

    missing_first_round = [c for c in first_round_codes if c not in rows_by_code]
    if not missing_first_round:
        return rows

    inferred_rows = []

    for code in missing_first_round:
        try:
            num = int(code[2:])
        except Exception:
            continue

        # enfant = floor(num/2) avec la règle donnée
        child_num = num // 2 if num % 2 == 0 else (num - 1) // 2
        sibling_num = num + 1 if num % 2 == 0 else num - 1

        child_code = f"MS{child_num:03d}"
        sibling_code = f"MS{sibling_num:03d}"

        child_row = rows_by_code.get(child_code)
        sibling_row = rows_by_code.get(sibling_code)

        if not child_row or not sibling_row:
            if verbose:
                print(f"  [bye] impossible d'inférer {code}: child={child_code} ou sibling={sibling_code} absent")
            continue

        unique_child = _find_unique_child_player(child_row, sibling_row)
        if not unique_child:
            if verbose:
                print(f"  [bye] impossible d'inférer {code}: aucun joueur unique trouvé via {child_code} / {sibling_code}")
            continue

        inferred = {
            "event_id": tournament_id,
            "event_year": year,
            "match_id": code,
            "winner_player_name": unique_child.get("name"),
            "winner_player_id": unique_child.get("player_id"),
            "loser_player_name": "BYE",
            "loser_player_id": None,
            "winner_country": unique_child.get("country"),
            "loser_wountry": None,
            "score_string": None,
            "_source": "bye",
            "_match_state": "BYE",
            "_score_like": False,
        }
        inferred_rows.append(inferred)
        rows_by_code[code] = inferred

        if verbose:
            print(
                f"  [bye] {code}: "
                f"{unique_child.get('name')} ({unique_child.get('player_id')}) "
                f"vs BYE"
            )

    if inferred_rows and verbose:
        print(f"[bye] {len(inferred_rows)} match(s) BYE ajouté(s).")

    return rows + inferred_rows


# ---------------------------------------------------------------------
# Draw page parsing
# ---------------------------------------------------------------------

def _extract_player_id_from_draw_player_info(player_info) -> Optional[str]:
    if player_info is None:
        return None

    img = player_info.select_one("img.player-image")
    if img and img.get("src"):
        m = re.search(r"player-headshot/([^/?#]+)", img.get("src"), flags=re.I)
        if m:
            return m.group(1).upper()

    a = player_info.select_one(".name a[href]")
    if a and a.get("href"):
        href = a.get("href")
        m = re.search(r"/([A-Za-z0-9]{4})/overview", href)
        if m:
            return m.group(1).upper()
        parts = [p for p in href.split("/") if p]
        if len(parts) >= 2 and len(parts[-2]) in (4, 5):
            return parts[-2].upper()

    return None


def _extract_country_from_draw_player_info(player_info) -> Optional[str]:
    if player_info is None:
        return None
    use = player_info.select_one("svg.atp-flag use[href]")
    href = use.get("href") if use else None
    if href:
        m = re.search(r"#flag-([a-z0-9]+)", href, flags=re.I)
        if m:
            return m.group(1).upper()
    return None


def _extract_visible_player_name(player_info) -> Optional[str]:
    if player_info is None:
        return None
    name_div = player_info.select_one("div.name")
    if not name_div:
        return None
    txt = name_div.get_text(" ", strip=True)
    txt = re.sub(r"\s+\(.*?\)\s*$", "", txt).strip()
    return txt or None


def _parse_draw_select_players(soup: BeautifulSoup) -> List[Dict[str, Any]]:
    """
    Liste canonique des joueurs dans l'ordre du draw-select.
    """
    out: List[Dict[str, Any]] = []
    seen = set()
    for sel in soup.select("select[id^='player-draw-select']"):
        for opt in sel.find_all("option"):
            pid = _norm_str(opt.get("data-value"))
            if not pid:
                continue
            pid_u = pid.upper()
            if pid_u in seen:
                continue
            seen.add(pid_u)
            first = _norm_str(opt.get("data-first"))
            last = _norm_str(opt.get("data-last"))
            full_name = None
            if first or last:
                full_name = (_norm_str(first) + " " + _norm_str(last)).strip()
            else:
                full_name = opt.get_text(" ", strip=True)
            out.append(
                {
                    "player_id": pid_u,
                    "full_name": _norm_str(full_name),
                    "first_name": first,
                    "last_name": last,
                    "country_code": _norm_str(opt.get("data-country-code")) and opt.get("data-country-code").upper(),
                    "country_name": _norm_str(opt.get("data-country-name")),
                    "display_text": _norm_str(opt.get_text(" ", strip=True)),
                }
            )
    return out


def _parse_draw_player_slot(stats_item, select_map: Dict[str, Dict[str, Any]]) -> Dict[str, Any]:
    player_info = stats_item.select_one("div.player-info")
    if player_info is None:
        return {
            "name": None,
            "player_id": None,
            "country": None,
            "is_bye": False,
            "is_winner": False,
        }

    visible_name = _extract_visible_player_name(player_info)
    pid = _extract_player_id_from_draw_player_info(player_info)
    pid_u = pid.upper() if pid else None
    is_bye = bool(visible_name and visible_name.strip().lower() == "bye") or bool(visible_name is None and pid_u in (None, "0"))
    is_winner = player_info.select_one("div.winner") is not None

    if is_bye:
        return {
            "name": "BYE",
            "player_id": None,
            "country": None,
            "is_bye": True,
            "is_winner": is_winner,
        }

    canonical = select_map.get(pid_u or "") if pid_u else None
    if canonical:
        name = canonical.get("full_name") or visible_name
        country = canonical.get("country_code")
        player_id = canonical.get("player_id")
    else:
        name = visible_name
        country = _extract_country_from_draw_player_info(player_info)
        player_id = pid_u

    return {
        "name": _norm_str(name),
        "player_id": _norm_str(player_id),
        "country": _norm_str(country),
        "is_bye": False,
        "is_winner": is_winner,
    }


def _extract_draw_score_string(stats_item) -> Optional[str]:
    scores = []
    for score_item in stats_item.select("div.scores div.score-item"):
        txt = score_item.get_text(" ", strip=True)
        txt = txt.replace(" ", "")
        if txt and txt != "-":
            scores.append(txt)
    if scores:
        return ",".join(scores)
    return None


def parse_atp_draw_html_summary(html: str, tournament_id: int, year: int, num_players: int) -> List[Dict[str, Any]]:
    soup = BeautifulSoup(html, "html.parser")

    select_players = _parse_draw_select_players(soup)
    select_map = {p["player_id"].upper(): p for p in select_players if p.get("player_id")}

    draw_items = soup.select("div.draw-content > div.draw-item")
    draw_size = _next_power_of_two(num_players)
    first_round_slots = draw_size // 2
    draw_items = draw_items[:first_round_slots]

    if not draw_items:
        draw_items = soup.select("div.draw-item")[:first_round_slots]

    records: List[Dict[str, Any]] = []
    for idx, draw_item in enumerate(draw_items):
        stats_items = draw_item.select("div.draw-stats > div.stats-item")
        if len(stats_items) < 2:
            continue

        left = _parse_draw_player_slot(stats_items[0], select_map)
        right = _parse_draw_player_slot(stats_items[1], select_map)

        score_string = _extract_draw_score_string(draw_item)

        if left["is_bye"] and not right["is_bye"]:
            winner, loser = right, left
        elif right["is_bye"] and not left["is_bye"]:
            winner, loser = left, right
        elif left["is_winner"] and not right["is_winner"]:
            winner, loser = left, right
        elif right["is_winner"] and not left["is_winner"]:
            winner, loser = right, left
        else:
            winner, loser = left, right

        match_code_num = (draw_size // 2) + idx
        match_id = f"MS{match_code_num:03d}"

        records.append(
            {
                "event_id": tournament_id,
                "event_year": year,
                "match_id": match_id,
                "winner_player_name": winner.get("name"),
                "winner_player_id": winner.get("player_id"),
                "loser_player_name": loser.get("name"),
                "loser_player_id": loser.get("player_id"),
                "winner_country": winner.get("country"),
                "loser_wountry": loser.get("country"),
                "score_string": score_string,
                "_source": "draw",
                "_match_state": "draw",
                "_score_like": _looks_like_actual_score(score_string),
            }
        )

    return records


def _build_draw_url_candidates(slug: str, tournament_id: int) -> List[str]:
    slug = _slugify(slug) if slug else slug
    urls = []
    for tmpl in DRAW_URL_TEMPLATES:
        urls.append(tmpl.format(slug=slug, tournament_id=tournament_id))
    return urls


def fetch_draw_html_with_retry(
    slug: str,
    tournament_id: int,
    headless: bool = True,
    verbose: bool = False,
) -> Optional[str]:
    for url in _build_draw_url_candidates(slug, tournament_id):
        if verbose:
            print(f"    [draw] tentative URL: {url}")
        html = fetch_url_with_playwright(url, headless=headless, expect_html=True)
        if not html:
            continue
        if "player-draw-select" in html and "draw-content" in html:
            if verbose:
                print(f"    [draw] page valide: {url}")
            return html
    return None


def _load_or_fetch_draw_records(
    tournament_id: int,
    year: int,
    num_players: int,
    slug: str,
    out_folder: Path,
    headless: bool,
    verbose: bool,
) -> Optional[Dict[str, Any]]:
    debug_draw_dir = out_folder / "debug_draw"
    _ensure_dir(debug_draw_dir)
    cache_path = debug_draw_dir / f"draw_{tournament_id}_{year}.json"

    if cache_path.exists():
        try:
            cached = json.loads(cache_path.read_text(encoding="utf-8"))
            if isinstance(cached, dict) and isinstance(cached.get("matches"), list):
                if verbose:
                    print(f"  [draw-cache] reuse -> {cache_path}")
                return cached
        except Exception:
            pass

    html = fetch_draw_html_with_retry(slug, tournament_id, headless=headless, verbose=verbose)
    if not html:
        return None

    records = parse_atp_draw_html_summary(html, tournament_id=tournament_id, year=year, num_players=num_players)
    payload = {
        "event_id": tournament_id,
        "event_year": year,
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
            for r in records
        ],
        "_raw_records": records,
    }

    try:
        cache_path.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")
        if verbose:
            print(f"  [draw-cache] saved -> {cache_path}")
    except Exception as e:
        if verbose:
            print(f"  [warn] impossible d'écrire le cache draw: {e}")

    return payload


# ---------------------------------------------------------------------
# Match state helpers
# ---------------------------------------------------------------------

def _serialize_final_record(r: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "match_id": r.get("match_id"),
        "winner_player_name": r.get("winner_player_name"),
        "winner_player_id": r.get("winner_player_id"),
        "loser_player_name": r.get("loser_player_name"),
        "loser_player_id": r.get("loser_player_id"),
        "winner_country": r.get("winner_country"),
        "loser_wountry": r.get("loser_wountry"),
        "score_string": r.get("score_string"),
    }


def _is_score_json_valid(debug_dir: Path, tourn_id: int, year: int, match_code: str, verbose: bool = False):
    path = debug_dir / f"raw_{tourn_id}_{year}_{match_code}.json"
    if not path.exists():
        return False, None
    try:
        txt = path.read_text(encoding="utf-8")
        if not txt.strip():
            return False, None
        j = json.loads(txt)
    except Exception as e:
        if verbose:
            print(f"    [resume-check] impossible de parser {path} ({e}) -> re-fetch")
        return False, None
    if isinstance(j, dict) and ("error" in j or (j.get("url") and j.get("error"))):
        return False, None
    if isinstance(j, dict) and ("Tournament" in j or "Match" in j):
        return True, j
    if isinstance(j, dict) and len(j) > 0:
        return True, j
    return False, None


# ---------------------------------------------------------------------
# Orchestration
# ---------------------------------------------------------------------

def run_scrape_one_tournament(
    tournament_id: int,
    year: int,
    num_players: int,
    slug: str,
    out_folder="data_atp",
    headless=False,
    verbose=True,
    max_attempts_per_match=10,
    wait_between_retries=3.0,
    wait_between_rounds=5.0,
    match_source="auto",  # auto | score | draw
):
    out_folder = Path(out_folder)
    _ensure_dir(out_folder)
    debug_dir = out_folder / "debug_jsons"
    _ensure_dir(debug_dir)
    logs_dir = out_folder / "logs"
    _ensure_dir(logs_dir)

    draw_size = _next_power_of_two(num_players)
    first_round_slots = draw_size // 2
    
    if match_source == "draw":
        expected_rows = first_round_slots
        candidate_codes = [f"MS{n:03d}" for n in range(first_round_slots, draw_size)]
    else:
        expected_rows = draw_size - 1
        _, candidate_codes = _all_match_info(num_players)

    if verbose:
        print(f"\n=== TOURNOI {tournament_id} ({year}) ===")
        print(f"[config] num_players={num_players} -> draw_size={draw_size} -> first_round_slots={first_round_slots}")
        print(f"[config] match_source={match_source}")
        print(f"[config] match codes: {candidate_codes[0]}..{candidate_codes[-1] if candidate_codes else 'N/A'}")

    rows: List[Dict[str, Any]] = []
    collected_codes = set()
    per_match_attempts = {code: 0 for code in candidate_codes}
    total_fetches = 0

    draw_payload = None
    draw_map = None

    def get_draw_record(match_code: str) -> Optional[Dict[str, Any]]:
        nonlocal draw_payload, draw_map
        if draw_map is None:
            draw_payload = _load_or_fetch_draw_records(
                tournament_id=tournament_id,
                year=year,
                num_players=num_players,
                slug=slug,
                out_folder=out_folder,
                headless=headless,
                verbose=verbose,
            )
            if draw_payload and isinstance(draw_payload.get("_raw_records"), list):
                draw_map = {r.get("match_id"): r for r in draw_payload["_raw_records"] if r.get("match_id")}
            else:
                draw_map = {}
        return draw_map.get(match_code) if draw_map else None

    def _fetch_one_score_match(match_code: str, allow_draw_fallback: bool = False) -> Optional[Dict[str, Any]]:
        nonlocal total_fetches
        ok, j = _is_score_json_valid(debug_dir, tournament_id, year, match_code, verbose=verbose)
        if ok:
            parsed = parse_atp_score_json_summary(
                j,
                year=year,
                tournament_id=tournament_id,
                match_code=match_code,
                verbose=verbose,
            )
            if parsed:
                if allow_draw_fallback and (
                    parsed.get("_match_state") == "U"
                    or not parsed.get("_score_like")
                    or not parsed.get("score_string")
                ):
                    draw_rec = get_draw_record(match_code)
                    if draw_rec:
                        parsed = draw_rec
                return parsed

        per_match_attempts[match_code] += 1
        total_fetches += 1
        url = SCORE_URL_TEMPLATE.format(year=year, tournament_id=tournament_id, match_code=match_code)
        if verbose:
            print(
                f"  [phase1] fetch #{total_fetches} -> {match_code} "
                f"(attempt {per_match_attempts[match_code]}/{max_attempts_per_match})"
            )

        body = None
        try:
            body = fetch_url_with_playwright(url, headless=headless, expect_html=False)
        except Exception as e:
            if verbose:
                print(f"    [error] fetch score failed: {e}")

        raw_path = debug_dir / f"raw_{tournament_id}_{year}_{match_code}.json"
        try:
            if body is None:
                raw_path.write_text(
                    json.dumps({"error": "no json returned", "url": url}, ensure_ascii=False),
                    encoding="utf-8",
                )
            else:
                try:
                    parsed_json = json.loads(body)
                    raw_path.write_text(json.dumps(parsed_json, indent=2, ensure_ascii=False), encoding="utf-8")
                    body = parsed_json
                except Exception:
                    raw_path.write_text(
                        json.dumps({"error": "invalid json text", "url": url, "body": body[:1200]}, ensure_ascii=False),
                        encoding="utf-8",
                    )
                    body = None
        except Exception as e:
            if verbose:
                print(f"    [warn] impossible d'écrire le debug json: {e}")

        if body:
            parsed = parse_atp_score_json_summary(
                body,
                year=year,
                tournament_id=tournament_id,
                match_code=match_code,
                verbose=verbose,
            )
            if parsed:
                if allow_draw_fallback and (
                    parsed.get("_match_state") == "U"
                    or not parsed.get("_score_like")
                    or not parsed.get("score_string")
                ):
                    draw_rec = get_draw_record(match_code)
                    if draw_rec:
                        parsed = draw_rec
                return parsed

        return None

    # MODE DRAW: on se base entièrement sur la page draw
    if match_source == "draw":
        draw_payload = _load_or_fetch_draw_records(
            tournament_id=tournament_id,
            year=year,
            num_players=num_players,
            slug=slug,
            out_folder=out_folder,
            headless=headless,
            verbose=verbose,
        )
        if not draw_payload:
            if verbose:
                print("[proc] impossible de récupérer la page draw.")
            return None

        rows = draw_payload.get("_raw_records", []) or []
        rows = sorted(rows, key=lambda x: x.get("match_id") or "")
        out_data = {
            "event_id": tournament_id,
            "event_year": year,
            "matches": [_serialize_final_record(r) for r in rows],
        }

        outpath = out_folder / f"atp_{tournament_id}_{year}_temporary.json"
        outpath.write_text(json.dumps(out_data, indent=2, ensure_ascii=False), encoding="utf-8")
        if verbose:
            print(f"[proc] sauvegardé -> {outpath} ({len(rows)} matches, draw mode)")
        try:
            CREATED_FILES.append(os.path.relpath(outpath, start=Path.cwd()))
        except Exception:
            CREATED_FILES.append(str(outpath))
        return outpath

    # MODE SCORE ou AUTO: on récupère les matchs joués
    if match_source == "score":
        # On tente tous les matchs du tournoi, puis on réessaie les manquants.
        remaining_codes = candidate_codes[:]
        max_rounds = max(1, int(max_attempts_per_match))

        for round_idx in range(1, max_rounds + 1):
            if not remaining_codes:
                break

            if verbose:
                print(f"[score] tour {round_idx}/{max_rounds} — {len(remaining_codes)} matchs à tenter")

            succeeded_this_round = []

            for match_code in list(remaining_codes):
                parsed = _fetch_one_score_match(match_code, allow_draw_fallback=False)
                if parsed:
                    rows.append(parsed)
                    collected_codes.add(match_code)
                    succeeded_this_round.append(match_code)
                    if verbose:
                        print(f"    parsed OK -> collected {len(rows)} match(s)")
                else:
                    if verbose:
                        print(f"    échec -> {match_code} sera retenté")

                time.sleep(0.3)

            remaining_codes = [c for c in remaining_codes if c not in succeeded_this_round]

            if remaining_codes and round_idx < max_rounds:
                if verbose:
                    print(f"[score] fin du tour {round_idx}: {len(remaining_codes)} restant(s), pause {wait_between_rounds}s")
                time.sleep(wait_between_rounds)

    else:
        # MODE AUTO: score d'abord, fallback draw si besoin
        for match_code in candidate_codes:
            ok, j = _is_score_json_valid(debug_dir, tournament_id, year, match_code, verbose=verbose)
            if ok:
                parsed = parse_atp_score_json_summary(j, year=year, tournament_id=tournament_id, match_code=match_code, verbose=verbose)
                if parsed:
                    if parsed.get("_match_state") == "U" or not parsed.get("_score_like") or not parsed.get("score_string"):
                        draw_rec = get_draw_record(match_code)
                        if draw_rec:
                            parsed = draw_rec
                    rows.append(parsed)
                    collected_codes.add(match_code)
                    if verbose:
                        print(f"  [resume] reused {match_code} -> collected {len(rows)}")
                    continue

            parsed = _fetch_one_score_match(match_code, allow_draw_fallback=True)
            if parsed:
                rows.append(parsed)
                collected_codes.add(match_code)
                if verbose:
                    print(f"    parsed OK -> collected {len(rows)}")
            else:
                if verbose:
                    print("    aucun JSON obtenu pour ce match (voir debug).")

            time.sleep(0.3)

    # Ajout des BYE en mode score / auto à partir de l'arbre des matchs déjà obtenus
    if match_source in ("score", "auto"):
        before = len(rows)
        rows = infer_bye_rows_from_score_rows(
            rows=rows,
            num_players=num_players,
            tournament_id=tournament_id,
            year=year,
            verbose=verbose,
        )
        after = len(rows)
        if verbose and after != before:
            print(f"[bye] rows avant={before}, après={after}")

    # Sauvegarde finale
    rows_sorted = sorted(rows, key=lambda x: x.get("match_id") or "")
    out_data = {
        "event_id": tournament_id,
        "event_year": year,
        "matches": [_serialize_final_record(r) for r in rows_sorted],
    }

    outpath = out_folder / f"atp_{tournament_id}_{year}_temporary.json"
    outpath.write_text(json.dumps(out_data, indent=2, ensure_ascii=False), encoding="utf-8")

    if verbose:
        print(f"[proc] sauvegardé -> {outpath} ({len(rows_sorted)} matches)")

    try:
        CREATED_FILES.append(os.path.relpath(outpath, start=Path.cwd()))
    except Exception:
        CREATED_FILES.append(str(outpath))

    if len(rows_sorted) < expected_rows:
        missing_after = [c for c in candidate_codes if c not in collected_codes]
        log_data = {
            "tournament_id": tournament_id,
            "year": year,
            "expected_rows": expected_rows,
            "obtained_rows": len(rows_sorted),
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


# ---------------------------------------------------------------------
# Multi-tournois
# ---------------------------------------------------------------------

def run_scrape_multi_tournaments(
    tournaments: List[Dict[str, Any]],
    out_folder="data_atp",
    headless=False,
    verbose=True,
    max_attempts_per_match=10,
    wait_between_retries=3.0,
    wait_between_rounds=5.0,
    match_source="auto",
    pause_between_tournaments=2.0,
):
    if not isinstance(tournaments, list):
        raise ValueError("tournaments doit être une liste de specs.")

    if verbose:
        print(f"[multi] tournois: {[t['tournament_id'] for t in tournaments]}")

    for idx, t in enumerate(tournaments):
        if verbose:
            print(f"\n>>> Début scraping pour le tournoi {t['tournament_id']} ({idx + 1}/{len(tournaments)})")
        run_scrape_one_tournament(
            tournament_id=t["tournament_id"],
            year=t["year"],
            num_players=t["num_players"],
            slug=t["slug"],
            out_folder=out_folder,
            headless=headless,
            verbose=verbose,
            max_attempts_per_match=max_attempts_per_match,
            wait_between_retries=wait_between_retries,
            wait_between_rounds=wait_between_rounds,
            match_source=match_source,
        )
        if idx < len(tournaments) - 1:
            time.sleep(pause_between_tournaments)


# ---------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------

def _build_arg_parser():
    p = argparse.ArgumentParser(description="Scraper ATP -> JSON résumé par tournoi.")

    p.add_argument(
        "--tournament",
        action="append",
        default=[],
        help="Spec tournoi: TOURNAMENT_ID:YEAR:NUM_PLAYERS:SLUG. Peut être répété.",
    )

    p.add_argument(
        "--match-source",
        choices=["auto", "score", "draw"],
        default="auto",
        help="score = endpoint MatchStats, draw = page draws, auto = score puis draw si incomplet",
    )

    p.add_argument("--out-folder", "--out-dir", dest="out_folder", type=str, default="data_atp", help="Dossier de sortie.")
    p.add_argument("--headless", action="store_true", help="Mode headless.")
    p.add_argument("--max-attempts", type=int, default=10, help="Max tentatives par match (mode score).")
    p.add_argument("--wait-between-retries", type=float, default=3.0)
    p.add_argument("--wait-between-rounds", type=float, default=5.0)
    p.add_argument("--pause-between-tournaments", type=float, default=2.0)
    p.add_argument("--created-files-out", type=str, default="created_files.txt", help="Fichier liste des sorties créées.")
    p.add_argument("--verbose", "-v", action="store_true", help="Verbose")
    return p


def main(argv=None):
    parser = _build_arg_parser()
    args = parser.parse_args(argv)

    if not args.tournament:
        print(
            "Aucun tournoi fourni. Exemple: --tournament 902:2026:30:miami-open-presented-by-itau",
            file=sys.stderr,
        )
        sys.exit(2)

    tournaments = []
    for spec in args.tournament:
        try:
            tournaments.append(_parse_cli_tournament_spec(spec))
        except Exception as e:
            print(f"Spec invalide {spec!r}: {e}", file=sys.stderr)
            sys.exit(2)

    run_scrape_multi_tournaments(
        tournaments=tournaments,
        out_folder=args.out_folder,
        headless=args.headless,
        verbose=args.verbose,
        max_attempts_per_match=args.max_attempts,
        wait_between_retries=args.wait_between_retries,
        wait_between_rounds=args.wait_between_rounds,
        match_source=args.match_source,
        pause_between_tournaments=args.pause_between_tournaments,
    )

    if CREATED_FILES:
        out_cf = args.created_files_out or "created_files.txt"
        try:
            with open(out_cf, "w", encoding="utf-8") as fh:
                for pth in CREATED_FILES:
                    fh.write(pth + "\n")
            if args.verbose:
                print(f"[main] wrote created files list -> {out_cf} ({len(CREATED_FILES)} entries)")
        except Exception as e:
            print(f"[warn] could not write created files list to {out_cf}: {e}", file=sys.stderr)


if __name__ == "__main__":
    main()