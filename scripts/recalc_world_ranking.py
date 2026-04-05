
#!/usr/bin/env python3
"""
Recalculate annual ranking points and world rank from `user_performances_this_year`.

What this script does:
1) Reads all rows from the ranking table (default: `bracket`).
2) Parses each user's `user_performances_this_year` JSON history.
3) Keeps only the last 52 weeks.
4) Scores each performance using the official level/draw-size table.
5) For each user:
   - always keeps Grand Slams / GS and 1000 / WTA 1000
   - adds the 12 highest scoring remaining performances
   - writes the total to `user_rank_points`
6) Rebuilds `user_world_rank` independently for ATP and WTA.
7) Updates the table in Supabase.

Notes:
- By default, this updates the `bracket` table because that is where
  `user_performances_this_year`, `user_rank_points`, and `user_world_rank`
  were described.
- If you want to target another table later, set `RANKING_TABLE`.
- The point tables below follow the values you provided.
- The script uses the tournament metadata files in:
    docs/atp_tournaments_2026
    docs/wta_tournaments_2026
  to reconstruct levels and draw sizes when needed.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import sys
from collections import defaultdict
from copy import deepcopy
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

SUPABASE_URL = os.environ.get("SUPABASE_URL", "").rstrip("/")
SUPABASE_KEY = os.environ.get("SUPABASE_SERVICE_ROLE_KEY", "")

RANKING_TABLE = os.environ.get("RANKING_TABLE", os.environ.get("BRACKET_TABLE", "bracket"))
ATP_TOURNAMENTS_PATH = os.environ.get("ATP_TOURNAMENTS_PATH", "docs/atp_tournaments_2026")
WTA_TOURNAMENTS_PATH = os.environ.get("WTA_TOURNAMENTS_PATH", "docs/wta_tournaments_2026")

HISTORY_WEEKS = int(os.environ.get("RANKING_HISTORY_WEEKS", "52"))
TOP_NON_MAJOR_PERFORMANCES = int(os.environ.get("TOP_NON_MAJOR_PERFORMANCES", "12"))

MONTHS = {
    "january": 1, "february": 2, "march": 3, "april": 4, "may": 5, "june": 6,
    "july": 7, "august": 8, "september": 9, "october": 10, "november": 11, "december": 12,
}

JSON_HEADERS = {
    "apikey": SUPABASE_KEY,
    "Authorization": f"Bearer {SUPABASE_KEY}",
    "Accept": "application/json",
}

# ---------------------------------------------------------------------
# Point tables
# ---------------------------------------------------------------------

POINT_TABLES: Dict[str, Dict[str, Dict[str, int]]] = {
    "ATP": {
        "Grand Slam (ATP)": {
            "W": 2000, "F": 1300, "SF": 800, "QF": 400, "R16": 200, "R32": 100, "R64": 50, "R128": 10
        },
        "ATP Finals": {
            "W_max": 1500, "F_max": 1000, "RR_win": 200, "RR_max": 600, "SF_win": 400, "W_bonus": 900
        },
        "ATP 1000 (96)": {
            "W": 1000, "F": 650, "SF": 400, "QF": 200, "R16": 100, "R32": 50, "R64": 30, "R128": 10
        },
        "ATP 1000 (56)": {
            "W": 1000, "F": 650, "SF": 400, "QF": 200, "R16": 100, "R32": 50, "R64": 30, "R128": 0
        },
        "ATP 500 (48)": {
            "W": 500, "F": 330, "SF": 200, "QF": 100, "R16": 50, "R32": 25
        },
        "ATP 500 (32)": {
            "W": 500, "F": 330, "SF": 200, "QF": 100, "R16": 50, "R32": 0
        },
        "ATP 250 (48)": {
            "W": 250, "F": 165, "SF": 100, "QF": 50, "R16": 25, "R32": 13
        },
        "ATP 250 (32)": {
            "W": 250, "F": 165, "SF": 100, "QF": 50, "R16": 25, "R32": 0
        },
        "Challenger 175": {
            "W": 175, "F": 90, "SF": 50, "QF": 25, "R16": 13, "R32": 0
        },
        "Challenger 125": {
            "W": 125, "F": 64, "SF": 35, "QF": 16, "R16": 8, "R32": 0
        },
        "Challenger 100": {
            "W": 100, "F": 50, "SF": 25, "QF": 14, "R16": 7, "R32": 0
        },
        "Challenger 75": {
            "W": 75, "F": 44, "SF": 22, "QF": 12, "R16": 6, "R32": 0
        },
        "Challenger 50": {
            "W": 50, "F": 25, "SF": 14, "QF": 8, "R16": 4, "R32": 0
        },
        "Future M25": {
            "W": 25, "F": 16, "SF": 8, "QF": 3, "R16": 1, "R32": 0
        },
        "Future M15": {
            "W": 15, "F": 8, "SF": 4, "QF": 2, "R16": 1, "R32": 0
        },
    },
    "WTA": {
        "Grand Slam (WTA)": {
            "W": 2000, "F": 1300, "SF": 780, "QF": 430, "R16": 240, "R32": 130, "R64": 70, "R128": 10
        },
        "WTA Finals": {
            "W_max": 1500, "F_max": 1000, "SF_max": 600, "RR_win": 200
        },
        "WTA 1000 (96)": {
            "W": 1000, "F": 650, "SF": 390, "QF": 215, "R16": 120, "R32": 65, "R64": 35, "R128": 10
        },
        "WTA 1000 (56)": {
            "W": 1000, "F": 650, "SF": 390, "QF": 215, "R16": 120, "R32": 65, "R64": 10
        },
        "WTA 500 (48)": {
            "W": 500, "F": 325, "SF": 195, "QF": 108, "R16": 60, "R32": 32, "R64": 1
        },
        "WTA 500 (30)": {
            "W": 500, "F": 325, "SF": 195, "QF": 108, "R16": 60, "R32": 1
        },
        "WTA 500 (28)": {
            "W": 500, "F": 325, "SF": 195, "QF": 108, "R16": 60, "R32": 1
        },
        "WTA 250 (32)": {
            "W": 250, "F": 163, "SF": 98, "QF": 54, "R16": 30, "R32": 1
        },
        "WTA 125 (32)": {
            "W": 125, "F": 81, "SF": 49, "QF": 27, "R16": 15, "R32": 1
        },
        "W100 (48)": {
            "W": 100, "F": 65, "SF": 39, "QF": 21, "R16": 12, "R32": 7, "R64": 1
        },
        "W100 (32)": {
            "W": 100, "F": 65, "SF": 39, "QF": 21, "R16": 12, "R32": 1
        },
        "W75 (48)": {
            "W": 75, "F": 49, "SF": 29, "QF": 16, "R16": 9, "R32": 5, "R64": 1
        },
        "W75 (32)": {
            "W": 75, "F": 49, "SF": 29, "QF": 16, "R16": 9, "R32": 1
        },
        "W50 (48)": {
            "W": 50, "F": 33, "SF": 20, "QF": 11, "R16": 6, "R32": 3, "R64": 1
        },
        "W50 (32)": {
            "W": 50, "F": 33, "SF": 20, "QF": 11, "R16": 6, "R32": 1
        },
        "W35 (48)": {
            "W": 35, "F": 23, "SF": 14, "QF": 8, "R16": 4, "R32": 2, "R64": 1
        },
        "W35 (32)": {
            "W": 35, "F": 23, "SF": 14, "QF": 8, "R16": 4, "R32": 1
        },
        "W15 (32)": {
            "W": 15, "F": 10, "SF": 6, "QF": 3, "R16": 1
        },
    }
}

# ---------------------------------------------------------------------
# Generic helpers
# ---------------------------------------------------------------------

def now_utc_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def safe_str(value: Any) -> str:
    return "" if value is None else str(value)


def clean_key(value: Any) -> str:
    return safe_str(value).strip()


def read_json_file(path: Path) -> Any:
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)


def read_json_file_flexible(path_like: str) -> Optional[Any]:
    p = Path(path_like)
    candidates = [p]
    if p.suffix.lower() != ".json":
        candidates.append(Path(str(p) + ".json"))
    for cand in candidates:
        if cand.exists():
            return read_json_file(cand)
    return None


def parse_json_text_maybe(value: Any) -> Any:
    if value is None:
        return None
    if isinstance(value, (dict, list)):
        return value
    if not isinstance(value, str):
        return None
    raw = value.strip()
    if not raw:
        return None
    try:
        return json.loads(raw)
    except Exception:
        return None


def parse_history(value: Any) -> List[dict]:
    parsed = parse_json_text_maybe(value)
    if isinstance(parsed, list):
        return [x for x in parsed if isinstance(x, dict)]
    if isinstance(parsed, dict):
        return [parsed]
    return []


def dump_text_json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"))


def month_name_to_num(name: str) -> Optional[int]:
    return MONTHS.get(safe_str(name).lower())


def try_parse_date(value: Any) -> Optional[date]:
    if value is None:
        return None
    if isinstance(value, date) and not isinstance(value, datetime):
        return value
    if isinstance(value, datetime):
        return value.date()
    raw = safe_str(value).strip()
    if not raw:
        return None
    try:
        return date.fromisoformat(raw[:10])
    except Exception:
        return None


def normalize_tour(value: Any) -> str:
    tour = safe_str(value).upper().strip()
    if tour in {"ATP", "WTA"}:
        return tour
    if "ATP" in tour:
        return "ATP"
    if "WTA" in tour:
        return "WTA"
    return tour or "UNKNOWN"


def get_match_prefix(match_id: Any) -> str:
    m = re.match(r"^(MS|LS)", safe_str(match_id), re.I)
    return m.group(1).upper() if m else "MS"


def get_match_num(match_id: Any) -> int:
    m = re.match(r"^(?:MS|LS)(\d+)$", safe_str(match_id), re.I)
    return int(m.group(1)) if m else 10**9


def next_power_of_two(n: int) -> int:
    p = 1
    while p < max(1, n):
        p *= 2
    return p


def round_label_from_place(rank: int, total_players: int) -> str:
    """
    Tennis-like placement labels:
      1 -> Winner
      2 -> Finalist
      3-4 -> SF
      5-8 -> QF
      9-16 -> R16
      17-32 -> R32
      ...
    """
    if rank <= 1:
        return "Winner"
    if rank == 2:
        return "Finalist"
    if rank <= 4:
        return "SF"
    if rank <= 8:
        return "QF"

    boundary = 16
    limit = next_power_of_two(total_players)
    while boundary < limit:
        if rank <= boundary:
            return f"R{boundary}"
        boundary *= 2
    return f"R{boundary}"


def normalize_performance_label(value: Any, rank: Any = None) -> Optional[str]:
    """
    Normalize a stored performance label to one of:
      W, F, SF, QF, R16, R32, R64, R128
    Also accepts 'Winner', 'Finalist', etc.
    """
    s = safe_str(value).strip().upper()
    if not s and rank is not None:
        try:
            r = int(rank)
            return round_label_from_place(r, max(r, 32)).upper()
        except Exception:
            return None

    s = s.replace("SEMIFINALIST", "SF").replace("QUARTERFINALIST", "QF")
    if s in {"W", "WINNER"}:
        return "W"
    if s in {"F", "FINALIST"}:
        return "F"
    if s in {"SF", "SEMI", "SEMIFINAL", "SEMIFINALS"}:
        return "SF"
    if s in {"QF", "QUARTERFINAL", "QUARTERFINALS"}:
        return "QF"

    m = re.match(r"^R\s*(\d+)$", s)
    if m:
        return f"R{int(m.group(1))}"

    if rank is not None:
        try:
            r = int(rank)
            return round_label_from_place(r, max(r, 32)).upper()
        except Exception:
            pass

    return None


def stage_group(label: Optional[str]) -> str:
    """
    Returns:
      - MAJOR for GS / 1000
      - OTHER for everything else
    """
    if not label:
        return "OTHER"
    return "MAJOR" if label in {"GS", "GRAND SLAM", "GRAND SLAM (ATP)", "GRAND SLAM (WTA)", "1000", "WTA 1000"} else "OTHER"


# ---------------------------------------------------------------------
# Supabase REST helpers
# ---------------------------------------------------------------------

def require_supabase() -> None:
    if not SUPABASE_URL or not SUPABASE_KEY:
        raise RuntimeError("Missing SUPABASE_URL or SUPABASE_SERVICE_ROLE_KEY")


def supabase_request(method: str, table: str, query: Optional[Dict[str, Any]] = None, payload: Any = None) -> Any:
    require_supabase()
    query = query or {}

    url = f"{SUPABASE_URL}/rest/v1/{table}"
    if query:
        url += "?" + urlencode([(k, v) for k, v in query.items() if v is not None and v != ""])

    headers = dict(JSON_HEADERS)
    body = None
    if method.upper() != "GET":
        headers["Content-Type"] = "application/json"
        headers["Prefer"] = "return=representation"
        if payload is not None:
            body = json.dumps(payload).encode("utf-8")

    req = Request(url, data=body, headers=headers, method=method.upper())

    try:
        with urlopen(req, timeout=120) as resp:
            raw = resp.read().decode("utf-8")
            if not raw:
                return None
            try:
                return json.loads(raw)
            except Exception:
                return raw
    except HTTPError as e:
        raw = e.read().decode("utf-8", errors="replace")
        raise RuntimeError(f"Supabase {method} {table} failed: {e.code} {raw}") from e
    except URLError as e:
        raise RuntimeError(f"Network error while calling Supabase: {e}") from e


def supabase_select_all(table: str, query: Optional[Dict[str, Any]] = None, page_size: int = 1000) -> List[dict]:
    rows: List[dict] = []
    offset = 0
    while True:
        q = dict(query or {})
        q["limit"] = page_size
        q["offset"] = offset
        chunk = supabase_request("GET", table, q)
        if not chunk:
            break
        if not isinstance(chunk, list):
            raise RuntimeError(f"Expected list from {table}, got {type(chunk).__name__}")
        rows.extend(chunk)
        if len(chunk) < page_size:
            break
        offset += page_size
    return rows


def supabase_select_one(table: str, query: Dict[str, Any]) -> Optional[dict]:
    rows = supabase_request("GET", table, dict(query, limit=1))
    if isinstance(rows, list) and rows:
        return rows[0]
    return None


def supabase_patch(table: str, query: Dict[str, Any], payload: dict) -> Any:
    return supabase_request("PATCH", table, query, payload)


# ---------------------------------------------------------------------
# Tournament metadata lookup
# ---------------------------------------------------------------------

def normalize_atp_level(raw: dict) -> str:
    pieces = [
        safe_str(raw.get("Type")),
        safe_str(raw.get("EventType")),
        safe_str(raw.get("ChallengerCategory")),
        safe_str(raw.get("Name")),
        safe_str(raw.get("Location")),
    ]
    text = " ".join(pieces).upper()

    if "GRAND SLAM" in text or re.search(r"\bGS\b", text):
        return "GS"

    for lvl in ("1000", "500", "250"):
        if re.search(rf"(?<!\d){lvl}(?!\d)", text):
            return lvl

    m = re.search(r"\bCH\s*([0-9]{2,3})\b", text)
    if m:
        return f"Challenger {m.group(1)}"
    m = re.search(r"\bCHALLENGER\s*([0-9]{2,3})\b", text)
    if m:
        return f"Challenger {m.group(1)}"
    if "CHALLENGER" in text:
        m = re.search(r"\b(CH(?:ALLENGER)?\s*[0-9]{2,3})\b", text)
        if m:
            num = re.search(r"([0-9]{2,3})", m.group(1))
            if num:
                return f"Challenger {num.group(1)}"
        return "Challenger"

    m = re.search(r"\bM\s*(15|25)\b", text)
    if m:
        return f"Future M{m.group(1)}"

    if "FUTURE" in text or safe_str(raw.get("Type")).upper() == "FU":
        m = re.search(r"\bM\s*(15|25)\b", text)
        if m:
            return f"Future M{m.group(1)}"
        return "Future"

    if safe_str(raw.get("Type")).strip():
        return safe_str(raw.get("Type")).strip()
    if safe_str(raw.get("EventType")).strip():
        return safe_str(raw.get("EventType")).strip()
    return "ATP"


def normalize_wta_level(raw: dict) -> str:
    pieces = [
        safe_str(raw.get("title")),
        safe_str(raw.get("level")),
        safe_str(raw.get("name")),
        safe_str(raw.get("tournamentGroup", {}).get("level") if isinstance(raw.get("tournamentGroup"), dict) else None),
    ]
    text = " ".join(pieces).upper()

    if "GRAND SLAM" in text or re.search(r"\bGS\b", text):
        return "Grand Slam"
    for lvl in ("1000", "500", "250", "125"):
        if re.search(rf"\bWTA\s*{lvl}\b", text):
            return f"WTA {lvl}"

    if re.search(r"\bWTA\s*124\b", text):
        return "WTA 125"

    for code in ("W100", "W75", "W50", "W35", "W25", "W15"):
        if re.search(rf"\b{code}\b", text):
            return code

    lvl = safe_str(raw.get("level"))
    if lvl:
        if lvl.upper() == "ITF":
            m = re.search(r"\bW(100|75|50|35|25|15)\b", text)
            if m:
                return f"W{m.group(1)}"
            m = re.search(r"\bWTA\s*(125|124)\b", text)
            if m:
                return "WTA 125"
            return "ITF"
        return lvl.strip()

    return "WTA"


def parse_atp_formatted_date(formatted: str) -> Optional[date]:
    if not formatted:
        return None
    s = formatted.strip()

    m = re.match(r"^(\d{1,2})\s+([A-Za-z]+)\s*-\s*(\d{1,2})\s+([A-Za-z]+),\s*(\d{4})$", s)
    if m:
        d1, mon1, _, _, y = m.groups()
        month = MONTHS.get(mon1.lower())
        if month:
            return date(int(y), month, int(d1))

    m = re.match(r"^(\d{1,2})\s*-\s*(\d{1,2})\s+([A-Za-z]+),\s*(\d{4})$", s)
    if m:
        d1, _, mon, y = m.groups()
        month = MONTHS.get(mon.lower())
        if month:
            return date(int(y), month, int(d1))

    m = re.match(r"^(\d{1,2})\s+([A-Za-z]+),\s*(\d{4})$", s)
    if m:
        d1, mon, y = m.groups()
        month = MONTHS.get(mon.lower())
        if month:
            return date(int(y), month, int(d1))

    return None


def tournament_date_for_history(meta: dict) -> str:
    if meta.get("start_date"):
        return safe_str(meta["start_date"])
    if meta.get("end_date"):
        return safe_str(meta["end_date"])
    if meta.get("formatted_date") and meta.get("tour") == "ATP":
        parsed = parse_atp_formatted_date(safe_str(meta["formatted_date"]))
        if parsed:
            return parsed.isoformat()
    if meta.get("year"):
        return f"{meta['year']}-01-01"
    return date.today().isoformat()


def load_atp_metadata_index(path_like: str) -> Dict[str, dict]:
    data = read_json_file_flexible(path_like) or {}
    index: Dict[str, dict] = {}

    dates = data.get("TournamentDates") if isinstance(data, dict) else []
    if not isinstance(dates, list):
        return index

    for block in dates:
        if not isinstance(block, dict):
            continue
        for raw in block.get("Tournaments", []) or []:
            if not isinstance(raw, dict):
                continue
            tid = clean_key(raw.get("Id"))
            if not tid:
                continue
            meta = {
                "tour": "ATP",
                "id": tid,
                "name": clean_key(raw.get("Name")) or f"ATP {tid}",
                "level": normalize_atp_level(raw),
                "year": 2026,
                "start_date": None,
                "end_date": None,
                "formatted_date": clean_key(raw.get("FormattedDate")),
                "surface": clean_key(raw.get("Surface")),
                "country": clean_key(raw.get("Location")),
                "draw_size": int(raw.get("SglDrawSize") or 0) or None,
                "raw": raw,
            }
            index[tid] = meta
    return index


def load_wta_metadata_index(path_like: str) -> Dict[str, dict]:
    data = read_json_file_flexible(path_like) or {}
    index: Dict[str, dict] = {}

    content = data.get("content") if isinstance(data, dict) else []
    if not isinstance(content, list):
        return index

    for raw in content:
        if not isinstance(raw, dict):
            continue
        tg = raw.get("tournamentGroup")
        tid = clean_key(tg.get("id") if isinstance(tg, dict) else None)
        if not tid:
            continue
        meta = {
            "tour": "WTA",
            "id": tid,
            "name": clean_key(raw.get("title")) or clean_key(tg.get("name") if isinstance(tg, dict) else None) or f"WTA {tid}",
            "level": normalize_wta_level(raw),
            "year": int(raw.get("year") or 2026),
            "start_date": clean_key(raw.get("startDate")),
            "end_date": clean_key(raw.get("endDate")),
            "formatted_date": clean_key(raw.get("startDate")),
            "surface": clean_key(raw.get("surface")),
            "country": clean_key(raw.get("country")),
            "draw_size": int(raw.get("singlesDrawSize") or 0) or None,
            "raw": raw,
        }
        index[tid] = meta
    return index


def build_tournament_index() -> Dict[str, dict]:
    index: Dict[str, dict] = {}
    index.update(load_atp_metadata_index(ATP_TOURNAMENTS_PATH))
    index.update(load_wta_metadata_index(WTA_TOURNAMENTS_PATH))
    return index


# ---------------------------------------------------------------------
# Level / points resolution
# ---------------------------------------------------------------------

def infer_tour(entry: dict, meta: Optional[dict]) -> str:
    if meta and meta.get("tour"):
        return normalize_tour(meta.get("tour"))
    for key in ("tour", "user_tour", "tour_type"):
        if entry.get(key):
            return normalize_tour(entry.get(key))
    level = safe_str(entry.get("level")).upper()
    if "WTA" in level or level.startswith("W"):
        return "WTA"
    if "ATP" in level or "CH" in level or "FU" in level or "GS" in level:
        return "ATP"
    name = safe_str(entry.get("tournament_name") or entry.get("name")).upper()
    if any(x in name for x in ("WTA", "W100", "W75", "W50", "W35", "W25", "W15")):
        return "WTA"
    return "ATP"


def parse_draw_size_from_text(text: str) -> Optional[int]:
    m = re.search(r"\((\d+)\)", safe_str(text))
    if m:
        return int(m.group(1))
    return None


def resolve_level_label(entry: dict, meta: Optional[dict], tour: str) -> str:
    """
    Returns a normalized level label compatible with POINT_TABLES.
    """
    raw_level = safe_str(entry.get("level") or (meta or {}).get("level")).strip()
    name = safe_str(entry.get("tournament_name") or (meta or {}).get("name")).strip()
    combined = f"{raw_level} {name}".upper()

    if tour == "ATP":
        if "GRAND SLAM" in combined or raw_level.upper() == "GS":
            return "Grand Slam (ATP)"
        if "ATP FINALS" in combined:
            return "ATP Finals"

        if re.search(r"\b1000\b", combined):
            draw = (meta or {}).get("draw_size") or parse_draw_size_from_text(combined)
            return f"ATP 1000 ({draw or 96 if '96' in combined else 56})"
        if re.search(r"\b500\b", combined):
            draw = (meta or {}).get("draw_size") or parse_draw_size_from_text(combined)
            if draw in (48, 32):
                return f"ATP 500 ({draw})"
            if "32" in combined:
                return "ATP 500 (32)"
            return "ATP 500 (48)"
        if re.search(r"\b250\b", combined):
            draw = (meta or {}).get("draw_size") or parse_draw_size_from_text(combined)
            if draw in (48, 32):
                return f"ATP 250 ({draw})"
            if "32" in combined:
                return "ATP 250 (32)"
            return "ATP 250 (48)"

        m = re.search(r"\bCH(?:ALLENGER)?\s*([0-9]{2,3})\b", combined)
        if m:
            return f"Challenger {m.group(1)}"
        if raw_level.upper().startswith("CH") or "CHALLENGER" in combined:
            m = re.search(r"\b(175|125|100|75|50)\b", combined)
            if m:
                return f"Challenger {m.group(1)}"
            return "Challenger"

        m = re.search(r"\bM\s*(25|15)\b", combined)
        if m:
            return f"Future M{m.group(1)}"
        if raw_level.upper().startswith("FU") or "FUTURE" in combined:
            m = re.search(r"\bM\s*(25|15)\b", combined)
            if m:
                return f"Future M{m.group(1)}"
            return "Future"

        if raw_level in POINT_TABLES["ATP"]:
            return raw_level
        return raw_level or "ATP"

    # WTA
    if "GRAND SLAM" in combined or raw_level.upper() in {"GS", "GRAND SLAM"}:
        return "Grand Slam (WTA)"
    if "WTA FINALS" in combined:
        return "WTA Finals"

    if re.search(r"\bWTA\s*1000\b", combined):
        draw = (meta or {}).get("draw_size") or parse_draw_size_from_text(combined)
        if draw in (96, 56):
            return f"WTA 1000 ({draw})"
        return "WTA 1000 (96)" if "96" in combined else "WTA 1000 (56)"

    if re.search(r"\bWTA\s*500\b", combined):
        draw = (meta or {}).get("draw_size") or parse_draw_size_from_text(combined)
        if draw in (48, 30, 28):
            return f"WTA 500 ({draw})"
        if "48" in combined:
            return "WTA 500 (48)"
        if "30" in combined:
            return "WTA 500 (30)"
        if "28" in combined:
            return "WTA 500 (28)"
        return "WTA 500 (48)"

    if re.search(r"\bWTA\s*250\b", combined):
        return "WTA 250 (32)"

    if re.search(r"\bWTA\s*124\b", combined) or re.search(r"\bWTA\s*125\b", combined):
        return "WTA 125 (32)"

    for code in ("W100", "W75", "W50", "W35", "W25", "W15"):
        if re.search(rf"\b{code}\b", combined):
            draw = (meta or {}).get("draw_size") or parse_draw_size_from_text(combined)
            if draw in (48, 32):
                return f"{code} ({draw})"
            if code == "W15":
                return "W15 (32)"
            if "48" in combined:
                return f"{code} (48)"
            if "32" in combined:
                return f"{code} (32)"
            return f"{code} (48)" if code != "W15" else "W15 (32)"

    if raw_level.upper() == "ITF":
        m = re.search(r"\b(W100|W75|W50|W35|W25|W15)\b", combined)
        if m:
            code = m.group(1)
            draw = (meta or {}).get("draw_size") or parse_draw_size_from_text(combined)
            if code == "W15":
                return "W15 (32)"
            if draw in (48, 32):
                return f"{code} ({draw})"
            return f"{code} (32)" if code == "W15" else f"{code} (48)"
        return "ITF"

    if raw_level in POINT_TABLES["WTA"]:
        return raw_level
    return raw_level or "WTA"


def resolve_points_key(entry: dict, meta: Optional[dict]) -> Tuple[str, str]:
    """
    Returns (tour, points_table_key)
    """
    tour = infer_tour(entry, meta)
    level_key = resolve_level_label(entry, meta, tour)
    table = POINT_TABLES.get(tour, {})

    if level_key in table:
        return tour, level_key

    # Fallbacks for missing draw-size labels.
    if tour == "ATP":
        if level_key == "Grand Slam (ATP)":
            return tour, "Grand Slam (ATP)"
        if level_key.startswith("ATP 1000"):
            return tour, "ATP 1000 (96)" if "96" in level_key else "ATP 1000 (56)"
        if level_key.startswith("ATP 500"):
            if "32" in level_key:
                return tour, "ATP 500 (32)"
            return tour, "ATP 500 (48)"
        if level_key.startswith("ATP 250"):
            if "32" in level_key:
                return tour, "ATP 250 (32)"
            return tour, "ATP 250 (48)"
        if level_key.startswith("Challenger "):
            num = re.search(r"(\d{2,3})", level_key)
            if num:
                exact = f"Challenger {num.group(1)}"
                if exact in table:
                    return tour, exact
        if level_key.startswith("Future M"):
            num = re.search(r"M(15|25)", level_key)
            if num:
                exact = f"Future M{num.group(1)}"
                if exact in table:
                    return tour, exact

    if tour == "WTA":
        if level_key == "Grand Slam (WTA)":
            return tour, "Grand Slam (WTA)"
        if level_key == "WTA Finals":
            return tour, "WTA Finals"
        if level_key.startswith("WTA 1000"):
            if "56" in level_key:
                return tour, "WTA 1000 (56)"
            return tour, "WTA 1000 (96)"
        if level_key.startswith("WTA 500"):
            if "30" in level_key:
                return tour, "WTA 500 (30)"
            if "28" in level_key:
                return tour, "WTA 500 (28)"
            return tour, "WTA 500 (48)"
        if level_key.startswith("WTA 250"):
            return tour, "WTA 250 (32)"
        if level_key.startswith("WTA 125"):
            return tour, "WTA 125 (32)"
        if level_key.startswith("W100"):
            if "32" in level_key:
                return tour, "W100 (32)"
            return tour, "W100 (48)"
        if level_key.startswith("W75"):
            if "32" in level_key:
                return tour, "W75 (32)"
            return tour, "W75 (48)"
        if level_key.startswith("W50"):
            if "32" in level_key:
                return tour, "W50 (32)"
            return tour, "W50 (48)"
        if level_key.startswith("W35"):
            if "32" in level_key:
                return tour, "W35 (32)"
            return tour, "W35 (48)"
        if level_key.startswith("W15"):
            return tour, "W15 (32)"

    return tour, level_key


def available_top_key(tour: str, points_key: str) -> bool:
    """
    Whether a performance is always included in the annual total.
    """
    return points_key in {"Grand Slam (ATP)", "Grand Slam (WTA)", "ATP 1000 (96)", "ATP 1000 (56)", "WTA 1000 (96)", "WTA 1000 (56)"}


def is_major_key(points_key: str) -> bool:
    return points_key in {
        "Grand Slam (ATP)", "Grand Slam (WTA)", "ATP 1000 (96)", "ATP 1000 (56)", "WTA 1000 (96)", "WTA 1000 (56)"
    }


def score_finals_event(points_table: Dict[str, int], entry: dict, stage: Optional[str]) -> int:
    """
    Finals tables include RR_win / RR_max / W_bonus etc.
    We only have a bracket-like performance label in the history, so we use the
    best available stage field and optionally consume extra optional keys if they
    exist in the history item.

    Supported optional keys in an entry (future-proof):
      rr_wins, round_robin_wins, rr_points, final_bonus, bonus
    """
    stage = stage or ""
    extra_rr_wins = (
        entry.get("rr_wins")
        or entry.get("round_robin_wins")
        or entry.get("rr_win_count")
        or entry.get("group_wins")
        or 0
    )
    try:
        extra_rr_wins = int(extra_rr_wins)
    except Exception:
        extra_rr_wins = 0

    if "ATP Finals" in points_table:
        if stage == "W":
            base = points_table.get("W_max", 0)
            bonus = int(entry.get("w_bonus") or entry.get("bonus") or 0)
            rr = min(extra_rr_wins * points_table.get("RR_win", 0), points_table.get("RR_max", 0) or 10**9)
            return int(base) + int(rr) + int(bonus)
        if stage == "F":
            base = points_table.get("F_max", 0)
            rr = min(extra_rr_wins * points_table.get("RR_win", 0), points_table.get("RR_max", 0) or 10**9)
            return int(base) + int(rr)
        if stage == "SF":
            base = points_table.get("SF_win", 0)
            rr = min(extra_rr_wins * points_table.get("RR_win", 0), points_table.get("RR_max", 0) or 10**9)
            return int(base) + int(rr)
        rr = min(extra_rr_wins * points_table.get("RR_win", 0), points_table.get("RR_max", 0) or 10**9)
        return int(rr)

    if "WTA Finals" in points_table:
        if stage == "W":
            base = points_table.get("W_max", 0)
            rr = extra_rr_wins * points_table.get("RR_win", 0)
            return int(base) + int(rr)
        if stage == "F":
            base = points_table.get("F_max", 0)
            rr = extra_rr_wins * points_table.get("RR_win", 0)
            return int(base) + int(rr)
        if stage == "SF":
            base = points_table.get("SF_max", 0)
            rr = extra_rr_wins * points_table.get("RR_win", 0)
            return int(base) + int(rr)
        return int(extra_rr_wins * points_table.get("RR_win", 0))

    return 0


def score_entry(entry: dict, meta_index: Dict[str, dict]) -> Dict[str, Any]:
    tournament_id = clean_key(entry.get("tournament_id"))
    meta = meta_index.get(tournament_id) if tournament_id else None

    tour, points_key = resolve_points_key(entry, meta)
    stage = normalize_performance_label(entry.get("performance"), entry.get("rank"))
    table = POINT_TABLES.get(tour, {}).get(points_key, {})

    if points_key in {"ATP Finals", "WTA Finals"}:
        points = score_finals_event(table, entry, stage)
    else:
        if stage is None:
            points = int(entry.get("points") or 0)
        else:
            points = int(table.get(stage, 0))

    major = is_major_key(points_key)
    return {
        "tour": tour,
        "points_key": points_key,
        "points": int(points),
        "major": major,
        "stage": stage,
        "meta": meta,
    }


# ---------------------------------------------------------------------
# History normalisation
# ---------------------------------------------------------------------

def history_event_key(item: dict) -> Tuple[str, str, str]:
    return (
        clean_key(item.get("tour") or item.get("user_tour")).upper(),
        clean_key(item.get("tournament_id")),
        str(item.get("year") or ""),
    )


def dedupe_history_items(items: List[dict]) -> List[dict]:
    """
    Keep the latest occurrence per (tour, tournament_id, year).
    """
    by_key: Dict[Tuple[str, str, str], dict] = {}
    for item in items:
        if not isinstance(item, dict):
            continue
        key = history_event_key(item)
        prev = by_key.get(key)
        if prev is None:
            by_key[key] = item
            continue

        d1 = try_parse_date(item.get("date")) or date.min
        d0 = try_parse_date(prev.get("date")) or date.min
        if d1 >= d0:
            by_key[key] = item
    return list(by_key.values())


def keep_last_n_weeks(items: List[dict], weeks: int) -> List[dict]:
    cutoff = date.today() - timedelta(weeks=weeks)
    kept = []
    for item in items:
        d = try_parse_date(item.get("date"))
        if d is not None and d >= cutoff:
            kept.append(item)
    return kept


def sanitize_history_item(item: dict) -> dict:
    out = dict(item)
    # Normalize some keys if needed.
    out["tour"] = normalize_tour(out.get("tour") or out.get("user_tour"))
    if "date" in out and out["date"] is not None:
        d = try_parse_date(out["date"])
        if d:
            out["date"] = d.isoformat()
    return out


def compute_user_rank_points(history_text: Any, meta_index: Dict[str, dict]) -> Tuple[int, List[dict]]:
    """
    Returns:
      total_points, scored_items
    """
    items = [sanitize_history_item(x) for x in parse_history(history_text)]
    items = dedupe_history_items(items)
    items = keep_last_n_weeks(items, HISTORY_WEEKS)

    scored_items: List[dict] = []
    for item in items:
        scored = score_entry(item, meta_index)
        scored_item = dict(item)
        scored_item.update(scored)
        scored_items.append(scored_item)

    majors = [x for x in scored_items if x["major"]]
    others = [x for x in scored_items if not x["major"]]

    # Always keep majors. For the rest, keep top 12 by points.
    others.sort(
        key=lambda x: (
            -int(x.get("points") or 0),
            try_parse_date(x.get("date")) or date.min,
            clean_key(x.get("tournament_name")).lower(),
            clean_key(x.get("tournament_id")),
        ),
        reverse=False,
    )
    others.sort(key=lambda x: int(x.get("points") or 0), reverse=True)

    selected = majors + others[:TOP_NON_MAJOR_PERFORMANCES]
    total_points = int(sum(int(x.get("points") or 0) for x in selected))
    return total_points, scored_items


# ---------------------------------------------------------------------
# Ranking helpers
# ---------------------------------------------------------------------

def latest_row_by_user(rows: List[dict]) -> Dict[str, dict]:
    grouped: Dict[str, List[dict]] = defaultdict(list)
    for row in rows:
        uid = clean_key(row.get("user_id"))
        if uid:
            grouped[uid].append(row)

    out: Dict[str, dict] = {}
    for uid, arr in grouped.items():
        def sort_key(r: dict):
            return (
                clean_key(r.get("updated_at")),
                clean_key(r.get("created_at")),
                clean_key(r.get("id")),
            )
        arr.sort(key=sort_key, reverse=True)
        out[uid] = arr[0]
    return out


def group_rows_by_tour(rows: List[dict]) -> Dict[str, List[dict]]:
    out: Dict[str, List[dict]] = defaultdict(list)
    for row in rows:
        tour = normalize_tour(row.get("user_tour") or row.get("tour"))
        if tour in {"ATP", "WTA"}:
            out[tour].append(row)
    return out


def rank_tied_items(items: List[dict]) -> List[dict]:
    """
    Standard competition ranking:
      1, 2, 2, 4 ...
    The returned items contain `world_rank`.
    """
    sorted_items = sorted(
        items,
        key=lambda x: (
            -int(x["user_rank_points"]),
            clean_key(x.get("user_name")).lower(),
            clean_key(x.get("user_id")),
        ),
    )

    last_points = None
    last_rank = 0
    for idx, item in enumerate(sorted_items, start=1):
        pts = int(item["user_rank_points"])
        if last_points is None or pts != last_points:
            last_rank = idx
            last_points = pts
        item["world_rank"] = last_rank
    return sorted_items


# ---------------------------------------------------------------------
# Main processing
# ---------------------------------------------------------------------

def process_ranking_table(table_name: str, dry_run: bool = False) -> None:
    meta_index = build_tournament_index()
    rows = supabase_select_all(table_name, {"select": "*"})
    if not rows:
        print(f"No rows found in {RANKING_TABLE}")
        return

    rows_by_user = latest_row_by_user(rows)
    rows_by_tour = group_rows_by_tour(list(rows_by_user.values()))

    computed_rows: List[dict] = []

    for tour in ("ATP", "WTA"):
        tour_rows = rows_by_tour.get(tour, [])
        if not tour_rows:
            continue

        print(f"\n=== {tour}: {len(tour_rows)} users ===")
        for row in tour_rows:
            total_points, scored_items = compute_user_rank_points(row.get("user_performances_this_year"), meta_index)

            computed_rows.append({
                "id": row.get("id"),
                "user_id": row.get("user_id"),
                "user_name": row.get("user_name") or row.get("pseudo") or "",
                "user_tour": tour,
                "user_rank_points": int(total_points),
                "world_rank": None,
            })

            print(
                f"  - {row.get('user_name') or row.get('pseudo') or row.get('user_id')}: "
                f"{total_points} pts ({len(scored_items)} performances)"
            )

    # Separate ranking per tour.
    updated_payloads: Dict[str, dict] = {}
    for tour in ("ATP", "WTA"):
        tour_items = [x for x in computed_rows if x["user_tour"] == tour]
        ranked = rank_tied_items(tour_items)
        for item in ranked:
            updated_payloads[item["id"]] = {
                "user_rank_points": int(item["user_rank_points"]),
                "user_world_rank": int(item["world_rank"]),
                "updated_at": now_utc_iso(),
            }

    # Write updates.
    for row_id, payload in updated_payloads.items():
        if not dry_run:
            supabase_patch(RANKING_TABLE, {"id": f"eq.{row_id}"}, payload)

    print(f"\nUpdated {len(updated_payloads)} rows in {table_name}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Recalculate user_rank_points and user_world_rank from user_performances_this_year.")
    parser.add_argument("--dry-run", action="store_true", help="Compute everything without writing to Supabase.")
    parser.add_argument("--table", default=RANKING_TABLE, help="Target table (default: bracket).")
    args = parser.parse_args()

    try:
        process_ranking_table(args.table, dry_run=args.dry_run)
    except Exception as e:
        print(f"ERROR: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
