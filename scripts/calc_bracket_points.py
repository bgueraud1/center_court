
import argparse
import json
import math
import os
import re
import sys
from collections import defaultdict
from copy import deepcopy
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

SUPABASE_URL = os.environ.get("SUPABASE_URL", "").rstrip("/")
SUPABASE_KEY = os.environ.get("SUPABASE_SERVICE_ROLE_KEY", "")

USERS_TABLE = os.environ.get("USERS_TABLE", "users")
NEXT_INSCRIPTIONS_TABLE = os.environ.get("NEXT_INSCRIPTIONS_TABLE", "next_inscriptions")
BRACKET_TABLE = os.environ.get("BRACKET_TABLE", "bracket")

DEFAULT_COMPLETED_DIR = Path(os.environ.get("COMPLETED_TOURNAMENTS_DIR", "docs/bracket"))
ATP_TOURNAMENTS_PATH = os.environ.get("ATP_TOURNAMENTS_PATH", "docs/atp_tournaments_2026")
WTA_TOURNAMENTS_PATH = os.environ.get("WTA_TOURNAMENTS_PATH", "docs/wta_tournaments_2026")

POINTS_BASE = int(os.environ.get("POINTS_BASE", "1"))
POINTS_MULTIPLIER = int(os.environ.get("POINTS_MULTIPLIER", "2"))

JSON_HEADERS = {
    "apikey": SUPABASE_KEY,
    "Authorization": f"Bearer {SUPABASE_KEY}",
    "Accept": "application/json",
}

MONTHS = {
    "january": 1, "february": 2, "march": 3, "april": 4, "may": 5, "june": 6,
    "july": 7, "august": 8, "september": 9, "october": 10, "november": 11, "december": 12,
}


# ----------------------------
# Generic helpers
# ----------------------------

def now_utc_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def read_json_file(path: Path) -> Any:
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)


def read_json_file_flexible(path_like: str) -> Optional[Any]:
    """
    Supports:
      - exact path
      - exact path + ".json"
    """
    p = Path(path_like)
    candidates = [p]
    if p.suffix.lower() != ".json":
        candidates.append(Path(str(p) + ".json"))

    for cand in candidates:
        if cand.exists():
            return read_json_file(cand)
    return None


def dump_text_json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"))


def parse_text_json_maybe(value: Any) -> Any:
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


def parse_json_listish(value: Any) -> List[dict]:
    parsed = parse_text_json_maybe(value)
    if isinstance(parsed, list):
        return [x for x in parsed if isinstance(x, dict)]
    if isinstance(parsed, dict):
        return [parsed]
    return []


def safe_str(value: Any) -> str:
    return "" if value is None else str(value)


def clean_key(value: Any) -> str:
    return safe_str(value).strip()


def get_match_prefix(match_id: Any) -> str:
    m = re.match(r"^(MS|LS)", safe_str(match_id), re.I)
    return m.group(1).upper() if m else "MS"


def get_match_num(match_id: Any) -> int:
    m = re.match(r"^(?:MS|LS)(\d+)$", safe_str(match_id), re.I)
    return int(m.group(1)) if m else 10**9


def is_all_mode(flat_json: dict) -> bool:
    matches = flat_json.get("matches") if isinstance(flat_json, dict) else []
    if not isinstance(matches, list):
        return False
    for m in matches:
        if not isinstance(m, dict):
            continue
        mid = safe_str(m.get("match_id")).upper()
        if mid in {"MS001", "LS001"}:
            return True
    return False


def next_power_of_two(n: int) -> int:
    p = 1
    while p < max(1, n):
        p *= 2
    return p


def performance_label(rank: int, total_players: int) -> str:
    """
    Tennis-like places:
      1 Winner
      2 Finalist
      3-4 SF
      5-8 QF
      9-16 R16
      17-32 R32
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


# ----------------------------
# Supabase REST helpers
# ----------------------------

def require_supabase():
    if not SUPABASE_URL or not SUPABASE_KEY:
        raise RuntimeError("Missing SUPABASE_URL or SUPABASE_SERVICE_ROLE_KEY")


def supabase_request(
    method: str,
    table: str,
    query: Optional[Dict[str, Any]] = None,
    payload: Any = None,
    prefer_return_representation: bool = False,
) -> Any:
    require_supabase()

    query = query or {}
    url = f"{SUPABASE_URL}/rest/v1/{table}"
    if query:
        url += "?" + urlencode([(k, v) for k, v in query.items() if v is not None and v != ""])

    headers = dict(JSON_HEADERS)
    body = None
    if method.upper() != "GET":
        headers["Content-Type"] = "application/json"
        headers["Prefer"] = "return=representation" if prefer_return_representation else "return=minimal"
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
    """
    Paginates with limit/offset and returns all rows.
    """
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
    return supabase_request("PATCH", table, query, payload, prefer_return_representation=True)


def supabase_insert(table: str, payload: dict) -> Any:
    res = supabase_request("POST", table, {"select": "*"}, [payload], prefer_return_representation=True)
    if isinstance(res, list):
        return res[0] if res else None
    return res


def supabase_delete(table: str, query: Dict[str, Any]) -> Any:
    return supabase_request("DELETE", table, query, None, prefer_return_representation=True)


# ----------------------------
# Tournament metadata lookup
# ----------------------------

def normalize_atp_level(raw: dict) -> str:
    """
    Tries to map ATP metadata to one of:
      GS, 1000, 500, 250, Challenger 175/125/100/75/50, Future M25/M15
    Falls back to a readable raw label when needed.
    """
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

    # Challenger examples: CH175, Challenger 125, etc.
    m = re.search(r"\bCH\s*([0-9]{2,3})\b", text)
    if m:
        return f"Challenger {m.group(1)}"
    m = re.search(r"\bCHALLENGER\s*([0-9]{2,3})\b", text)
    if m:
        return f"Challenger {m.group(1)}"
    if "CHALLENGER" in text:
        # Sometimes the level is only discoverable in the name/title.
        m = re.search(r"\b(CH(?:ALLENGER)?\s*[0-9]{2,3})\b", text)
        if m:
            num = re.search(r"([0-9]{2,3})", m.group(1))
            if num:
                return f"Challenger {num.group(1)}"
        return "Challenger"

    # Futures
    m = re.search(r"\bM\s*(15|25)\b", text)
    if m:
        return f"Future M{m.group(1)}"
    if "FUTURE" in text or safe_str(raw.get("Type")).upper() == "FU":
        m = re.search(r"\bM\s*(15|25)\b", text)
        if m:
            return f"Future M{m.group(1)}"
        return "Future"

    # Fallbacks
    if safe_str(raw.get("Type")).strip():
        return safe_str(raw.get("Type")).strip()
    if safe_str(raw.get("EventType")).strip():
        return safe_str(raw.get("EventType")).strip()
    return "ATP"


def normalize_wta_level(raw: dict) -> str:
    """
    Tries to map WTA metadata to one of:
      Grand Slam, WTA 1000, WTA 500, WTA 250, WTA 125,
      W100, W75, W50, W35, W25, W15
    'WTA 124' is treated as an alias of WTA 125 for compatibility.
    """
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

    # Compatibility with the user's wording.
    if re.search(r"\bWTA\s*124\b", text):
        return "WTA 125"

    for code in ("W100", "W75", "W50", "W35", "W25", "W15"):
        if re.search(rf"\b{code}\b", text):
            return code

    # ITF or other raw values
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
    """
    Examples:
      '2 - 11 January, 2026'
      '18 January - 1 February, 2026'
      '12 - 17 January, 2026'
    """
    if not formatted:
        return None
    s = formatted.strip()

    # 18 January - 1 February, 2026
    m = re.match(
        r"^(\d{1,2})\s+([A-Za-z]+)\s*-\s*(\d{1,2})\s+([A-Za-z]+),\s*(\d{4})$",
        s,
    )
    if m:
        d1, mon1, _, _, y = m.groups()
        month = MONTHS.get(mon1.lower())
        if month:
            return date(int(y), month, int(d1))

    # 2 - 11 January, 2026
    m = re.match(
        r"^(\d{1,2})\s*-\s*(\d{1,2})\s+([A-Za-z]+),\s*(\d{4})$",
        s,
    )
    if m:
        d1, _, mon, y = m.groups()
        month = MONTHS.get(mon.lower())
        if month:
            return date(int(y), month, int(d1))

    # 2 January, 2026
    m = re.match(r"^(\d{1,2})\s+([A-Za-z]+),\s*(\d{4})$", s)
    if m:
        d1, mon, y = m.groups()
        month = MONTHS.get(mon.lower())
        if month:
            return date(int(y), month, int(d1))

    return None


def tournament_date_for_history(meta: dict) -> str:
    """
    Returns an ISO date string whenever possible.
    """
    if meta.get("start_date"):
        return safe_str(meta["start_date"])
    if meta.get("end_date"):
        return safe_str(meta["end_date"])
    if meta.get("formatted_date") and meta.get("tour") == "ATP":
        parsed = parse_atp_formatted_date(meta["formatted_date"])
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
                "year": 2026,  # can be overwritten when discovered from filenames
                "start_date": None,
                "end_date": None,
                "formatted_date": clean_key(raw.get("FormattedDate")),
                "surface": clean_key(raw.get("Surface")),
                "country": clean_key(raw.get("Location")),
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
        tid = clean_key((raw.get("tournamentGroup") or {}).get("id") if isinstance(raw.get("tournamentGroup"), dict) else None)
        if not tid:
            continue
        meta = {
            "tour": "WTA",
            "id": tid,
            "name": clean_key(raw.get("title")) or clean_key((raw.get("tournamentGroup") or {}).get("name")) or f"WTA {tid}",
            "level": normalize_wta_level(raw),
            "year": int(raw.get("year") or 2026),
            "start_date": clean_key(raw.get("startDate")),
            "end_date": clean_key(raw.get("endDate")),
            "formatted_date": clean_key(raw.get("startDate")),
            "surface": clean_key(raw.get("surface")),
            "country": clean_key(raw.get("country")),
            "raw": raw,
        }
        index[tid] = meta
    return index


def build_tournament_index() -> Dict[str, dict]:
    atp = load_atp_metadata_index(ATP_TOURNAMENTS_PATH)
    wta = load_wta_metadata_index(WTA_TOURNAMENTS_PATH)

    index: Dict[str, dict] = {}
    index.update(atp)
    index.update(wta)
    return index


def infer_tournament_info(full_json: dict, filename: str, meta_index: Dict[str, dict]) -> dict:
    """
    Tries JSON first, then filename, then metadata index.
    """
    event_id = clean_key(full_json.get("event_id") or full_json.get("tournament_id") or full_json.get("id"))
    event_year = full_json.get("event_year") or full_json.get("year")
    tour = clean_key(full_json.get("tour") or full_json.get("tour_type") or full_json.get("surfaceTour"))

    if not tour:
        m = re.search(r"\b(atp|wta)\b", filename, re.I)
        if m:
            tour = m.group(1).upper()

    # filename pattern: atp_425_2026.json or atp_425_2026_temporary.json
    m = re.search(r"\b(atp|wta)_(\d+)_(\d{4})", filename, re.I)
    if m:
        tour = m.group(1).upper()
        event_id = event_id or m.group(2)
        event_year = event_year or int(m.group(3))

    meta = meta_index.get(str(event_id)) if event_id else None
    if meta:
        resolved = deepcopy(meta)
        resolved["year"] = int(event_year or resolved.get("year") or 2026)
        resolved["tour"] = resolved.get("tour") or tour.upper()
        return resolved

    # Fallback minimal metadata.
    resolved = {
        "tour": tour.upper() if tour else "UNKNOWN",
        "id": str(event_id) if event_id else None,
        "year": int(event_year or 2026),
        "name": full_json.get("event_name") or full_json.get("name") or filename,
        "level": full_json.get("level") or full_json.get("type") or "UNKNOWN",
        "start_date": full_json.get("start_date") or full_json.get("startDate"),
        "end_date": full_json.get("end_date") or full_json.get("endDate"),
        "formatted_date": full_json.get("formatted_date") or full_json.get("FormattedDate"),
        "surface": full_json.get("surface"),
        "country": full_json.get("country"),
        "raw": full_json,
    }
    return resolved


# ----------------------------
# Bracket / scoring logic
# ----------------------------

def build_rounds_from_matches(flat_json: dict) -> Tuple[List[List[str]], Dict[str, int]]:
    """
    Replicates the round inference used in your JS bracket renderer.
    Returns:
      rounds: list of round match_id lists, from earliest to latest
      round_by_match_id: mapping match_id -> round_index (0 = earliest)
    """
    matches = flat_json.get("matches") if isinstance(flat_json, dict) else []
    if not isinstance(matches, list):
        return [], {}

    ids = sorted(
        [safe_str(m.get("match_id")) for m in matches if isinstance(m, dict) and m.get("match_id")],
        key=get_match_num,
    )
    if not ids:
        return [], {}

    all_mode = is_all_mode(flat_json)
    leaf_count = math.ceil(len(ids) / 2) if all_mode else len(ids)
    leaf_ids = sorted(ids[-leaf_count:], key=get_match_num)

    rounds: List[List[str]] = []
    current_ids = leaf_ids[:]
    safety = 0

    while current_ids and safety < 30:
        rounds.append(current_ids[:])
        if len(current_ids) == 1:
            break

        next_ids: List[str] = []
        for i in range(0, len(current_ids), 2):
            left_id = current_ids[i]
            right_id = current_ids[i + 1] if i + 1 < len(current_ids) else None
            if not left_id or not right_id:
                continue
            next_num = math.floor(get_match_num(left_id) / 2)
            next_ids.append(f"{get_match_prefix(left_id)}{next_num:03d}")

        current_ids = sorted(next_ids, key=get_match_num)
        safety += 1

    round_by_match_id = {}
    for ridx, round_ids in enumerate(rounds):
        for mid in round_ids:
            round_by_match_id[mid] = ridx

    return rounds, round_by_match_id


def compare_winner(off: dict, pred: dict) -> bool:
    """
    True when the chosen winner matches.
    Compares id first, then name.
    """
    if not isinstance(off, dict) or not isinstance(pred, dict):
        return False

    off_id = clean_key(off.get("winner_player_id"))
    pred_id = clean_key(pred.get("winner_player_id"))
    off_name = clean_key(off.get("winner_player_name")).upper()
    pred_name = clean_key(pred.get("winner_player_name")).upper()

    if off_id and pred_id:
        return off_id == pred_id
    if off_name and pred_name:
        return off_name == pred_name
    return False


def score_proposal(official: dict, proposal: dict) -> dict:
    """
    Returns:
      {
        points: int,
        correct_total: int,
        correct_by_round: [int...],
        match_scores: {match_id: bool}
      }
    """
    rounds, round_by_match_id = build_rounds_from_matches(official)
    official_matches = official.get("matches") if isinstance(official, dict) else []
    proposal_matches = proposal.get("matches") if isinstance(proposal, dict) else []

    if not isinstance(official_matches, list) or not isinstance(proposal_matches, list):
        return {"points": 0, "correct_total": 0, "correct_by_round": [], "match_scores": {}}

    official_by_id = {
        safe_str(m.get("match_id")): m
        for m in official_matches
        if isinstance(m, dict) and m.get("match_id")
    }
    proposal_by_id = {
        safe_str(m.get("match_id")): m
        for m in proposal_matches
        if isinstance(m, dict) and m.get("match_id")
    }

    correct_by_round = [0 for _ in rounds]
    match_scores: Dict[str, bool] = {}
    total_points = 0
    correct_total = 0

    for mid, off in official_by_id.items():
        pred = proposal_by_id.get(mid)
        ok = compare_winner(off, pred) if pred else False
        match_scores[mid] = ok
        if ok:
            correct_total += 1
            ridx = round_by_match_id.get(mid, 0)
            if 0 <= ridx < len(correct_by_round):
                correct_by_round[ridx] += 1
            total_points += POINTS_BASE * (POINTS_MULTIPLIER ** ridx)

    return {
        "points": total_points,
        "correct_total": correct_total,
        "correct_by_round": correct_by_round,
        "match_scores": match_scores,
        "rounds": rounds,
    }


def tournament_draw_size(meta: dict, official_json: dict) -> int:
    """
    Prefer metadata draw size if available, otherwise use match count / inferred first round size.
    """
    raw = meta.get("raw") if isinstance(meta, dict) else None

    # ATP
    if isinstance(raw, dict):
        for key in ("SglDrawSize", "singlesDrawSize", "drawSize"):
            val = raw.get(key)
            if isinstance(val, int) and val > 0:
                return val

    # WTA
    if isinstance(raw, dict):
        for key in ("singlesDrawSize", "drawSize"):
            val = raw.get(key)
            if isinstance(val, int) and val > 0:
                return val

    # Fallback from official JSON
    matches = official_json.get("matches") if isinstance(official_json, dict) else []
    if isinstance(matches, list) and matches:
        # first round size is roughly number of leaf matches * 2
        rounds, _ = build_rounds_from_matches(official_json)
        if rounds:
            return len(rounds[0]) * 2
    return len(matches) if isinstance(matches, list) else 0


# ----------------------------
# User history helpers
# ----------------------------

def parse_event_date_for_history(meta: dict) -> Optional[date]:
    iso = tournament_date_for_history(meta)
    try:
        return date.fromisoformat(iso[:10])
    except Exception:
        return None


def build_performance_item(meta: dict, rank: int, points: int, total_players: int) -> dict:
    event_date = tournament_date_for_history(meta)
    return {
        "tour": meta.get("tour"),
        "tournament_name": meta.get("name"),
        "tournament_id": meta.get("id"),
        "level": meta.get("level"),
        "year": meta.get("year"),
        "date": event_date,
        "points": points,
        "rank": rank,
        "performance": performance_label(rank, total_players),
    }


def keep_last_52_weeks(history: List[dict], cutoff: date) -> List[dict]:
    out = []
    for item in history:
        if not isinstance(item, dict):
            continue
        raw_date = clean_key(item.get("date"))
        d = None
        try:
            d = date.fromisoformat(raw_date[:10])
        except Exception:
            d = None
        if d is None or d >= cutoff:
            out.append(item)
    return out


def update_user_performance_text(existing_text: Any, new_item: dict) -> str:
    history = parse_json_listish(existing_text)

    # Replace same tournament+year if already present, then re-sort/trim.
    key = (clean_key(new_item.get("tournament_id")), str(new_item.get("year") or ""), clean_key(new_item.get("tour")))
    filtered = []
    for item in history:
        item_key = (clean_key(item.get("tournament_id")), str(item.get("year") or ""), clean_key(item.get("tour")))
        if item_key != key:
            filtered.append(item)
    filtered.append(new_item)

    cutoff = date.today() - timedelta(weeks=52)
    filtered = keep_last_52_weeks(filtered, cutoff)

    # Sort by date if possible.
    def sort_key(x: dict):
        try:
            return date.fromisoformat(clean_key(x.get("date"))[:10])
        except Exception:
            return date.min

    filtered.sort(key=sort_key)
    return dump_text_json(filtered)


def update_user_wins_text(existing_text: Any, new_item: dict) -> str:
    wins = parse_json_listish(existing_text)
    key = (clean_key(new_item.get("tournament_id")), str(new_item.get("year") or ""), clean_key(new_item.get("tour")))
    filtered = []
    for item in wins:
        item_key = (clean_key(item.get("tournament_id")), str(item.get("year") or ""), clean_key(item.get("tour")))
        if item_key != key:
            filtered.append(item)
    filtered.append(new_item)

    def sort_key(x: dict):
        try:
            return date.fromisoformat(clean_key(x.get("date"))[:10])
        except Exception:
            return date.min

    filtered.sort(key=sort_key)
    return dump_text_json(filtered)


# ----------------------------
# Data loading
# ----------------------------

def load_completed_tournament(path: Path) -> dict:
    data = read_json_file(path)
    if not isinstance(data, dict):
        raise RuntimeError(f"{path} does not look like a tournament JSON object")
    if not isinstance(data.get("matches"), list) or not data["matches"]:
        raise RuntimeError(f"{path} has no matches")
    return data


def discover_completed_files(input_path: Path) -> List[Path]:
    if input_path.is_file():
        return [input_path]
    if not input_path.exists():
        return []
    files = []
    for p in sorted(input_path.rglob("*.json")):
        name = p.name.lower()
        # exclude temporary/partial files
        if name.endswith("_temporary.json"):
            continue
        if "open_inscriptions" in name:
            continue
        files.append(p)
    return files


def latest_row_by_user(rows: List[dict]) -> Dict[str, dict]:
    """
    Deduplicates by user_id; keeps the most recently updated row when possible.
    """
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


def row_matches_tournament(row: dict, meta: dict) -> bool:
    tid = clean_key(meta.get("id"))
    tour = clean_key(meta.get("tour")).upper()
    year = str(meta.get("year") or "")

    row_tid_1 = clean_key(row.get("current_tournament_bracket_id"))
    row_tid_2 = clean_key(row.get("user_current_tournament_bracket_id"))
    row_tour = clean_key(row.get("user_tour") or row.get("tour")).upper()
    row_target = clean_key(row.get("target_start_date"))

    if tid and (row_tid_1 == tid or row_tid_2 == tid):
        return True
    if tour and row_tour and tour == row_tour:
        # if id is unavailable, use year or target_start_date as a weak fallback
        if year and year in row_target:
            return True
    return False


# ----------------------------
# Main processing
# ----------------------------

def process_tournament_file(path: Path, meta_index: Dict[str, dict], dry_run: bool = False) -> None:
    print(f"\n=== Processing: {path.name} ===")
    full_json = load_completed_tournament(path)
    meta = infer_tournament_info(full_json, path.name, meta_index)

    tournament_id = clean_key(meta.get("id"))
    tournament_name = clean_key(meta.get("name"))
    tour = clean_key(meta.get("tour")).upper()
    year = int(meta.get("year") or 2026)

    if not tournament_id:
        raise RuntimeError(f"Cannot resolve tournament id for {path.name}")

    print(f"Resolved tournament: {tour} {tournament_id} | {tournament_name} | {meta.get('level')} | {year}")

    # 1) Load current bracket rows for this tournament.
    # We fetch a broad set and filter locally because the schema can vary.
    bracket_rows = supabase_select_all(BRACKET_TABLE, {"select": "*"})
    current_rows = [r for r in bracket_rows if row_matches_tournament(r, meta)]
    current_rows_by_user = latest_row_by_user(current_rows)

    if not current_rows_by_user:
        print("No current bracket rows found for this tournament.")
    else:
        print(f"Found {len(current_rows_by_user)} current users to score.")

    # 2/3) Score every user's proposition against the official completed draw.
    official_proposal = parse_text_json_maybe(full_json) or full_json
    if not isinstance(official_proposal, dict):
        raise RuntimeError(f"{path.name} official JSON is not a dict")

    scored = []
    for user_id, row in current_rows_by_user.items():
        proposal_json = parse_text_json_maybe(row.get("user_current_tournament_bracket_proposition"))
        score = score_proposal(official_proposal, proposal_json or {"matches": []})

        scored.append({
            "user_id": user_id,
            "row": row,
            "score": score["points"],
            "correct_total": score["correct_total"],
            "correct_by_round": score["correct_by_round"],
            "match_scores": score["match_scores"],
        })

    # 4) Rank users on the same tournament.
    def sort_key(item: dict):
        # Points desc, then later rounds correct desc, then total correct desc, then stable name.
        corr = item["correct_by_round"]
        later_first = tuple(reversed(corr))  # final round first
        return (
            -item["score"],
            tuple(-x for x in later_first),
            -item["correct_total"],
            safe_str(item["row"].get("user_name")).lower(),
            item["user_id"],
        )

    scored.sort(key=sort_key)

    total_players = len(scored)
    draw_size = tournament_draw_size(meta, official_proposal)

    print(f"Ranking {total_players} users | inferred draw size: {draw_size}")

    # 5) Update user_performances_this_year
    # 6) If winner, update user_tournaments_won
    for rank, item in enumerate(scored, start=1):
        row = item["row"]
        user_id = item["user_id"]
        points = item["score"]
        perf_item = build_performance_item(meta, rank=rank, points=points, total_players=total_players)

        user_row = supabase_select_one(USERS_TABLE, {"select": "id,pseudo,user_performances_this_year,user_tournaments_won", "id": f"eq.{user_id}"})
        if not user_row:
            print(f"  - user {user_id}: not found in users table, skipping history update")
            continue

        new_perf_text = update_user_performance_text(user_row.get("user_performances_this_year"), perf_item)
        update_payload = {
            "user_performances_this_year": new_perf_text,
        }

        if rank == 1:
            win_item = {
                "tour": meta.get("tour"),
                "tournament_name": meta.get("name"),
                "tournament_id": meta.get("id"),
                "level": meta.get("level"),
                "year": meta.get("year"),
                "date": tournament_date_for_history(meta),
            }
            new_wins_text = update_user_wins_text(user_row.get("user_tournaments_won"), win_item)
            update_payload["user_tournaments_won"] = new_wins_text

        if not dry_run:
            supabase_patch(USERS_TABLE, {"id": f"eq.{user_id}"}, update_payload)

        print(
            f"  - rank {rank:>3} | {row.get('user_name') or user_id} | "
            f"{points} pts | {perf_item['performance']}"
            + (" | WINNER" if rank == 1 else "")
        )

    # 7) Transfer next_inscriptions -> bracket for the users concerned by this tournament.
    # Here we promote rows matching the same tournament id / tour.
    next_rows = supabase_select_all(NEXT_INSCRIPTIONS_TABLE, {"select": "*"})
    relevant_next = [
        r for r in next_rows
        if clean_key(r.get("tournament_id")) == tournament_id
           or (clean_key(r.get("tour")).upper() == tour and clean_key(r.get("target_start_date"))[:4] == str(year))
    ]

    if relevant_next:
        print(f"Promoting {len(relevant_next)} next_inscriptions rows into bracket")
    else:
        print("No next_inscriptions rows to promote for this tournament")

    bracket_by_user = latest_row_by_user([r for r in bracket_rows if clean_key(r.get("user_id"))])

    official_json_text = dump_text_json(full_json)

    for row in relevant_next:
        user_id = clean_key(row.get("user_id"))
        if not user_id:
            continue

        payload = {
            "user_id": user_id,
            "user_name": row.get("user_name"),
            "user_tour": row.get("tour") or tour,
            "user_country": row.get("user_country"),
            "user_world_rank": row.get("user_world_rank"),
            "current_tournament_bracket_id": tournament_id,
            "current_tournament_bracket_name": tournament_name,
            "current_tournament_bracket": official_json_text,
            "user_current_tournament_bracket_id": tournament_id,
            "user_current_tournament_bracket_name": tournament_name,
            "user_current_tournament_bracket_proposition": row.get("user_proposition_next_week"),
            "updated_at": now_utc_iso(),
        }

        existing = bracket_by_user.get(user_id)

        if not dry_run:
            if existing and clean_key(existing.get("id")):
                supabase_patch(BRACKET_TABLE, {"id": f"eq.{existing['id']}"}, payload)
            else:
                supabase_insert(BRACKET_TABLE, payload)

        # 8) Delete the next_inscriptions row after transfer.
        if not dry_run and clean_key(row.get("id")):
            supabase_delete(NEXT_INSCRIPTIONS_TABLE, {"id": f"eq.{row['id']}"})

        print(f"  - transferred + deleted next inscription for user {user_id}")

    print("Done.")


def main() -> None:
    parser = argparse.ArgumentParser(description="Calculate bracket points, update user history, and migrate next-week submissions.")
    parser.add_argument(
        "--input",
        type=str,
        default=str(DEFAULT_COMPLETED_DIR),
        help="Completed tournament JSON file or directory (default: docs/bracket).",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Compute everything but do not write to Supabase.",
    )
    args = parser.parse_args()

    input_path = Path(args.input)
    meta_index = build_tournament_index()
    files = discover_completed_files(input_path)

    if not files:
        print(f"No completed tournament JSON files found in {input_path}", file=sys.stderr)
        sys.exit(1)

    for f in files:
        try:
            process_tournament_file(f, meta_index, dry_run=args.dry_run)
        except Exception as e:
            print(f"ERROR processing {f.name}: {e}", file=sys.stderr)


if __name__ == "__main__":
    main()
