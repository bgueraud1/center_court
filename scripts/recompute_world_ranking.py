from __future__ import annotations

import json
import math
import os
import re
from collections import defaultdict
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import requests

try:
    from zoneinfo import ZoneInfo
    PARIS_TZ = ZoneInfo("Europe/Paris")
except Exception:
    from datetime import timezone
    PARIS_TZ = timezone.utc


# -----------------------------
# Config
# -----------------------------
WORKSPACE = Path(os.environ.get("GITHUB_WORKSPACE", ".")).resolve()

ATP_TOURNAMENTS_JSON = WORKSPACE / os.environ.get("ATP_TOURNAMENTS_JSON", "docs/atp_tournaments_2026.json")
WTA_TOURNAMENTS_JSON = WORKSPACE / os.environ.get("WTA_TOURNAMENTS_JSON", "docs/wta_tournaments_2026.json")
TOURNAMENT_POINTS_JSON = WORKSPACE / os.environ.get("TOURNAMENT_POINTS_JSON", "docs/Tools/tournament_points.json")
BRACKET_DIR = WORKSPACE / os.environ.get("BRACKET_TOURNAMENTS_DIR", "docs/bracket/tournaments")

SUPABASE_URL = os.environ["SUPABASE_URL"].rstrip("/")
SUPABASE_SERVICE_ROLE_KEY = os.environ["SUPABASE_SERVICE_ROLE_KEY"]

BRACKET_TABLE = os.environ.get("BRACKET_TABLE", "bracket")
INSCRIPTIONS_TABLE = os.environ.get("INSCRIPTIONS_TABLE", "inscriptions")
USERS_TABLE = os.environ.get("USERS_TABLE", "users")

WINDOW_DAYS = 365  # 52 weeks
MATCH_WEIGHT_BASE = 2  # tunable


HEADERS = {
    "apikey": SUPABASE_SERVICE_ROLE_KEY,
    "Authorization": f"Bearer {SUPABASE_SERVICE_ROLE_KEY}",
    "Content-Type": "application/json",
    "Prefer": "return=representation",
}


# -----------------------------
# Helpers JSON / date
# -----------------------------
MONTHS = {
    "january": 1, "february": 2, "march": 3, "april": 4,
    "may": 5, "june": 6, "july": 7, "august": 8,
    "september": 9, "october": 10, "november": 11, "december": 12,
}


def today_paris() -> date:
    return datetime.now(PARIS_TZ).date()


def load_json(path: Path, default: Any = None) -> Any:
    if not path.exists():
        return default
    return json.loads(path.read_text(encoding="utf-8"))


def dump_json_text(obj: Any) -> str:
    return json.dumps(obj, ensure_ascii=False, indent=2)


def parse_iso_date(value: str) -> date:
    return date.fromisoformat(value)


def normalize_space(text: str) -> str:
    return " ".join(str(text).replace("–", "-").replace("—", "-").split()).strip()


def parse_atp_formatted_date(text: str) -> Tuple[date, date]:
    """
    Supports:
      - 4 - 11 January, 2026
      - 26 December 2025 - 02 January, 2026
      - 26 January - 02 February, 2026
    """
    s = normalize_space(text)
    patterns = [
        re.compile(
            r"^(?P<sd>\d{1,2})\s+(?P<sm>[A-Za-z]+)\s+(?P<sy>\d{4})\s*-\s*"
            r"(?P<ed>\d{1,2})\s+(?P<em>[A-Za-z]+),?\s+(?P<ey>\d{4})$"
        ),
        re.compile(
            r"^(?P<sd>\d{1,2})\s+(?P<sm>[A-Za-z]+)\s*-\s*"
            r"(?P<ed>\d{1,2})\s+(?P<em>[A-Za-z]+),?\s+(?P<ey>\d{4})$"
        ),
        re.compile(
            r"^(?P<sd>\d{1,2})\s*-\s*"
            r"(?P<ed>\d{1,2})\s+(?P<em>[A-Za-z]+),?\s+(?P<ey>\d{4})$"
        ),
    ]
    for rx in patterns:
        m = rx.match(s)
        if not m:
            continue
        g = m.groupdict()
        end_year = int(g["ey"])
        end_month = MONTHS[g["em"].lower()]
        end_day = int(g["ed"])
        end_date = date(end_year, end_month, end_day)

        if g.get("sy") and g.get("sm"):
            start_year = int(g["sy"])
            start_month = MONTHS[g["sm"].lower()]
            start_day = int(g["sd"])
        elif g.get("sm"):
            start_year = end_year
            start_month = MONTHS[g["sm"].lower()]
            start_day = int(g["sd"])
        else:
            start_year = end_year
            start_month = end_month
            start_day = int(g["sd"])

        return date(start_year, start_month, start_day), end_date

    raise ValueError(f"Unable to parse ATP formatted date: {text!r}")


def next_power_of_two(n: int) -> int:
    if n <= 1:
        return 1
    return 1 << (n - 1).bit_length()


# -----------------------------
# Supabase REST helpers
# -----------------------------
def rest_get(table: str, params: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
    url = f"{SUPABASE_URL}/rest/v1/{table}"
    params = params or {}
    resp = requests.get(url, headers=HEADERS, params=params, timeout=120)
    if not resp.ok:
        raise RuntimeError(f"GET {table} failed: {resp.status_code} {resp.text}")
    if not resp.text.strip():
        return []
    return resp.json()


def rest_patch(table: str, filters: Dict[str, Any], payload: Dict[str, Any]) -> List[Dict[str, Any]]:
    url = f"{SUPABASE_URL}/rest/v1/{table}"
    params = {k: f"eq.{v}" for k, v in filters.items()}
    resp = requests.patch(url, headers=HEADERS, params=params, json=payload, timeout=120)
    if not resp.ok:
        raise RuntimeError(f"PATCH {table} failed: {resp.status_code} {resp.text}")
    return resp.json() if resp.text.strip() else []


def rest_upsert(table: str, payload: Dict[str, Any], on_conflict: str) -> List[Dict[str, Any]]:
    url = f"{SUPABASE_URL}/rest/v1/{table}"
    headers = dict(HEADERS)
    headers["Prefer"] = f"resolution=merge-duplicates,return=representation"
    params = {"on_conflict": on_conflict}
    resp = requests.post(url, headers=headers, params=params, json=payload, timeout=120)
    if not resp.ok:
        raise RuntimeError(f"UPSERT {table} failed: {resp.status_code} {resp.text}")
    return resp.json() if resp.text.strip() else []


# -----------------------------
# Metadata parsing
# -----------------------------
def build_tournament_metadata() -> Dict[Tuple[str, str], Dict[str, Any]]:
    meta: Dict[Tuple[str, str], Dict[str, Any]] = {}

    atp = load_json(ATP_TOURNAMENTS_JSON, default={"TournamentDates": []})
    for group in atp.get("TournamentDates", []):
        for t in group.get("Tournaments", []):
            tid = str(t.get("Id") or "").strip()
            if not tid:
                continue
            try:
                start_dt, end_dt = parse_atp_formatted_date(t.get("FormattedDate", ""))
            except Exception:
                continue

            meta[("ATP", tid)] = {
                "tour": "ATP",
                "tournament_id": tid,
                "tournament_name": t.get("Name") or "",
                "location": t.get("Location") or "",
                "start_date": start_dt.isoformat(),
                "end_date": end_dt.isoformat(),
                "draw_size": int(t.get("SglDrawSize") or 0),
                "type": str(t.get("Type") or "").strip().upper(),
                "event_type_detail": t.get("EventTypeDetail"),
                "challenger_category": t.get("ChallengerCategory"),
                "raw": t,
            }

    wta = load_json(WTA_TOURNAMENTS_JSON, default={"content": []})
    for t in wta.get("content", []):
        tid = str((t.get("tournamentGroup") or {}).get("id") or "").strip()
        if not tid:
            continue
        try:
            start_dt = parse_iso_date(t["startDate"])
            end_dt = parse_iso_date(t["endDate"])
        except Exception:
            continue

        meta[("WTA", tid)] = {
            "tour": "WTA",
            "tournament_id": tid,
            "tournament_name": t.get("title") or "",
            "location": t.get("country") or "",
            "start_date": start_dt.isoformat(),
            "end_date": end_dt.isoformat(),
            "draw_size": int(t.get("singlesDrawSize") or 0),
            "level": ((t.get("tournamentGroup") or {}).get("level") or "").strip(),
            "title": t.get("title") or "",
            "tournament_link": t.get("tournamentLink") or "",
            "raw": t,
        }

    return meta


def tournament_points_key(meta: Dict[str, Any]) -> Optional[str]:
    tour = meta["tour"]
    if tour == "ATP":
        ttype = str(meta.get("type") or "").upper()
        draw = int(meta.get("draw_size") or 0)
        detail = meta.get("event_type_detail")
        name = str(meta.get("tournament_name") or "").upper()

        if ttype == "GS":
            return "Grand Slam (ATP)"
        if ttype == "1000":
            return f"ATP 1000 ({draw})"
        if ttype == "500":
            return f"ATP 500 ({draw})"
        if ttype == "250":
            return f"ATP 250 ({draw})"
        if ttype == "CH":
            try:
                return f"Challenger {int(detail)}"
            except Exception:
                return None
        if "M25" in name:
            return "Future M25"
        if "M15" in name:
            return "Future M15"
        if "FINALS" in name:
            return "ATP Finals"
        return None

    if tour == "WTA":
        level = str(meta.get("level") or "").strip().upper()
        draw = int(meta.get("draw_size") or 0)
        title = str(meta.get("title") or "").upper()

        if level == "GRAND SLAM":
            return "Grand Slam (WTA)"
        if level in {"WTA 1000", "WTA 500", "WTA 250", "WTA 125"}:
            return f"{level} ({draw})"

        # ITF / W100 / W75 / W50 / W35 / W15 from title
        m = re.search(r"\bW(\d{2,3})\b", title)
        if m:
            return f"W{m.group(1)} ({draw})"

        if "FINALS" in title:
            return "WTA Finals"
        return None

    return None


def is_mandatory_event(meta: Dict[str, Any], points_key: Optional[str]) -> bool:
    if not points_key:
        return False
    return points_key.startswith("Grand Slam") or "1000" in points_key


def rank_label_from_position(position: int) -> str:
    if position <= 0:
        return "NA"
    if position == 1:
        return "W"
    if position == 2:
        return "F"
    if position <= 4:
        return "SF"
    if position <= 8:
        return "QF"
    if position <= 16:
        return "R16"
    if position <= 32:
        return "R32"
    if position <= 64:
        return "R64"
    return "R128"


# -----------------------------
# Bracket extraction
# -----------------------------
def load_completed_bracket_files() -> List[Path]:
    if not BRACKET_DIR.exists():
        return []
    return sorted([p for p in BRACKET_DIR.glob("*.json") if p.is_file()])


def parse_filename(path: Path) -> Optional[Tuple[str, str, int]]:
    m = re.match(r"^(atp|wta)_(\d+)_(\d{4})_temporary\.json$", path.name, re.I)
    if not m:
        return None
    return m.group(1).upper(), m.group(2), int(m.group(3))


def extract_matches(payload: Dict[str, Any]) -> List[Dict[str, Any]]:
    # Preferred format
    if isinstance(payload.get("matches_flat"), list):
        return [m for m in payload["matches_flat"] if isinstance(m, dict)]

    if isinstance(payload.get("rounds"), list):
        matches = []
        for round_obj in payload["rounds"]:
            round_number = int(round_obj.get("round_number") or 0)
            for m in round_obj.get("matches") or []:
                if not isinstance(m, dict):
                    continue
                mm = dict(m)
                mm["round_number"] = int(mm.get("round_number") or round_number or 0)
                matches.append(mm)
        return matches

    if isinstance(payload.get("matches"), list):
        # Less rich fallback; only works if match objects already include round_number
        matches = []
        for m in payload["matches"]:
            if isinstance(m, dict):
                matches.append(m)
        return matches

    return []


def build_match_index(payload: Dict[str, Any]) -> Dict[str, Dict[str, Any]]:
    idx = {}
    for m in extract_matches(payload):
        mid = str(m.get("match_id") or "").strip()
        if not mid:
            continue
        idx[mid] = {
            "match_id": mid,
            "round_number": int(m.get("round_number") or 0),
            "winner_player_id": str(m.get("winner_player_id") or "").strip(),
            "winner_player_name": str(m.get("winner_player_name") or "").strip(),
            "raw": m,
        }
    return idx


def is_completed_bracket(payload: Dict[str, Any], expected_first_round_count: int) -> bool:
    if isinstance(payload.get("rounds"), list) and len(payload["rounds"]) > 1:
        return True
    if isinstance(payload.get("matches_flat"), list) and len(payload["matches_flat"]) > expected_first_round_count:
        return True
    if isinstance(payload.get("matches"), list) and len(payload["matches"]) > expected_first_round_count:
        return True
    return False


def first_round_count(draw_size: int) -> int:
    if draw_size <= 0:
        return 0
    return next_power_of_two(draw_size) // 2


# -----------------------------
# Scoring logic
# -----------------------------
def score_prediction(user_payload: Dict[str, Any], actual_payload: Dict[str, Any]) -> Tuple[int, Dict[int, int], int]:
    """
    Returns:
      total_score,
      per_round_correct_counts,
      total_correct_matches
    """
    user_idx = build_match_index(user_payload)
    actual_idx = build_match_index(actual_payload)

    score = 0
    per_round = defaultdict(int)
    total_correct = 0

    for match_id, um in user_idx.items():
        round_number = int(um.get("round_number") or 0)
        if round_number <= 1:
            continue

        am = actual_idx.get(match_id)
        if not am:
            continue

        user_winner = str(um.get("winner_player_id") or "").strip()
        actual_winner = str(am.get("winner_player_id") or "").strip()

        if user_winner and actual_winner and user_winner == actual_winner:
            total_correct += 1
            per_round[round_number] += 1
            score += MATCH_WEIGHT_BASE ** (round_number - 2)

    return score, dict(per_round), total_correct


def parse_history(value: Any) -> List[Dict[str, Any]]:
    if value is None or value == "":
        return []
    if isinstance(value, list):
        return [x for x in value if isinstance(x, dict)]
    if isinstance(value, dict):
        return [value]
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
            if isinstance(parsed, list):
                return [x for x in parsed if isinstance(x, dict)]
            if isinstance(parsed, dict):
                return [parsed]
        except Exception:
            return []
    return []


def save_history(history: List[Dict[str, Any]]) -> str:
    return json.dumps(history, ensure_ascii=False, indent=2)


def prune_history(history: List[Dict[str, Any]], cutoff: date) -> List[Dict[str, Any]]:
    pruned = []
    for entry in history:
        end_date_s = entry.get("end_date")
        if not end_date_s:
            continue
        try:
            if parse_iso_date(end_date_s) >= cutoff:
                pruned.append(entry)
        except Exception:
            continue
    return pruned


def dedupe_history_by_tournament(history: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Keep only the latest entry for the same tournament_id.
    """
    by_tid: Dict[str, Dict[str, Any]] = {}
    for entry in history:
        tid = str(entry.get("tournament_id") or "").strip()
        if not tid:
            continue
        by_tid[tid] = entry
    return list(by_tid.values())


def performance_points_from_label(points_map: Dict[str, Any], key: str, label: str) -> int:
    obj = points_map.get(key)
    if not isinstance(obj, dict):
        return 0
    val = obj.get(label, 0)
    try:
        return int(val)
    except Exception:
        return 0


def compute_user_total_points(
    user_history: List[Dict[str, Any]],
    user_tour: str,
    points_map: Dict[str, Any],
    cutoff: date,
) -> Tuple[int, List[Dict[str, Any]]]:
    """
    Applies:
      - 52-week window
      - mandatory events count even if absent (0)
      - only 19 total performances max, after mandatory inclusion
    """
    tour = user_tour.upper().strip()
    filtered = []
    for e in user_history:
        try:
            if parse_iso_date(e["end_date"]) >= cutoff:
                filtered.append(e)
        except Exception:
            continue

    # Identify mandatory events by tournament_id if present in history entries
    mandatory_entries = []
    optional_entries = []

    for e in filtered:
        if e.get("mandatory"):
            mandatory_entries.append(e)
        else:
            optional_entries.append(e)

    # If there are missing mandatory entries, they should already be present as 0-point imputed entries.
    # Keep all mandatory entries.
    selected = list(mandatory_entries)

    remaining_slots = max(0, 19 - len(selected))
    optional_entries.sort(
        key=lambda x: (
            int(x.get("performance_points") or 0),
            str(x.get("end_date") or ""),
            str(x.get("tournament_name") or ""),
        ),
        reverse=True,
    )
    selected.extend(optional_entries[:remaining_slots])

    total_points = sum(int(e.get("performance_points") or 0) for e in selected)
    return total_points, selected


# -----------------------------
# Tournament processing
# -----------------------------
def tournament_key(meta: Dict[str, Any]) -> Optional[str]:
    return tournament_points_key(meta)


def process_completed_tournaments(meta_index: Dict[Tuple[str, str], Dict[str, Any]], points_map: Dict[str, Any]) -> None:
    files = load_completed_bracket_files()
    if not files:
        print("No tournament JSON files found.")
        return

    completed = []

    for path in files:
        parsed = parse_filename(path)
        if not parsed:
            continue
        tour, tid, year = parsed
        meta = meta_index.get((tour, tid))
        if not meta:
            continue

        try:
            payload = load_json(path, default=None)
        except Exception:
            continue
        if not isinstance(payload, dict):
            continue

        expected_fr = first_round_count(int(meta.get("draw_size") or 0))
        if not is_completed_bracket(payload, expected_fr):
            continue

        pts_key = tournament_key(meta)
        if not pts_key or pts_key not in points_map:
            # skip if we cannot map the tournament to a points model
            continue

        completed.append((meta, payload, pts_key))

    if not completed:
        print("No completed tournaments to score.")
        return

    # Build a tournament -> participants map from inscriptions
    for meta, actual_payload, pts_key in completed:
        tournament_id = str(meta["tournament_id"])
        tour = meta["tour"]
        end_date = meta["end_date"]
        draw_size = int(meta.get("draw_size") or 0)

        print(f"Scoring {tour} tournament {tournament_id} ({meta.get('tournament_name')})")

        inscriptions = rest_get(
            INSCRIPTIONS_TABLE,
            params={
                "select": "user_id,user_name,user_world_rank,user_tour,user_country,tournament_id,tournament_name,tournament_num_players,tournament_start_date,registration_week_start,tour,tournament_level",
                "tournament_id": f"eq.{tournament_id}",
            },
        )

        participants = []
        for row in inscriptions:
            if str(row.get("tour") or tour).upper() != tour:
                continue
            participants.append(row)

        if not participants:
            print(f"  - no inscriptions found for tournament {tournament_id}, skipping")
            continue

        # fetch all existing bracket rows for those participants
        user_ids = [str(p.get("user_id") or "").strip() for p in participants if p.get("user_id")]
        bracket_rows = []
        if user_ids:
            # fetch all bracket rows for this tournament; filter participants in Python
            bracket_rows = rest_get(
                BRACKET_TABLE,
                params={
                    "select": "*",
                    "current_tournament_bracket_id": f"eq.{tournament_id}",
                },
            )

        row_by_user = {str(r.get("user_id") or "").strip(): r for r in bracket_rows}

        # Score each participant
        scoring_rows = []
        for p in participants:
            user_id = str(p.get("user_id") or "").strip()
            bracket_row = row_by_user.get(user_id)

            user_name = (
                (bracket_row or {}).get("user_name")
                or p.get("user_name")
                or ""
            )
            user_tour = str((bracket_row or {}).get("user_tour") or p.get("tour") or tour).upper().strip()

            history = parse_history((bracket_row or {}).get("user_performances_this_year"))
            history = dedupe_history_by_tournament(history)
            history = prune_history(history, today_paris() - timedelta(days=WINDOW_DAYS))

            submitted = False
            user_payload = None

            if bracket_row and bracket_row.get("user_current_tournament_bracket_proposition"):
                try:
                    user_payload = json.loads(bracket_row["user_current_tournament_bracket_proposition"])
                    submitted = True
                except Exception:
                    user_payload = None
                    submitted = False

            score, per_round, total_correct = (0, {}, 0)
            if submitted and isinstance(user_payload, dict):
                score, per_round, total_correct = score_prediction(user_payload, actual_payload)

            scoring_rows.append({
                "user_id": user_id,
                "user_name": user_name,
                "user_tour": user_tour,
                "user_country": (bracket_row or {}).get("user_country") or p.get("user_country"),
                "user_world_rank": (bracket_row or {}).get("user_world_rank") or p.get("user_world_rank"),
                "submitted": submitted,
                "score": score,
                "per_round": per_round,
                "total_correct": total_correct,
                "history": history,
                "bracket_row": bracket_row,
                "participant": p,
            })

        # rank participants in this tournament
        def sort_key(x: Dict[str, Any]):
            per_round = x["per_round"]
            actual_rounds = sorted({int(m.get("round_number") or 0) for m in extract_matches(actual_payload) if int(m.get("round_number") or 0) > 1}, reverse=True)
            deeper = tuple(int(per_round.get(r, 0)) for r in actual_rounds)
            return (
                int(x["score"]),
                int(x["total_correct"]),
                1 if x["submitted"] else 0,
                deeper,
                -(int(x["user_world_rank"]) if str(x["user_world_rank"]).isdigit() else 999999),
                str(x["user_name"]).lower(),
            )

        scoring_rows.sort(key=sort_key, reverse=True)

        # convert rank -> performance entry
        for position, row in enumerate(scoring_rows, start=1):
            label = rank_label_from_position(position)
            points = int(points_map[pts_key].get(label, 0)) if pts_key in points_map else 0

            perf_entry = {
                "tournament_id": tournament_id,
                "tournament_name": meta.get("tournament_name") or row["participant"].get("tournament_name") or "",
                "tour": tour,
                "tournament_level": pts_key,
                "start_date": meta.get("start_date"),
                "end_date": end_date,
                "performance": label,
                "performance_points": points,
                "prediction_score": int(row["score"]),
                "total_correct_matches": int(row["total_correct"]),
                "rank_in_tournament": position,
                "submitted": bool(row["submitted"]),
                "mandatory": bool(is_mandatory_event(meta, pts_key)),
                "updated_at": datetime.now(PARIS_TZ).isoformat(),
            }

            # Update or create history
            history = row["history"]
            history = [e for e in history if str(e.get("tournament_id") or "").strip() != tournament_id]
            history.append(perf_entry)
            history = dedupe_history_by_tournament(prune_history(history, today_paris() - timedelta(days=WINDOW_DAYS)))
            history.sort(key=lambda e: str(e.get("end_date") or ""), reverse=True)

            # Update bracket row
            if row["bracket_row"]:
                rest_patch(
                    BRACKET_TABLE,
                    filters={
                        "user_id": row["user_id"],
                        "current_tournament_bracket_id": tournament_id,
                    },
                    payload={
                        "user_performances_this_year": save_history(history),
                        # Optional: keep this if you want an all-time winners list
                        # "user_tournaments_won": ...
                    },
                )
            else:
                # Optional fallback: insert a row if none exists.
                # In many setups this will never be needed.
                insert_payload = {
                    "user_id": row["user_id"],
                    "user_name": row["user_name"],
                    "user_world_rank": row["user_world_rank"],
                    "user_tour": row["user_tour"],
                    "user_country": row["user_country"],
                    "current_tournament_bracket_id": tournament_id,
                    "current_tournament_bracket_name": meta.get("tournament_name") or "",
                    "user_performances_this_year": save_history(history),
                }
                rest_upsert(BRACKET_TABLE, insert_payload, on_conflict="user_id,current_tournament_bracket_id")

        print(f"  - scored {len(scoring_rows)} participants")


# -----------------------------
# World ranking recalculation
# -----------------------------
def recompute_world_ranks(points_map: Dict[str, Any]) -> None:
    # Fetch all bracket rows
    rows = rest_get(BRACKET_TABLE, params={"select": "*"})

    cutoff = today_paris() - timedelta(days=WINDOW_DAYS)

    # Group by tour
    by_tour: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    for row in rows:
        tour = str(row.get("user_tour") or "").upper().strip()
        if tour not in {"ATP", "WTA"}:
            continue
        by_tour[tour].append(row)

    updates = []

    for tour, tour_rows in by_tour.items():
        # parse histories and compute points
        computed = []
        for row in tour_rows:
            history = parse_history(row.get("user_performances_this_year"))
            history = dedupe_history_by_tournament(history)
            history = prune_history(history, cutoff)

            # Ensure mandatory events with 0 are kept if they are missing only within the recorded window.
            # The tournament scoring pass already inserts them for completed mandatory events.
            total_points, selected_entries = compute_user_total_points(history, tour, points_map, cutoff)

            # Overwrite history with the pruned, deduped version
            history.sort(key=lambda e: str(e.get("end_date") or ""), reverse=True)

            computed.append({
                "row": row,
                "user_id": str(row.get("user_id") or ""),
                "user_name": row.get("user_name") or "",
                "user_tour": tour,
                "points": int(total_points),
                "history": history,
                "selected_entries": selected_entries,
                "wins": sum(1 for e in selected_entries if str(e.get("performance") or "") == "W"),
                "finals": sum(1 for e in selected_entries if str(e.get("performance") or "") == "F"),
                "semis": sum(1 for e in selected_entries if str(e.get("performance") or "") == "SF"),
                "quarters": sum(1 for e in selected_entries if str(e.get("performance") or "") == "QF"),
            })

        computed.sort(
            key=lambda x: (
                x["points"],
                x["wins"],
                x["finals"],
                x["semis"],
                x["quarters"],
                str(x["user_name"]).lower(),
            ),
            reverse=True,
        )

        for rank, item in enumerate(computed, start=1):
            updates.append({
                "user_id": item["user_id"],
                "user_tour": tour,
                "payload": {
                    "user_rank_points": item["points"],
                    "user_world_rank": rank,
                    "user_performances_this_year": save_history(item["history"]),
                }
            })

    # Push updates one by one
    for upd in updates:
        user_id = upd["user_id"]
        tour = upd["user_tour"]
        payload = upd["payload"]
        rest_patch(
            BRACKET_TABLE,
            filters={"user_id": user_id, "user_tour": tour},
            payload=payload,
        )

    print(f"Updated world ranks for {len(updates)} users.")


def main() -> int:
    points_map = load_json(TOURNAMENT_POINTS_JSON, default={})
    if not isinstance(points_map, dict):
        raise RuntimeError("tournament_points.json must be a JSON object")

    meta_index = build_tournament_metadata()

    print(f"Today Paris: {today_paris().isoformat()}")
    print(f"Loaded {len(meta_index)} tournament metadata rows")
    print(f"Loaded {len(points_map)} points tables")

    process_completed_tournaments(meta_index, points_map)
    recompute_world_ranks(points_map)

    print("Done.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())