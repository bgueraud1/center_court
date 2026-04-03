from __future__ import annotations

import argparse
import logging
import json
import math
import re
from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Iterable, Optional

import pandas as pd


# -----------------------------------------------------------------------------
# Paths / logging
# -----------------------------------------------------------------------------

SCRIPT_PATH = Path(__file__).resolve()
SCRIPT_DIR = SCRIPT_PATH.parent
PROJECT_ROOT = SCRIPT_DIR.parent if (SCRIPT_DIR / "docs").exists() or (SCRIPT_DIR.parent / "docs").exists() else Path.cwd()


# -----------------------------------------------------------------------------
# Configuration
# -----------------------------------------------------------------------------

# The ranking feeds may use different sign conventions. In the sample provided
# by the user, positive values are treated as a rise. If your upstream feed uses
# the opposite convention, flip this constant to -1.
RANK_DIRECTION = 1

# Significance thresholds are deliberately rank-dependent: the lower the rank
# number, the smaller the gap needed to qualify as a meaningful upset.
SIGNIFICANCE_THRESHOLDS = (
    (10, 3),
    (25, 6),
    (50, 12),
    (100, 20),
    (200, 35),
    (500, 60),
    (10_000, 100),
)

# Round ordering for standard elimination draws. Lower is deeper in the event.
ROUND_ORDER = {
    "W": 0,
    "F": 1,
    "SF": 2,
    "QF": 3,
    "R16": 4,
    "R32": 5,
    "R64": 6,
    "R128": 7,
    "RR": 4,  # best-effort for round robin; only used as a fallback.
}

# -----------------------------------------------------------------------------
# Level points table (from user input)
# -----------------------------------------------------------------------------

POINTS_TABLE = {
    "Grand Slam (ATP)": {
        "W": 2000,
        "F": 1300,
        "SF": 800,
        "QF": 400,
        "R16": 200,
        "R32": 100,
        "R64": 50,
        "R128": 10,
    },
    "ATP Finals": {
        "W_max": 1500,
        "F_max": 1000,
        "RR_win": 200,
        "RR_max": 600,
        "SF_win": 400,
        "W_bonus": 900,
    },
    "ATP 1000 (96)": {
        "W": 1000,
        "F": 650,
        "SF": 400,
        "QF": 200,
        "R16": 100,
        "R32": 50,
        "R64": 30,
        "R128": 10,
    },
    "ATP 1000 (56)": {
        "W": 1000,
        "F": 650,
        "SF": 400,
        "QF": 200,
        "R16": 100,
        "R32": 50,
        "R64": 30,
        "R128": 0,
    },
    "ATP 500 (48)": {
        "W": 500,
        "F": 330,
        "SF": 200,
        "QF": 100,
        "R16": 50,
        "R32": 25,
    },
    "ATP 500 (32)": {
        "W": 500,
        "F": 330,
        "SF": 200,
        "QF": 100,
        "R16": 50,
        "R32": 0,
    },
    "ATP 250 (48)": {
        "W": 250,
        "F": 165,
        "SF": 100,
        "QF": 50,
        "R16": 25,
        "R32": 13,
    },
    "ATP 250 (32)": {
        "W": 250,
        "F": 165,
        "SF": 100,
        "QF": 50,
        "R16": 25,
        "R32": 0,
    },
    "Challenger 175": {
        "W": 175,
        "F": 90,
        "SF": 50,
        "QF": 25,
        "R16": 13,
        "R32": 0,
    },
    "Challenger 125": {
        "W": 125,
        "F": 64,
        "SF": 35,
        "QF": 16,
        "R16": 8,
        "R32": 0,
    },
    "Challenger 100": {
        "W": 100,
        "F": 50,
        "SF": 25,
        "QF": 14,
        "R16": 7,
        "R32": 0,
    },
    "Challenger 75": {
        "W": 75,
        "F": 44,
        "SF": 22,
        "QF": 12,
        "R16": 6,
        "R32": 0,
    },
    "Challenger 50": {
        "W": 50,
        "F": 25,
        "SF": 14,
        "QF": 8,
        "R16": 4,
        "R32": 0,
    },
    "Future M25": {
        "W": 25,
        "F": 16,
        "SF": 8,
        "QF": 3,
        "R16": 1,
        "R32": 0,
    },
    "Future M15": {
        "W": 15,
        "F": 8,
        "SF": 4,
        "QF": 2,
        "R16": 1,
        "R32": 0,
    },
    "Grand Slam (WTA)": {
        "W": 2000,
        "F": 1300,
        "SF": 780,
        "QF": 430,
        "R16": 240,
        "R32": 130,
        "R64": 70,
        "R128": 10,
    },
    "WTA Finals": {
        "W_max": 1500,
        "F_max": 1000,
        "SF_max": 600,
        "RR_win": 200,
    },
    "WTA 1000 (96)": {
        "W": 1000,
        "F": 650,
        "SF": 390,
        "QF": 215,
        "R16": 120,
        "R32": 65,
        "R64": 35,
        "R128": 10,
    },
    "WTA 1000 (56)": {
        "W": 1000,
        "F": 650,
        "SF": 390,
        "QF": 215,
        "R16": 120,
        "R32": 65,
        "R64": 10,
    },
    "WTA 500 (48)": {
        "W": 500,
        "F": 325,
        "SF": 195,
        "QF": 108,
        "R16": 60,
        "R32": 32,
        "R64": 1,
    },
    "WTA 500 (30)": {
        "W": 500,
        "F": 325,
        "SF": 195,
        "QF": 108,
        "R16": 60,
        "R32": 1,
    },
    "WTA 500 (28)": {
        "W": 500,
        "F": 325,
        "SF": 195,
        "QF": 108,
        "R16": 60,
        "R32": 1,
    },
    "WTA 250 (32)": {
        "W": 250,
        "F": 163,
        "SF": 98,
        "QF": 54,
        "R16": 30,
        "R32": 1,
    },
    "WTA 125 (32)": {
        "W": 125,
        "F": 81,
        "SF": 49,
        "QF": 27,
        "R16": 15,
        "R32": 1,
    },
    "W100 (48)": {
        "W": 100,
        "F": 65,
        "SF": 39,
        "QF": 21,
        "R16": 12,
        "R32": 7,
        "R64": 1,
    },
    "W100 (32)": {
        "W": 100,
        "F": 65,
        "SF": 39,
        "QF": 21,
        "R16": 12,
        "R32": 1,
    },
    "W75 (48)": {
        "W": 75,
        "F": 49,
        "SF": 29,
        "QF": 16,
        "R16": 9,
        "R32": 5,
        "R64": 1,
    },
    "W75 (32)": {
        "W": 75,
        "F": 49,
        "SF": 29,
        "QF": 16,
        "R16": 9,
        "R32": 1,
    },
    "W50 (48)": {
        "W": 50,
        "F": 33,
        "SF": 20,
        "QF": 11,
        "R16": 6,
        "R32": 3,
        "R64": 1,
    },
    "W50 (32)": {
        "W": 50,
        "F": 33,
        "SF": 20,
        "QF": 11,
        "R16": 6,
        "R32": 1,
    },
    "W35 (48)": {
        "W": 35,
        "F": 23,
        "SF": 14,
        "QF": 8,
        "R16": 4,
        "R32": 2,
        "R64": 1,
    },
    "W35 (32)": {
        "W": 35,
        "F": 23,
        "SF": 14,
        "QF": 8,
        "R16": 4,
        "R32": 1,
    },
    "W15 (32)": {
        "W": 15,
        "F": 10,
        "SF": 6,
        "QF": 3,
        "R16": 1,
    },
}


# -----------------------------------------------------------------------------
# Helpers
# -----------------------------------------------------------------------------


def as_float(value: Any) -> Optional[float]:
    if value is None:
        return None
    if isinstance(value, float) and math.isnan(value):
        return None
    if isinstance(value, (int, float)):
        return float(value)
    text = str(value).strip()
    if not text or text.lower() in {"nan", "none", "null"}:
        return None
    text = text.replace(",", ".")
    try:
        return float(text)
    except ValueError:
        return None

def safe_str(value: Any, default: str = "") -> str:
    """Convert values to a clean string while tolerating pandas.NA / NaN."""
    if value is None:
        return default
    try:
        if pd.isna(value):
            return default
    except Exception:
        pass
    s = str(value).strip()
    if s.lower() in {"nan", "none", "null", "<na>"}:
        return default
    return s


def safe_id(value: Any) -> str:
    """Normalize player identifiers coming from CSV/JSON.

    Handles numbers, numeric strings, and float-like strings such as '325088.0'.
    """
    s = safe_str(value)
    if not s:
        return ""
    if re.fullmatch(r"\d+\.0+", s):
        s = s.split(".", 1)[0]
    return s


def normalize_name_key(value: Any) -> str:
    """Normalize a player name for robust matching.

    Removes accents, punctuation, and repeated spaces.
    """
    s = safe_str(value).lower()
    if not s:
        return ""
    s = unicodedata.normalize("NFKD", s)
    s = "".join(ch for ch in s if not unicodedata.combining(ch))
    s = re.sub(r"[^a-z0-9]+", " ", s)
    return re.sub(r"\s+", " ", s).strip()


def as_int(value: Any) -> Optional[int]:
    f = as_float(value)
    if f is None:
        return None
    return int(round(f))


def first_existing(row: pd.Series, candidates: Iterable[str]) -> Any:
    for candidate in candidates:
        if candidate in row.index:
            value = row[candidate]
            if pd.notna(value):
                return value
    return None


def parse_date(value: Any) -> Optional[date]:
    if value is None or (isinstance(value, float) and math.isnan(value)):
        return None
    try:
        ts = pd.to_datetime(value, errors="coerce")
    except Exception:
        return None
    if pd.isna(ts):
        return None
    return ts.date()


def parse_duration_hours(value: Any) -> Optional[float]:
    if value is None or (isinstance(value, float) and math.isnan(value)):
        return None
    text = str(value).strip()
    if not text:
        return None
    # Accept HH:MM:SS, MM:SS or raw seconds.
    parts = text.split(":")
    try:
        if len(parts) == 3:
            h, m, s = map(float, parts)
            return h + m / 60.0 + s / 3600.0
        if len(parts) == 2:
            m, s = map(float, parts)
            return m / 60.0 + s / 3600.0
        return float(text) / 3600.0
    except ValueError:
        return None


def round_order(round_code: Any) -> int:
    if round_code is None:
        return 999
    text = str(round_code).strip().upper()
    if not text:
        return 999
    if text in ROUND_ORDER:
        return ROUND_ORDER[text]
    m = re.search(r"(\d+)", text)
    if m:
        return int(m.group(1))
    return 999


def stage_label_from_order(order: int) -> str:
    if order <= 1:
        return "F"
    if order == 2:
        return "SF"
    if order == 3:
        return "QF"
    if order == 4:
        return "R16"
    if order == 5:
        return "R32"
    if order == 6:
        return "R64"
    if order == 7:
        return "R128"
    return f"R{2 ** (order - 1)}"


def significance_threshold(rank: Optional[int]) -> int:
    if rank is None or rank <= 0:
        return 0
    for max_rank, threshold in SIGNIFICANCE_THRESHOLDS:
        if rank <= max_rank:
            return threshold
    return 100


def is_significant_upset(player_rank: Optional[int], opponent_rank: Optional[int]) -> bool:
    if not player_rank or not opponent_rank:
        return False
    if opponent_rank >= player_rank:
        return False
    gap = player_rank - opponent_rank
    return gap >= significance_threshold(player_rank)


def is_significant_evolution(rank: Optional[int], delta: Optional[int]) -> bool:
    if rank is None or delta is None:
        return False
    return abs(delta) >= significance_threshold(rank)


def is_very_significant_evolution(rank: Optional[int], delta: Optional[int]) -> bool:
    if rank is None or delta is None:
        return False
    return abs(delta) >= 2 * significance_threshold(rank)


def normalize_country_code(value: Any) -> Optional[str]:
    if value is None:
        return None
    try:
        if pd.isna(value):
            return None
    except Exception:
        pass
    text = str(value).strip().upper()
    if not text or text in {"NAN", "NONE", "NULL", "<NA>"}:
        return None
    return text


def ranking_country_key(record: dict[str, Any]) -> Optional[str]:
    # Ranking feeds usually store the 3-letter code in country_name and the
    # 2-letter ISO code in country_code.
    for key in ("country_name", "country_code"):
        v = normalize_country_code(record.get(key))
        if v:
            return v
    return None


def find_ranking_file(candidates: list[str]) -> Optional[Path]:
    for candidate in candidates:
        path = Path(candidate)
        if path.exists():
            return path
    return None


def load_rankings(path: Path) -> tuple[dict[str, dict[str, Any]], dict[str, dict[str, Any]]]:
    with path.open("r", encoding="utf-8") as f:
        data = json.load(f)
    by_id: dict[str, dict[str, Any]] = {}
    by_name: dict[str, dict[str, Any]] = {}
    for record in data:
        if not isinstance(record, dict):
            continue
        pid = safe_id(record.get("player_id"))
        if pid:
            by_id[pid] = record
        name = str(record.get("full_name") or "").strip().lower()
        if name:
            by_name[name] = record
    return by_id, by_name


def resolve_ranking_record(
    player_id: str,
    player_name: str,
    ranking_by_id: dict[str, dict[str, Any]],
    ranking_by_name: dict[str, dict[str, Any]],
) -> Optional[dict[str, Any]]:
    """Resolve a ranking record by id or by name.

    ATP CSV files often contain abbreviated names like 'T. Griekspoor', so we
    first try exact matches, then a surname/initial fallback against the ranking
    full names.
    """
    if player_id and player_id in ranking_by_id:
        return ranking_by_id[player_id]

    name_key = normalize_name_key(player_name)
    if not name_key:
        return None

    # Exact normalized match.
    for full_name, record in ranking_by_name.items():
        if normalize_name_key(full_name) == name_key:
            return record

    # Abbreviation fallback: 'T Griekspoor' -> 'Tallon Griekspoor'
    parts = name_key.split()
    if len(parts) >= 2:
        surname = parts[-1]
        initial = parts[0][0] if parts[0] else ""
        for full_name, record in ranking_by_name.items():
            full_parts = normalize_name_key(full_name).split()
            if len(full_parts) < 2:
                continue
            if full_parts[-1] != surname:
                continue
            if initial and full_parts[0][0] != initial:
                continue
            return record

    # Last-resort fallback on unique surname.
    if len(parts) >= 2:
        surname = parts[-1]
        matches = []
        for full_name, record in ranking_by_name.items():
            full_parts = normalize_name_key(full_name).split()
            if full_parts and full_parts[-1] == surname:
                matches.append(record)
        if len(matches) == 1:
            return matches[0]

    return None


def locate_csv_files(directory: Path, year: int) -> list[Path]:
    if not directory.exists():
        return []
    all_csvs = sorted(directory.rglob("*.csv"))
    files = [path for path in all_csvs if re.search(rf"(^|[^0-9]){year}([^0-9]|$)", path.name)]
    if files:
        return files
    # Fallback for repositories where the season year is not embedded in the filename.
    return all_csvs


def normalize_event_level(circuit: str, level_raw: Any, tourney_name: Any, draw_size: Any) -> Optional[str]:
    circuit = circuit.upper().strip()
    level = " ".join(
        str(x).strip().upper() for x in [level_raw, tourney_name] if x is not None and str(x).strip()
    )
    draw = as_int(draw_size) or 0

    # Finals.
    if "FINALS" in level:
        return f"{circuit} Finals"

    # Grand Slams.
    if "GRAND SLAM" in level or "AUSTRALIAN OPEN" in level or "ROLAND GARROS" in level or "WIMBLEDON" in level or "US OPEN" in level:
        return f"Grand Slam ({circuit})"

    # ATP / WTA 1000.
    if re.search(r"\b1000\b", level):
        if circuit == "ATP":
            return "ATP 1000 (96)" if draw >= 80 else "ATP 1000 (56)"
        if circuit == "WTA":
            return "WTA 1000 (96)" if draw >= 80 else "WTA 1000 (56)"

    # ATP 500 / 250.
    if re.search(r"\b500\b", level):
        if circuit == "ATP":
            return "ATP 500 (48)" if draw >= 40 else "ATP 500 (32)"
        if circuit == "WTA":
            return "WTA 500 (48)" if draw >= 40 else ("WTA 500 (30)" if draw >= 30 else "WTA 500 (28)")

    if re.search(r"\b250\b", level):
        if circuit == "ATP":
            return "ATP 250 (48)" if draw >= 40 else "ATP 250 (32)"
        if circuit == "WTA":
            return "WTA 250 (32)"

    # Challenger and ITF/Futures.
    if "CHALLENGER" in level:
        # Attempt to infer the tier from either the explicit level string or the tournament name.
        for tier in ("175", "125", "100", "75", "50"):
            if tier in level:
                return f"Challenger {tier}"
        return "Challenger 125"

    if "FUTURE" in level or re.search(r"\bM25\b", level):
        return "Future M25"
    if re.search(r"\bM15\b", level):
        return "Future M15"

    # WTA ITF tiers.
    if re.search(r"\bW100\b", level):
        return "W100 (48)" if draw >= 40 else "W100 (32)"
    if re.search(r"\bW75\b", level):
        return "W75 (48)" if draw >= 40 else "W75 (32)"
    if re.search(r"\bW50\b", level):
        return "W50 (48)" if draw >= 40 else "W50 (32)"
    if re.search(r"\bW35\b", level):
        return "W35 (48)" if draw >= 40 else "W35 (32)"
    if re.search(r"\bW15\b", level):
        return "W15 (32)"
    if re.search(r"\bW125\b", level) or re.search(r"\b125\b", level):
        return "WTA 125 (32)"

    return None


def level_points(level_key: Optional[str], round_label: str, won: bool) -> int:
    if not level_key or level_key not in POINTS_TABLE:
        return 0
    table = POINTS_TABLE[level_key]
    round_label = round_label.upper()

    if level_key in {"ATP Finals", "WTA Finals"}:
        # Best-effort handling for finals. The feed structure can vary a lot.
        if round_label in {"W", "F", "SF", "RR"}:
            if won and "W_max" in table:
                return int(table.get("W_max", 0))
            if not won and "F_max" in table:
                return int(table.get("F_max", 0))
            return int(table.get("RR_win", 0) if won else table.get("RR_max", 0))
        return 0

    if won and round_label == "F":
        return int(table.get("W", 0))
    if won and round_label == "RR":
        return int(table.get("RR_win", 0))
    if not won and round_label == "RR":
        return 0

    return int(table.get(round_label, 0))


@dataclass
class Appearance:
    circuit: str
    period: str
    country: str
    player_id: str
    player_name: str
    ranking: Optional[int]
    ranked_last_week: Optional[bool]
    ranked_last_year: Optional[bool]
    ranked_beginning_year: Optional[bool]
    ever_ranked: Optional[bool]
    evolution: Optional[int]
    evolution_year: Optional[int]
    evolution_this_year: Optional[int]
    match_id: str
    match_date: Optional[date]
    tourney_name: str
    level_key: Optional[str]
    round_code: str
    round_order: int
    won: bool
    opponent_id: str
    opponent_name: str
    opponent_ranking: Optional[int]
    opponent_country: str
    is_significant_win: bool
    is_significant_loss: bool
    tournament_points_estimated: int
    aces: Optional[float]
    aces_per_service_point: Optional[float]
    double_faults: Optional[float]
    double_faults_per_service_point: Optional[float]
    first_serve_pct: Optional[float]
    first_serve_points_won_pct: Optional[float]
    second_serve_points_won_pct: Optional[float]
    service_points_won_pct: Optional[float]
    return_points_won_pct: Optional[float]
    breakpoints_faced: Optional[float]
    breakpoints_converted_count: Optional[float]
    breakpoints_converted_rate: Optional[float]
    service_games_played: Optional[float]
    service_games_lost_rate: Optional[float]
    first_serve_in: Optional[float]
    first_serve_attempts: Optional[float]
    first_serve_points_won: Optional[float]
    first_serve_points_attempts: Optional[float]
    second_serve_points_won: Optional[float]
    second_serve_points_attempts: Optional[float]
    service_points_won_count: Optional[float]
    service_points_played: Optional[float]
    return_points_won_count: Optional[float]
    return_points_played: Optional[float]
    tie_breaks_played: Optional[float]
    tie_breaks_won: Optional[float]
    tie_breaks_win_rate: Optional[float]
    match_time_hours: Optional[float]


# -----------------------------------------------------------------------------
# CSV normalization
# -----------------------------------------------------------------------------


def read_matches_csv(path: Path, circuit: str) -> pd.DataFrame:
    df = pd.read_csv(path, low_memory=False)
    df.columns = [c.strip() for c in df.columns]
    df["__source_file__"] = path.name
    df["__circuit__"] = circuit
    return df


def standardize_match_columns(df: pd.DataFrame, circuit: str) -> pd.DataFrame:
    circuit = circuit.upper().strip()
    out = df.copy()

    # Unified columns.
    if circuit == "ATP":
        mapping = {
            "tourney_name": "tourney_name",
            "level": "level",
            "start_date": "start_date",
            "end_date": "end_date",
            "singles_draw_size": "singles_draw_size",
            "match_id": "match_id",
            "round": "round",
            "match_time_total": "match_time_total",
            "winner_player_name": "winner_player_name",
            "loser_player_name": "loser_player_name",
            "player_id_winner": "winner_id",
            "player_id_loser": "loser_id",
            "country_winner": "winner_country",
            "country_loser": "loser_country",
            "winner_seed": "winner_seed",
            "loser_seed": "loser_seed",
            "match_date": "match_date",
        }
    else:
        mapping = {
            "tourney_name": "tourney_name",
            "level": "level",
            "start_date": "start_date",
            "end_date": "end_date",
            "singles_draw_size": "singles_draw_size",
            "match_id": "match_id",
            "round": "round",
            "match_time_total": "match_time_total",
            "winner_player_name": "winner_player_name",
            "loser_player_name": "loser_player_name",
            "player_id_winner": "winner_id",
            "player_id_loser": "loser_id",
            "country_winner": "winner_country",
            "country_loser": "loser_country",
            "winner_seed": "winner_seed",
            "loser_seed": "loser_seed",
            "date": "match_date",
        }

    for src, dst in mapping.items():
        if src in out.columns and dst != src:
            out[dst] = out[src]
        elif src not in out.columns and dst not in out.columns:
            out[dst] = None

    # Backfill when ATP/WTA have alternative naming patterns.
    if "match_date" not in out.columns:
        if "date" in out.columns:
            out["match_date"] = out["date"]
        elif "match_timestamp" in out.columns:
            out["match_date"] = out["match_timestamp"].astype(str).str.slice(0, 10)

    # Keep datetimes (not python date objects) so min/max and comparisons remain stable
    # even when some rows are missing dates.
    out["match_date"] = pd.to_datetime(out["match_date"], errors="coerce", utc=True).dt.tz_convert(None).dt.normalize()
    if "start_date" in out.columns:
        out["start_date"] = pd.to_datetime(out.get("start_date"), errors="coerce", utc=True).dt.tz_convert(None).dt.normalize()
    else:
        out["start_date"] = pd.NaT
    if "end_date" in out.columns:
        out["end_date"] = pd.to_datetime(out.get("end_date"), errors="coerce", utc=True).dt.tz_convert(None).dt.normalize()
    else:
        out["end_date"] = pd.NaT
    out["round"] = out["round"].astype(str).str.upper()
    out["match_id"] = out["match_id"].astype(str)
    out["winner_id"] = out.get("winner_id", pd.Series([None] * len(out)))
    out["loser_id"] = out.get("loser_id", pd.Series([None] * len(out)))
    out["winner_country"] = out.get("winner_country", pd.Series([None] * len(out)))
    out["loser_country"] = out.get("loser_country", pd.Series([None] * len(out)))
    out["winner_player_name"] = out.get("winner_player_name", pd.Series([None] * len(out)))
    out["loser_player_name"] = out.get("loser_player_name", pd.Series([None] * len(out)))
    out["singles_draw_size"] = pd.to_numeric(out.get("singles_draw_size"), errors="coerce")
    out["__circuit__"] = circuit
    return out


# -----------------------------------------------------------------------------
# Appearance creation
# -----------------------------------------------------------------------------


def player_record_from_row(
    row: pd.Series,
    side: str,
    circuit: str,
    period: str,
    ranking_by_id: dict[str, dict[str, Any]],
    ranking_by_name: dict[str, dict[str, Any]],
) -> Appearance:
    side = side.lower()
    opponent_side = "loser" if side == "winner" else "winner"

    player_id = safe_id(row.get(f"{side}_id"))
    player_name = safe_str(row.get(f"{side}_player_name"))
    country = normalize_country_code(row.get(f"{side}_country")) or ""
    opponent_id = safe_id(row.get(f"{opponent_side}_id"))
    opponent_name = safe_str(row.get(f"{opponent_side}_player_name"))
    opponent_country = normalize_country_code(row.get(f"{opponent_side}_country")) or ""
    won = side == "winner"

    rank_record = resolve_ranking_record(player_id, player_name, ranking_by_id, ranking_by_name)

    opp_rank_record = resolve_ranking_record(opponent_id, opponent_name, ranking_by_id, ranking_by_name)

    ranking = as_int(rank_record.get("ranking")) if rank_record else None
    opp_ranking = as_int(opp_rank_record.get("ranking")) if opp_rank_record else None

    evolution = as_int(rank_record.get("evolution")) if rank_record else None
    evolution_year = as_int(rank_record.get("evolution_year")) if rank_record else None
    evolution_this_year = as_int(rank_record.get("evolution_this_year")) if rank_record else None

    def _bool_or_none(record: Optional[dict[str, Any]], field: str) -> Optional[bool]:
        if record is None:
            return None
        value = record.get(field)
        if value is None or pd.isna(value):
            return None
        return bool(value)

    ranked_last_week = _bool_or_none(rank_record, "ranked_last_week")
    ranked_last_year = _bool_or_none(rank_record, "ranked_last_year")
    ranked_beginning_year = _bool_or_none(rank_record, "ranked_beginning_year")
    ever_ranked = _bool_or_none(rank_record, "ever_ranked")

    level_key = normalize_event_level(circuit, row.get("level"), row.get("tourney_name"), row.get("singles_draw_size"))
    round_code = safe_str(row.get("round")).upper()
    order = round_order(round_code)

    significant_win = is_significant_upset(ranking, opp_ranking) if won else False
    significant_loss = is_significant_upset(opp_ranking, ranking) if not won else False

    # Best-effort tournament point estimate from the deepest observed round.
    # The formula uses the player's deepest observed appearance in the available
    # CSVs, which is suitable for weekly snapshots and year-to-date rollups.
    estimated_round_label = stage_label_from_order(order if not won else max(order - 1, 1)) if order < 999 else ""
    tournament_points = level_points(level_key, estimated_round_label, won and order == 1)
    if level_key in {"ATP Finals", "WTA Finals"}:
        tournament_points = level_points(level_key, estimated_round_label or round_code, won)

    # Numeric stats; use the side-specific columns when available.
    aces = as_float(first_existing(row, [f"aces_tot_{side}", f"{side}_aces", f"winner_aces" if side == "winner" else "loser_aces"]))
    double_faults = as_float(first_existing(row, [f"doublefaults_tot_{side}", f"double_faults_{side}", f"winner_dblflt" if side == "winner" else "loser_dblflt"]))
    first_serve_pct = as_float(first_existing(row, [f"firstserve_percent_tot_{side}", f"firstserve_percent_{side}"]))
    first_serve_points_won_pct = as_float(first_existing(row, [f"firstservepointswon_percent_tot_{side}", f"firstservepointswon_percent_{side}"]))
    second_serve_points_won_pct = as_float(first_existing(row, [f"secondservepointswon_percent_tot_{side}", f"secondservepointswon_percent_{side}"]))
    service_points_won_pct = as_float(first_existing(row, [f"totalservicepointswon_percent_tot_{side}", f"totalservicepointswon_percent_{side}"]))
    return_points_won_pct = as_float(first_existing(row, [f"totalreturnpointswon_percent_tot_{side}", f"totalreturnpointswon_percent_{side}"]))
    match_time_hours = parse_duration_hours(first_existing(row, ["match_time_total", "match_time", f"settime_tot_{side}"]))

    first_serve_in = as_float(first_existing(row, [f"firstserve_dividend_tot_{side}"]))
    first_serve_attempts = as_float(first_existing(row, [f"firstserve_divisor_tot_{side}"]))
    first_serve_points_won = as_float(first_existing(row, [f"firstservepointswon_dividend_tot_{side}"]))
    first_serve_points_attempts = as_float(first_existing(row, [f"firstservepointswon_divisor_tot_{side}"]))
    second_serve_points_won = as_float(first_existing(row, [f"secondservepointswon_dividend_tot_{side}"]))
    second_serve_points_attempts = as_float(first_existing(row, [f"secondservepointswon_divisor_tot_{side}"]))
    service_points_won_count = as_float(first_existing(row, [f"totalservicepointswon_dividend_tot_{side}"]))
    service_points_played = as_float(first_existing(row, [f"totalservicepointswon_divisor_tot_{side}"]))
    return_points_won_count = as_float(first_existing(row, [f"totalreturnpointswon_dividend_tot_{side}"]))
    return_points_played = as_float(first_existing(row, [f"totalreturnpointswon_divisor_tot_{side}"]))

    # Fallback reconstructions when some denominator columns are missing.
    if service_points_played is None and service_points_won_count is not None and service_points_won_pct not in (None, 0):
        service_points_played = service_points_won_count / (service_points_won_pct / 100.0)
    if first_serve_attempts is None and first_serve_in is not None and first_serve_pct not in (None, 0):
        first_serve_attempts = first_serve_in / (first_serve_pct / 100.0)
    if first_serve_points_attempts is None and first_serve_points_won is not None and first_serve_points_won_pct not in (None, 0):
        first_serve_points_attempts = first_serve_points_won / (first_serve_points_won_pct / 100.0)
    if second_serve_points_attempts is None and second_serve_points_won is not None and second_serve_points_won_pct not in (None, 0):
        second_serve_points_attempts = second_serve_points_won / (second_serve_points_won_pct / 100.0)
    if return_points_played is None and return_points_won_count is not None and return_points_won_pct not in (None, 0):
        return_points_played = return_points_won_count / (return_points_won_pct / 100.0)

    if service_points_played and service_points_played > 0:
        aces_per_service_point = (aces or 0.0) / service_points_played if aces is not None else None
        double_faults_per_service_point = (double_faults or 0.0) / service_points_played if double_faults is not None else None
    else:
        aces_per_service_point = None
        double_faults_per_service_point = None

    # Breakpoint stats.
    bp_faced = as_float(first_existing(row, [f"breakpointssaved_divisor_tot_{side}", f"breakpoints_faced_{side}"]))
    bp_saved = as_float(first_existing(row, [f"breakpointssaved_dividend_tot_{side}", f"breakpoints_saved_{side}"]))
    if bp_faced is not None and bp_faced > 0 and bp_saved is not None:
        bp_converted = max(bp_faced - bp_saved, 0.0)
        bp_converted_rate = bp_converted / bp_faced
    else:
        bp_converted = None
        bp_converted_rate = None

    # Approximation: service games lost rate is estimated from breakpoints converted
    # over service games played, which is a stable proxy in the absence of a direct
    # service-games-lost column in the provided CSVs.
    service_games_played = as_float(first_existing(row, [f"servicegamesplayed_tot_{side}", f"servicegamesplayed_{side}"]))
    if bp_converted is not None and service_games_played and service_games_played > 0:
        service_games_lost_rate = bp_converted / service_games_played
    else:
        service_games_lost_rate = None

    # Tie-breaks: best-effort based on the existence of a tie-break score in the row.
    tiebreak_cols = [c for c in row.index if c.startswith("tiebreak_set")]
    tie_breaks_played = 0.0
    tie_breaks_won = 0.0
    for c in tiebreak_cols:
        v = row[c]
        if pd.notna(v) and str(v).strip() not in {"", "nan", "none", "null"}:
            # Any tie-break marker means one tie-break was played.
            if c.endswith("_winner"):
                # only count once per set when processing the winner column.
                if re.search(r"_winner$", c):
                    tie_breaks_played += 1.0
                    if won:
                        tie_breaks_won += 1.0
    tie_breaks_win_rate = tie_breaks_won / tie_breaks_played if tie_breaks_played > 0 else None

    return Appearance(
        circuit=circuit,
        period=period,
        country=country,
        player_id=player_id,
        player_name=player_name,
        ranking=ranking,
        ranked_last_week=ranked_last_week,
        ranked_last_year=ranked_last_year,
        ranked_beginning_year=ranked_beginning_year,
        ever_ranked=ever_ranked,
        evolution=evolution,
        evolution_year=evolution_year,
        evolution_this_year=evolution_this_year,
        match_id=safe_str(row.get("match_id")),
        match_date=row.get("match_date"),
        tourney_name=safe_str(row.get("tourney_name")),
        level_key=level_key,
        round_code=round_code,
        round_order=order,
        won=won,
        opponent_id=opponent_id,
        opponent_name=opponent_name,
        opponent_ranking=opp_ranking,
        opponent_country=opponent_country,
        is_significant_win=significant_win,
        is_significant_loss=significant_loss,
        tournament_points_estimated=tournament_points,
        aces=aces,
        aces_per_service_point=aces_per_service_point,
        double_faults=double_faults,
        double_faults_per_service_point=double_faults_per_service_point,
        first_serve_pct=first_serve_pct,
        first_serve_points_won_pct=first_serve_points_won_pct,
        second_serve_points_won_pct=second_serve_points_won_pct,
        service_points_won_pct=service_points_won_pct,
        return_points_won_pct=return_points_won_pct,
        breakpoints_faced=bp_faced,
        breakpoints_converted_count=bp_converted,
        breakpoints_converted_rate=bp_converted_rate,
        service_games_played=service_games_played,
        service_games_lost_rate=service_games_lost_rate,
        first_serve_in=first_serve_in,
        first_serve_attempts=first_serve_attempts,
        first_serve_points_won=first_serve_points_won,
        first_serve_points_attempts=first_serve_points_attempts,
        second_serve_points_won=second_serve_points_won,
        second_serve_points_attempts=second_serve_points_attempts,
        service_points_won_count=service_points_won_count,
        service_points_played=service_points_played,
        return_points_won_count=return_points_won_count,
        return_points_played=return_points_played,
        tie_breaks_played=tie_breaks_played,
        tie_breaks_won=tie_breaks_won,
        tie_breaks_win_rate=tie_breaks_win_rate,
        match_time_hours=match_time_hours,
    )


def build_appearances(
    matches: pd.DataFrame,
    ranking_by_id: dict[str, dict[str, Any]],
    ranking_by_name: dict[str, dict[str, Any]],
    circuit: str,
    period: str,
) -> list[Appearance]:
    appearances: list[Appearance] = []
    for _, row in matches.iterrows():
        # Skip incomplete dates.
        if row.get("match_date") is None or pd.isna(row.get("match_date")):
            continue
        appearances.append(player_record_from_row(row, "winner", circuit, period, ranking_by_id, ranking_by_name))
        appearances.append(player_record_from_row(row, "loser", circuit, period, ranking_by_id, ranking_by_name))
    return appearances


# -----------------------------------------------------------------------------
# Aggregation
# -----------------------------------------------------------------------------


def safe_mean(values: Iterable[Optional[float]]) -> Optional[float]:
    vals = [v for v in values if v is not None and not (isinstance(v, float) and math.isnan(v))]
    if not vals:
        return None
    return float(sum(vals) / len(vals))


def safe_sum(values: Iterable[Optional[float]]) -> float:
    return float(sum(v for v in values if v is not None and not (isinstance(v, float) and math.isnan(v))))


def aggregate_game_stats(appearances: list[Appearance]) -> dict[str, Any]:
    if not appearances:
        return {}

    def weighted_pct(numerator_getter, denominator_getter):
        nums = []
        dens = []
        for a in appearances:
            n = numerator_getter(a)
            d = denominator_getter(a)
            if n is not None and d is not None and d > 0:
                nums.append(n)
                dens.append(d)
        if not nums or not dens:
            return None
        return float(sum(nums) / sum(dens))

    def count_stat(getter):
        vals = [getter(a) for a in appearances]
        return {"mean": safe_mean(vals), "sum": safe_sum(vals)}

    def pct_or_fallback(numerator_getter, denominator_getter, fallback_getter, scale: float = 100.0):
        value = weighted_pct(numerator_getter, denominator_getter)
        if value is None:
            value = safe_mean([fallback_getter(a) for a in appearances])
        if value is None:
            return {"mean": None}
        return {"mean": scale * value}

    stats = {
        "Number of aces": count_stat(lambda a: a.aces),
        "Aces per service point": pct_or_fallback(
            lambda a: a.aces,
            lambda a: a.service_points_played,
            lambda a: a.aces_per_service_point,
            scale=1.0,
        ),
        "Number of double faults": count_stat(lambda a: a.double_faults),
        "Double faults per service point": pct_or_fallback(
            lambda a: a.double_faults,
            lambda a: a.service_points_played,
            lambda a: a.double_faults_per_service_point,
            scale=1.0,
        ),
        "First serve %": pct_or_fallback(
            lambda a: a.first_serve_in,
            lambda a: a.first_serve_attempts,
            lambda a: a.first_serve_pct / 100.0 if a.first_serve_pct is not None else None,
        ),
        "First serve points won %": pct_or_fallback(
            lambda a: a.first_serve_points_won,
            lambda a: a.first_serve_points_attempts,
            lambda a: a.first_serve_points_won_pct / 100.0 if a.first_serve_points_won_pct is not None else None,
        ),
        "Second serve points won %": pct_or_fallback(
            lambda a: a.second_serve_points_won,
            lambda a: a.second_serve_points_attempts,
            lambda a: a.second_serve_points_won_pct / 100.0 if a.second_serve_points_won_pct is not None else None,
        ),
        "Service points won %": pct_or_fallback(
            lambda a: a.service_points_won_count,
            lambda a: a.service_points_played,
            lambda a: a.service_points_won_pct / 100.0 if a.service_points_won_pct is not None else None,
        ),
        "Return points won %": pct_or_fallback(
            lambda a: a.return_points_won_count,
            lambda a: a.return_points_played,
            lambda a: a.return_points_won_pct / 100.0 if a.return_points_won_pct is not None else None,
        ),
        "Breakpoints faced": count_stat(lambda a: a.breakpoints_faced),
        "Breakpoints converted (count)": count_stat(lambda a: a.breakpoints_converted_count),
        "Breakpoints converted rate": {"mean": 100.0 * safe_mean([a.breakpoints_converted_rate for a in appearances]) if safe_mean([a.breakpoints_converted_rate for a in appearances]) is not None else None},
        "Service games lost rate": {"mean": 100.0 * safe_mean([a.service_games_lost_rate for a in appearances]) if safe_mean([a.service_games_lost_rate for a in appearances]) is not None else None},
        "Tie-breaks win rate": {"mean": 100.0 * safe_mean([a.tie_breaks_win_rate for a in appearances]) if safe_mean([a.tie_breaks_win_rate for a in appearances]) is not None else None},
        "Mean match time (hours)": {"mean": safe_mean([a.match_time_hours for a in appearances])},
    }
    return stats



def group_player_tournaments(appearances: list[Appearance]) -> list[dict[str, Any]]:
    by_key: dict[tuple[str, str], list[Appearance]] = defaultdict(list)
    for a in appearances:
        if not a.player_id:
            continue
        by_key[(a.player_id, a.tourney_name)].append(a)

    rows = []
    for (player_id, tourney_name), items in by_key.items():
        items_sorted = sorted(items, key=lambda x: (x.round_order, x.match_date or date.min))
        deepest = items_sorted[0]
        # Highest round reached is based on the deepest observed match. The winner
        # of a non-final round is assumed to have reached the next stage.
        if deepest.round_order == 999:
            highest_round = deepest.round_code or ""
        else:
            if deepest.won:
                highest_round = "W" if deepest.round_order == 1 else stage_label_from_order(deepest.round_order - 1)
            else:
                highest_round = stage_label_from_order(deepest.round_order)

        points = level_points(deepest.level_key, highest_round, deepest.won and deepest.round_order == 1)
        if deepest.level_key in {"ATP Finals", "WTA Finals"}:
            points = level_points(deepest.level_key, highest_round, deepest.won)

        rows.append(
            {
                "player_id": player_id,
                "player_name": deepest.player_name,
                "country": deepest.country,
                "ranking": deepest.ranking,
                "tourney_name": tourney_name,
                "level_key": deepest.level_key,
                "highest_round": highest_round,
                "highest_round_order": round_order(highest_round),
                "points_estimated": points,
            }
        )
    return rows


def build_country_summary(appearances: list[Appearance], ranking_lookup: dict[str, dict[str, Any]]) -> dict[str, Any]:
    if not appearances:
        return {
            "matches": 0,
            "player_appearances": 0,
            "tourney_name_most_played": None,
            "top_5_players_by_matches": [],
            "top_5_players_by_points": [],
            "tournaments_won": [],
            "significant_wins": [],
            "significant_evolutions": {
                "last_week": [],
                "last_year": [],
                "beginning_year": [],
            },
            "new_players": [],
            "game_stats": {},
            "players_by_ranking": [],
            "performance_index": {"players": []},
            "interesting": {},
        }

    unique_matches = sorted({a.match_id for a in appearances if a.match_id})
    player_appearances = len(appearances)

    tourney_counter = Counter(a.tourney_name for a in appearances if a.tourney_name)
    most_played_tourney = None
    if tourney_counter:
        name, count = tourney_counter.most_common(1)[0]
        most_played_tourney = {"tourney_name": name, "matches": int(count)}

    player_match_counts = Counter((a.player_id, a.player_name, a.ranking) for a in appearances if a.player_id)
    top_5_matches = [
        {
            "player_id": pid,
            "player_name": name,
            "ranking": rank,
            "matches": int(count),
        }
        for (pid, name, rank), count in player_match_counts.most_common(5)
    ]

    player_tournaments = group_player_tournaments(appearances)
    points_counter = Counter()
    player_info = {}
    for row in player_tournaments:
        points_counter[(row["player_id"], row["player_name"], row["ranking"])] += row["points_estimated"]
        player_info[row["player_id"]] = row

    top_5_points = [
        {
            "player_id": pid,
            "player_name": name,
            "ranking": rank,
            "points_estimated": int(points),
        }
        for (pid, name, rank), points in points_counter.most_common(5)
    ]

    tournaments_won = [
        {
            "player_id": row["player_id"],
            "player_name": row["player_name"],
            "ranking": row["ranking"],
            "tourney_name": row["tourney_name"],
            "level_key": row["level_key"],
            "points_estimated": row["points_estimated"],
        }
        for row in player_tournaments
        if row["highest_round"] == "W"
    ]

    significant_wins = [
        {
            "player_id": a.player_id,
            "player_name": a.player_name,
            "player_ranking": a.ranking,
            "opponent_name": a.opponent_name,
            "opponent_ranking": a.opponent_ranking,
            "tourney_name": a.tourney_name,
            "round": a.round_code,
            "match_date": a.match_date.isoformat() if a.match_date else None,
            "rank_gap": (a.ranking - a.opponent_ranking) if a.ranking and a.opponent_ranking else None,
        }
        for a in appearances
        if a.is_significant_win
    ]

    def evolution_rows(attr: str) -> list[dict[str, Any]]:
        rows = []
        seen_players = set()
        for a in appearances:
            if not a.player_id or a.player_id in seen_players:
                continue
            rec = ranking_lookup.get(a.player_id)
            if rec is None:
                rec = next((r for r in ranking_lookup.values() if str(r.get("full_name") or "").lower() == a.player_name.lower()), None)
            if not rec:
                continue
            delta = as_int(rec.get(attr))
            rank = as_int(rec.get("ranking"))
            if delta is None or rank is None:
                continue
            if not is_significant_evolution(rank, delta):
                continue
            rows.append(
                {
                    "player_id": a.player_id,
                    "player_name": a.player_name,
                    "ranking": rank,
                    "evolution": delta,
                    "direction": "rise" if delta * RANK_DIRECTION > 0 else "drop",
                }
            )
            seen_players.add(a.player_id)
        return sorted(rows, key=lambda x: (abs(x["evolution"]), -x["ranking"]), reverse=True)

    significant_evolutions = {
        "last_week": evolution_rows("evolution"),
        "last_year": evolution_rows("evolution_year"),
        "beginning_year": evolution_rows("evolution_this_year"),
    }

    new_players = []
    seen_ids = set()
    for a in appearances:
        if not a.player_id or a.player_id in seen_ids:
            continue
        if (
            a.ever_ranked is False
            and a.ranked_last_week is False
            and a.ranked_last_year is False
            and a.ranked_beginning_year is False
        ):
            new_players.append(
                {
                    "player_id": a.player_id,
                    "player_name": a.player_name,
                    "ranking": a.ranking,
                    "country": a.country,
                }
            )
            seen_ids.add(a.player_id)

    game_stats = aggregate_game_stats(appearances)

    # Player list ordered by ranking.
    players_by_ranking_map: dict[str, dict[str, Any]] = {}
    for row in player_tournaments:
        pid = row["player_id"]
        if not pid:
            continue
        current = players_by_ranking_map.get(pid)
        if current is None or (row["ranking"] is not None and (current["ranking"] is None or row["ranking"] < current["ranking"])):
            players_by_ranking_map[pid] = {
                "player_id": pid,
                "player_name": row["player_name"],
                "ranking": row["ranking"],
                "tournaments": [],
            }
        players_by_ranking_map[pid]["tournaments"].append(
            {
                "tourney_name": row["tourney_name"],
                "level_key": row["level_key"],
                "highest_round": row["highest_round"],
                "points_estimated": row["points_estimated"],
            }
        )
    players_by_ranking = sorted(
        [v for v in players_by_ranking_map.values() if v["ranking"] is not None],
        key=lambda x: x["ranking"],
    )

    # Performance index: normalized score in [0, 100].
    player_appearance_map: dict[str, list[Appearance]] = defaultdict(list)
    for a in appearances:
        if a.player_id:
            player_appearance_map[a.player_id].append(a)

    player_points = defaultdict(int)
    for row in player_tournaments:
        player_points[row["player_id"]] += row["points_estimated"]

    all_players = []
    for pid, items in player_appearance_map.items():
        p = items[0]
        wins = sum(1 for x in items if x.won)
        losses = sum(1 for x in items if not x.won)
        sig_wins = sum(1 for x in items if x.is_significant_win)
        sig_losses = sum(1 for x in items if x.is_significant_loss)
        opp_ranks = [x.opponent_ranking for x in items if x.opponent_ranking is not None]
        avg_opp_rank = float(sum(opp_ranks) / len(opp_ranks)) if opp_ranks else None
        movement = p.evolution if p.evolution is not None else 0
        movement = movement * RANK_DIRECTION
        all_players.append(
            {
                "player_id": pid,
                "player_name": p.player_name,
                "ranking": p.ranking,
                "matches": len(items),
                "wins": wins,
                "losses": losses,
                "win_rate": wins / len(items) if items else 0.0,
                "significant_wins": sig_wins,
                "significant_losses": sig_losses,
                "points_estimated": int(player_points.get(pid, 0)),
                "avg_opponent_rank": avg_opp_rank,
                "movement": movement,
            }
        )

    max_points = max((p["points_estimated"] for p in all_players), default=0) or 1
    max_matches = max((p["matches"] for p in all_players), default=0) or 1
    max_sig = max((abs(p["significant_wins"] - p["significant_losses"]) for p in all_players), default=0) or 1
    max_move = max((abs(p["movement"]) for p in all_players), default=0) or 1
    max_rank = max((p["ranking"] for p in all_players if p["ranking"] is not None), default=1)

    performance_players = []
    for p in all_players:
        # Normalizations.
        points_norm = p["points_estimated"] / max_points
        matches_norm = math.log1p(p["matches"]) / math.log1p(max_matches)
        sig_balance = (p["significant_wins"] - 1.25 * p["significant_losses"])
        sig_norm = (sig_balance + max_sig) / (2 * max_sig) if max_sig > 0 else 0.5
        move_norm = (p["movement"] + max_move) / (2 * max_move) if max_move > 0 else 0.5
        win_rate = p["win_rate"]
        # Reward a stronger average opponent ranking (smaller number is stronger).
        if p["avg_opponent_rank"] is None:
            opp_strength = 0.5
        else:
            opp_strength = 1.0 - min(max(p["avg_opponent_rank"] / max_rank, 0.0), 1.0)

        score = (
            40.0 * win_rate
            + 22.0 * points_norm
            + 15.0 * sig_norm
            + 13.0 * move_norm
            + 10.0 * opp_strength
            + 5.0 * matches_norm
        )
        performance_players.append(
            {
                **p,
                "performance_index": round(score, 2),
            }
        )

    performance_players = sorted(performance_players, key=lambda x: (x["performance_index"], x["ranking"] or 999999), reverse=True)

    interesting = {}
    if appearances:
        longest = max((a for a in appearances if a.match_time_hours is not None), key=lambda x: x.match_time_hours, default=None)
        if longest:
            interesting["longest_match"] = {
                "player_name": longest.player_name,
                "opponent_name": longest.opponent_name,
                "tourney_name": longest.tourney_name,
                "match_time_hours": longest.match_time_hours,
            }
        # Best upset = most negative rank gap among significant wins.
        if significant_wins:
            best_upset = min(significant_wins, key=lambda x: x["rank_gap"] or 0)
            interesting["best_upset"] = best_upset

    return {
        "matches": len(unique_matches),
        "player_appearances": player_appearances,
        "tourney_name_most_played": most_played_tourney,
        "top_5_players_by_matches": top_5_matches,
        "top_5_players_by_points": top_5_points,
        "tournaments_won": tournaments_won,
        "significant_wins": significant_wins,
        "significant_evolutions": significant_evolutions,
        "new_players": new_players,
        "game_stats": game_stats,
        "players_by_ranking": players_by_ranking,
        "performance_index": {
            "players": performance_players,
        },
        "interesting": interesting,
    }


# -----------------------------------------------------------------------------
# Pipeline
# -----------------------------------------------------------------------------


def filter_by_period(df: pd.DataFrame, period: str, today: date) -> pd.DataFrame:
    out = df.copy()
    out = out[out["match_date"].notna()]
    today_ts = pd.Timestamp(today).normalize()
    if period == "last_week":
        start = today_ts - pd.Timedelta(days=7)
        out = out[(out["match_date"] >= start) & (out["match_date"] <= today_ts)]
    elif period == "current_year":
        start = pd.Timestamp(date(today.year, 1, 1))
        out = out[(out["match_date"] >= start) & (out["match_date"] <= today_ts)]
    else:
        raise ValueError(f"Unknown period: {period}")
    return out


def load_and_prepare_circuit(
    circuit: str,
    matches_dir: Path,
    ranking_path: Path,
    year: int,
    today: date,
    debug: bool = False,
) -> tuple[list[Appearance], list[Appearance], dict[str, dict[str, Any]], dict[str, dict[str, Any]]]:
    ranking_by_id, ranking_by_name = load_rankings(ranking_path)

    if debug:
        logging.info("[%s] ranking file: %s", circuit, ranking_path)
        logging.info("[%s] rankings loaded: %d by id / %d by name", circuit, len(ranking_by_id), len(ranking_by_name))

    all_files = locate_csv_files(matches_dir, year)
    if debug:
        logging.info("[%s] match directory: %s", circuit, matches_dir)
        logging.info("[%s] csv files matching %s: %d", circuit, year, len(all_files))
        for p in all_files[:10]:
            logging.info("[%s]   file: %s", circuit, p)
        if len(all_files) > 10:
            logging.info("[%s]   ... +%d more", circuit, len(all_files) - 10)

    if not all_files:
        return [], [], ranking_by_id, ranking_by_name

    frames = []
    for path in all_files:
        try:
            df = read_matches_csv(path, circuit)
            frames.append(df)
            if debug:
                logging.info("[%s] loaded %s -> %d rows, %d cols", circuit, path.name, len(df), len(df.columns))
        except Exception as exc:
            if debug:
                logging.exception("[%s] failed reading %s: %s", circuit, path, exc)
            continue
    if not frames:
        return [], [], ranking_by_id, ranking_by_name

    raw = pd.concat(frames, ignore_index=True)
    raw = standardize_match_columns(raw, circuit)

    if debug and not raw.empty:
        min_date = raw["match_date"].min()
        max_date = raw["match_date"].max()
        logging.info("[%s] raw rows after concat: %d", circuit, len(raw))
        logging.info("[%s] date span: %s -> %s", circuit, min_date, max_date)
        logging.info("[%s] sample columns: %s", circuit, ", ".join(list(raw.columns[:20])))

    last_week_df = filter_by_period(raw, "last_week", today)
    year_df = filter_by_period(raw, "current_year", today)

    if debug:
        logging.info("[%s] last_week rows: %d", circuit, len(last_week_df))
        logging.info("[%s] current_year rows: %d", circuit, len(year_df))
        if not year_df.empty:
            countries_sample = sorted({safe_str(v) for v in pd.concat([year_df["winner_country"], year_df["loser_country"]], ignore_index=True).dropna().unique().tolist()})
            logging.info("[%s] countries in current_year: %s", circuit, countries_sample[:20])

    last_week_appearances = build_appearances(last_week_df, ranking_by_id, ranking_by_name, circuit, "last_week")
    year_appearances = build_appearances(year_df, ranking_by_id, ranking_by_name, circuit, "current_year")

    if debug:
        def coverage(apps):
            if not apps:
                return 0.0
            matched = sum(1 for a in apps if a.ranking is not None)
            return 100.0 * matched / len(apps)
        logging.info("[%s] last_week appearances: %d (ranking coverage %.1f%%)", circuit, len(last_week_appearances), coverage(last_week_appearances))
        logging.info("[%s] current_year appearances: %d (ranking coverage %.1f%%)", circuit, len(year_appearances), coverage(year_appearances))
        if circuit.upper() == "ATP" and coverage(last_week_appearances) == 0.0 and coverage(year_appearances) == 0.0:
            sample_names = []
            if not year_df.empty:
                sample_names = sorted({safe_str(v) for v in pd.concat([year_df["winner_player_name"], year_df["loser_player_name"]], ignore_index=True).dropna().unique().tolist()})[:20]
            logging.info("[%s] sample player names from matches: %s", circuit, sample_names)

    return last_week_appearances, year_appearances, ranking_by_id, ranking_by_name


def build_country_reports(
    atp_last_week: list[Appearance],
    atp_year: list[Appearance],
    wta_last_week: list[Appearance],
    wta_year: list[Appearance],
    atp_rankings: dict[str, dict[str, Any]],
    wta_rankings: dict[str, dict[str, Any]],
    output_dir: Path,
) -> list[Path]:
    countries = {a.country for a in atp_last_week + atp_year + wta_last_week + wta_year if a.country}

    output_dir.mkdir(parents=True, exist_ok=True)
    written: list[Path] = []

    for country in sorted(countries):
        country_report = {
            "country": country,
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "ATP": {
                "last_week": build_country_summary([a for a in atp_last_week if a.country == country], atp_rankings),
                "current_year": build_country_summary([a for a in atp_year if a.country == country], atp_rankings),
            },
            "WTA": {
                "last_week": build_country_summary([a for a in wta_last_week if a.country == country], wta_rankings),
                "current_year": build_country_summary([a for a in wta_year if a.country == country], wta_rankings),
            },
        }
        out_path = output_dir / f"{country}.json"
        out_path.write_text(json.dumps(country_report, ensure_ascii=False, indent=2), encoding="utf-8")
        written.append(out_path)

    return written


def default_paths() -> dict[str, Path]:
    root_candidates = [PROJECT_ROOT, SCRIPT_DIR, Path.cwd()]

    def pick_existing(*relative_parts: str) -> Path:
        for root in root_candidates:
            candidate = root.joinpath(*relative_parts)
            if candidate.exists():
                return candidate
        # Return a sensible default even if it does not exist yet.
        return root_candidates[0].joinpath(*relative_parts)

    def pick_first_existing(candidates: list[Path]) -> Path:
        for candidate in candidates:
            if candidate.exists():
                return candidate
        return candidates[0]

    return {
        "atp_matches": pick_existing("docs", "matches", "atp_matches"),
        "wta_matches": pick_existing("docs", "matches", "wta_matches"),
        "atp_ranking": pick_first_existing([
            pick_existing("docs", "tools", "latest_atp_ranking.json"),
            pick_existing("docs", "Tools", "latest_atp_ranking.json"),
        ]),
        "wta_ranking": pick_first_existing([
            pick_existing("docs", "tools", "latest_wta_ranking.json"),
            pick_existing("docs", "Tools", "latest_wta_ranking.json"),
        ]),
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Build weekly and YTD country reports for ATP/WTA matches.")
    parser.add_argument("--atp-matches-dir", default=str(default_paths()["atp_matches"]))
    parser.add_argument("--wta-matches-dir", default=str(default_paths()["wta_matches"]))
    parser.add_argument("--atp-ranking", default=str(default_paths()["atp_ranking"]))
    parser.add_argument("--wta-ranking", default=str(default_paths()["wta_ranking"]))
    parser.add_argument("--output-dir", default="docs/country_reports")
    parser.add_argument("--year", type=int, default=date.today().year)
    parser.add_argument("--today", default=date.today().isoformat(), help="Reference date in YYYY-MM-DD format.")
    args = parser.parse_args()

    today = pd.to_datetime(args.today).date()
    year = int(args.year)

    atp_last_week, atp_year, atp_rankings = load_and_prepare_circuit(
        "ATP", Path(args.atp_matches_dir), Path(args.atp_ranking), year, today
    )
    wta_last_week, wta_year, wta_rankings = load_and_prepare_circuit(
        "WTA", Path(args.wta_matches_dir), Path(args.wta_ranking), year, today
    )

    written = build_country_reports(
        atp_last_week,
        atp_year,
        wta_last_week,
        wta_year,
        atp_rankings,
        wta_rankings,
        Path(args.output_dir),
    )

    print(json.dumps({"written_files": [str(p) for p in written], "count": len(written)}, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
