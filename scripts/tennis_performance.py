from __future__ import annotations

import csv
import json
import math
import re
from collections import Counter, defaultdict
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple
from zoneinfo import ZoneInfo

PARIS_TZ = ZoneInfo("Europe/Paris")

# ---------------------------------------------------------------------------
# Ranking-point tables provided by the user.
# ---------------------------------------------------------------------------
POINTS_BY_LEVEL: Dict[str, Dict[str, int]] = {
    "Grand Slam (ATP)": {"W": 2000, "F": 1300, "SF": 800, "QF": 400, "R16": 200, "R32": 100, "R64": 50, "R128": 10},
    "ATP Finals": {"W_max": 1500, "F_max": 1000, "RR_win": 200, "RR_max": 600, "SF_win": 400, "W_bonus": 900},
    "ATP 1000 (96)": {"W": 1000, "F": 650, "SF": 400, "QF": 200, "R16": 100, "R32": 50, "R64": 30, "R128": 10},
    "ATP 1000 (56)": {"W": 1000, "F": 650, "SF": 400, "QF": 200, "R16": 100, "R32": 50, "R64": 30, "R128": 0},
    "ATP 500 (48)": {"W": 500, "F": 330, "SF": 200, "QF": 100, "R16": 50, "R32": 25},
    "ATP 500 (32)": {"W": 500, "F": 330, "SF": 200, "QF": 100, "R16": 50, "R32": 0},
    "ATP 250 (48)": {"W": 250, "F": 165, "SF": 100, "QF": 50, "R16": 25, "R32": 13},
    "ATP 250 (32)": {"W": 250, "F": 165, "SF": 100, "QF": 50, "R16": 25, "R32": 0},
    "Challenger 175": {"W": 175, "F": 90, "SF": 50, "QF": 25, "R16": 13, "R32": 0},
    "Challenger 125": {"W": 125, "F": 64, "SF": 35, "QF": 16, "R16": 8, "R32": 0},
    "Challenger 100": {"W": 100, "F": 50, "SF": 25, "QF": 14, "R16": 7, "R32": 0},
    "Challenger 75": {"W": 75, "F": 44, "SF": 22, "QF": 12, "R16": 6, "R32": 0},
    "Challenger 50": {"W": 50, "F": 25, "SF": 14, "QF": 8, "R16": 4, "R32": 0},
    "Future M25": {"W": 25, "F": 16, "SF": 8, "QF": 3, "R16": 1, "R32": 0},
    "Future M15": {"W": 15, "F": 8, "SF": 4, "QF": 2, "R16": 1, "R32": 0},
    "Grand Slam (WTA)": {"W": 2000, "F": 1300, "SF": 780, "QF": 430, "R16": 240, "R32": 130, "R64": 70, "R128": 10},
    "WTA Finals": {"W_max": 1500, "F_max": 1000, "SF_max": 600, "RR_win": 200},
    "WTA 1000 (96)": {"W": 1000, "F": 650, "SF": 390, "QF": 215, "R16": 120, "R32": 65, "R64": 35, "R128": 10},
    "WTA 1000 (56)": {"W": 1000, "F": 650, "SF": 390, "QF": 215, "R16": 120, "R32": 65, "R64": 10},
    "WTA 500 (48)": {"W": 500, "F": 325, "SF": 195, "QF": 108, "R16": 60, "R32": 32, "R64": 1},
    "WTA 500 (30)": {"W": 500, "F": 325, "SF": 195, "QF": 108, "R16": 60, "R32": 1},
    "WTA 500 (28)": {"W": 500, "F": 325, "SF": 195, "QF": 108, "R16": 60, "R32": 1},
    "WTA 250 (32)": {"W": 250, "F": 163, "SF": 98, "QF": 54, "R16": 30, "R32": 1},
    "WTA 125 (32)": {"W": 125, "F": 81, "SF": 49, "QF": 27, "R16": 15, "R32": 1},
    "W100 (48)": {"W": 100, "F": 65, "SF": 39, "QF": 21, "R16": 12, "R32": 7, "R64": 1},
    "W100 (32)": {"W": 100, "F": 65, "SF": 39, "QF": 21, "R16": 12, "R32": 1},
    "W75 (48)": {"W": 75, "F": 49, "SF": 29, "QF": 16, "R16": 9, "R32": 5, "R64": 1},
    "W75 (32)": {"W": 75, "F": 49, "SF": 29, "QF": 16, "R16": 9, "R32": 1},
    "W50 (48)": {"W": 50, "F": 33, "SF": 20, "QF": 11, "R16": 6, "R32": 3, "R64": 1},
    "W50 (32)": {"W": 50, "F": 33, "SF": 20, "QF": 11, "R16": 6, "R32": 1},
    "W35 (48)": {"W": 35, "F": 23, "SF": 14, "QF": 8, "R16": 4, "R32": 2, "R64": 1},
    "W35 (32)": {"W": 35, "F": 23, "SF": 14, "QF": 8, "R16": 4, "R32": 1},
    "W15 (32)": {"W": 15, "F": 10, "SF": 6, "QF": 3, "R16": 1},
}

# ---------------------------------------------------------------------------
# Tuning knobs / heuristics.
# ---------------------------------------------------------------------------

# Convention assumed for the ranking JSON evolution fields:
# positive evolution means an improvement in rank (a rise).
RANK_EVOLUTION_POSITIVE_IS_RISE = True

# Significance thresholds are intentionally stricter at the top of the ranking.
# The numbers are deltas in ranking places.
SIGNIFICANT_WIN_THRESHOLDS = [
    (10, 8),
    (25, 12),
    (50, 18),
    (100, 25),
    (200, 35),
    (500, 50),
    (1000, 80),
]
VERY_SIGNIFICANT_WIN_THRESHOLDS = [
    (10, 15),
    (25, 25),
    (50, 35),
    (100, 50),
    (200, 75),
    (500, 120),
    (1000, 180),
]

# Rank-evolution significance thresholds follow the same general logic.
SIGNIFICANT_RANK_CHANGE_THRESHOLDS = [
    (10, 5),
    (25, 8),
    (50, 12),
    (100, 18),
    (200, 25),
    (500, 40),
    (1000, 60),
]
VERY_SIGNIFICANT_RANK_CHANGE_THRESHOLDS = [
    (10, 10),
    (25, 15),
    (50, 25),
    (100, 35),
    (200, 50),
    (500, 80),
    (1000, 120),
]

ROUND_ORDER_MAP = {
    "F": 1,
    "SF": 2,
    "QF": 3,
    "R16": 4,
    "R32": 5,
    "R64": 6,
    "R128": 7,
    "RR": 8,
    "Q": 5,
    "S": 6,
    "R1": 7,
    "R2": 6,
    "R3": 5,
    "R4": 4,
}


@dataclass
class Participation:
    circuit: str
    period: str
    match_key: str
    event_key: str
    event_year: int
    event_id: str
    tourney_name: str
    level_raw: str
    level_canonical: str
    start_date: Optional[date]
    end_date: Optional[date]
    match_date: Optional[date]
    round_raw: str
    round_order: int
    round_label: str
    surface: str
    draw_size: Optional[int]
    player_id: str
    player_name: str
    opponent_id: str
    opponent_name: str
    country_code: str
    country_name: str
    is_winner: bool
    player_rank: Optional[int]
    opponent_rank: Optional[int]
    points_earned: int
    stats: Dict[str, Optional[float]]
    significant_win: bool = False
    significant_loss: bool = False
    very_significant_win: bool = False
    very_significant_loss: bool = False


@dataclass
class PlayerSummary:
    player_id: str
    player_name: str
    country_code: str
    country_name: str
    circuit: str
    period: str
    ranking: Optional[int]
    ranked_last_week: Optional[bool]
    ranked_last_year: Optional[bool]
    ranked_beginning_year: Optional[bool]
    ever_ranked: Optional[bool]
    matches: int = 0
    wins: int = 0
    losses: int = 0
    unique_tournaments: int = 0
    tournaments: List[Dict[str, Any]] = field(default_factory=list)
    best_round_order: Optional[int] = None
    best_round_label: Optional[str] = None
    points_earned: int = 0
    opponent_ranks: List[int] = field(default_factory=list)
    level_points: List[int] = field(default_factory=list)
    significant_wins: int = 0
    significant_losses: int = 0
    very_significant_wins: int = 0
    very_significant_losses: int = 0
    avg_opponent_rank: Optional[float] = None
    avg_tournament_points: Optional[float] = None
    performance_index_raw: float = 0.0
    performance_index: float = 0.0
    stats_sums: Dict[str, float] = field(default_factory=lambda: defaultdict(float))
    stats_weights: Dict[str, float] = field(default_factory=lambda: defaultdict(float))


# ---------------------------------------------------------------------------
# Generic helpers.
# ---------------------------------------------------------------------------


def now_paris() -> datetime:
    return datetime.now(tz=PARIS_TZ)


def parse_int(value: Any) -> Optional[int]:
    if value is None:
        return None
    if isinstance(value, bool):
        return int(value)
    if isinstance(value, (int, float)):
        if math.isnan(value):  # type: ignore[arg-type]
            return None
        return int(value)
    s = str(value).strip()
    if not s:
        return None
    if s.lower() in {"nan", "none", "null"}:
        return None
    try:
        return int(float(s))
    except Exception:
        return None


def parse_float(value: Any) -> Optional[float]:
    if value is None:
        return None
    if isinstance(value, bool):
        return float(value)
    if isinstance(value, (int, float)):
        if isinstance(value, float) and math.isnan(value):
            return None
        return float(value)
    s = str(value).strip()
    if not s:
        return None
    if s.lower() in {"nan", "none", "null"}:
        return None
    s = s.replace(",", ".")
    try:
        return float(s)
    except Exception:
        return None


def parse_bool(value: Any) -> Optional[bool]:
    if value is None:
        return None
    if isinstance(value, bool):
        return value
    s = str(value).strip().lower()
    if s in {"true", "t", "1", "yes", "y"}:
        return True
    if s in {"false", "f", "0", "no", "n"}:
        return False
    return None


def parse_date(value: Any) -> Optional[date]:
    if value is None:
        return None
    if isinstance(value, date) and not isinstance(value, datetime):
        return value
    s = str(value).strip()
    if not s:
        return None
    try:
        # Handle ISO datetimes too.
        if "T" in s:
            return datetime.fromisoformat(s.replace("Z", "+00:00")).date()
        return date.fromisoformat(s)
    except Exception:
        return None


def clean_str(value: Any, default: str = "") -> str:
    if value is None:
        return default
    s = str(value).strip()
    return s if s else default


def normalize_country(code: Any) -> str:
    s = clean_str(code).upper()
    return s


def safe_filename_slug(code: str) -> str:
    return re.sub(r"[^A-Za-z0-9_.-]+", "_", code.strip())


def json_dump(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as fh:
        json.dump(payload, fh, ensure_ascii=False, indent=2, sort_keys=False)


def weighted_average(values: Sequence[Optional[float]], weights: Sequence[float]) -> Optional[float]:
    total_weight = 0.0
    total_value = 0.0
    for v, w in zip(values, weights):
        if v is None:
            continue
        total_weight += w
        total_value += v * w
    if total_weight == 0:
        return None
    return total_value / total_weight


# ---------------------------------------------------------------------------
# Round, tournament level and significance logic.
# ---------------------------------------------------------------------------


def round_order(round_raw: Any) -> int:
    s = clean_str(round_raw).upper()
    if not s:
        return 999
    if s in ROUND_ORDER_MAP:
        return ROUND_ORDER_MAP[s]
    m = re.search(r"(\d{1,3})", s)
    if m:
        # Lower numbers are deeper rounds, as requested by the user.
        return int(m.group(1))
    return 999


def round_label(round_raw: Any) -> str:
    s = clean_str(round_raw).upper()
    if s in {"F", "SF", "QF", "R16", "R32", "R64", "R128", "RR"}:
        return s
    if s.startswith("MS") or s.startswith("LS"):
        return s
    if s == "S":
        return "SF"
    if s == "Q":
        return "QF"
    return s or "UNK"


def _contains_any(text: str, patterns: Sequence[str]) -> bool:
    t = text.lower()
    return any(p.lower() in t for p in patterns)


def canonical_level(circuit: str, level_raw: Any, tourney_name: Any, draw_size: Any) -> str:
    level = clean_str(level_raw).strip()
    name = clean_str(tourney_name)
    name_l = name.lower()
    level_l = level.lower()
    draw = parse_int(draw_size)

    # Finals first.
    if _contains_any(level_l + " " + name_l, ["atp finals", "wta finals", "finals"]):
        return "ATP Finals" if circuit == "ATP" else "WTA Finals"

    # Grand slams.
    if _contains_any(level_l + " " + name_l, ["grand slam", "australian open", "roland garros", "wimbledon", "us open"]):
        return "Grand Slam (ATP)" if circuit == "ATP" else "Grand Slam (WTA)"

    # ATP / WTA main tour.
    if circuit == "ATP":
        if any(x in level_l for x in ["1000", "masters 1000", "m1000"]):
            return "ATP 1000 (96)" if (draw or 0) >= 96 else "ATP 1000 (56)"
        if any(x in level_l for x in ["500", "atp 500"]):
            return "ATP 500 (48)" if (draw or 0) >= 48 else "ATP 500 (32)"
        if any(x in level_l for x in ["250", "atp 250"]):
            return "ATP 250 (48)" if (draw or 0) >= 48 else "ATP 250 (32)"
        if any(x in level_l for x in ["challenger 175", "175"]):
            return "Challenger 175"
        if any(x in level_l for x in ["challenger 125", "125"]):
            return "Challenger 125"
        if any(x in level_l for x in ["challenger 100", "100"]):
            return "Challenger 100"
        if any(x in level_l for x in ["challenger 75", "75"]):
            return "Challenger 75"
        if any(x in level_l for x in ["challenger 50", "50"]):
            return "Challenger 50"
        if any(x in level_l for x in ["m25", "future m25", "25"]):
            return "Future M25"
        if any(x in level_l for x in ["m15", "future m15", "15"]):
            return "Future M15"
    else:
        if any(x in level_l for x in ["1000", "wta 1000", "premier 5"]):
            return "WTA 1000 (96)" if (draw or 0) >= 96 else "WTA 1000 (56)"
        if any(x in level_l for x in ["500", "wta 500"]):
            if (draw or 0) >= 48:
                return "WTA 500 (48)"
            if (draw or 0) >= 30:
                return "WTA 500 (30)"
            return "WTA 500 (28)"
        if any(x in level_l for x in ["250", "wta 250"]):
            return "WTA 250 (32)"
        if any(x in level_l for x in ["125", "wta 125"]):
            return "WTA 125 (32)"
        if any(x in level_l for x in ["w100"]):
            return "W100 (48)" if (draw or 0) >= 48 else "W100 (32)"
        if any(x in level_l for x in ["w75"]):
            return "W75 (48)" if (draw or 0) >= 48 else "W75 (32)"
        if any(x in level_l for x in ["w50"]):
            return "W50 (48)" if (draw or 0) >= 48 else "W50 (32)"
        if any(x in level_l for x in ["w35"]):
            return "W35 (48)" if (draw or 0) >= 48 else "W35 (32)"
        if any(x in level_l for x in ["w15"]):
            return "W15 (32)"

    # Try the tournament name if level is not explicit.
    if circuit == "ATP":
        if re.search(r"\b1000\b", name_l):
            return "ATP 1000 (96)" if (draw or 0) >= 96 else "ATP 1000 (56)"
        if re.search(r"\b500\b", name_l):
            return "ATP 500 (48)" if (draw or 0) >= 48 else "ATP 500 (32)"
        if re.search(r"\b250\b", name_l):
            return "ATP 250 (48)" if (draw or 0) >= 48 else "ATP 250 (32)"
        if re.search(r"\b175\b", name_l):
            return "Challenger 175"
        if re.search(r"\b125\b", name_l):
            return "Challenger 125"
        if re.search(r"\b100\b", name_l):
            return "Challenger 100"
        if re.search(r"\b75\b", name_l):
            return "Challenger 75"
        if re.search(r"\b50\b", name_l):
            return "Challenger 50"
    else:
        if re.search(r"\bw100\b", name_l):
            return "W100 (48)" if (draw or 0) >= 48 else "W100 (32)"
        if re.search(r"\bw75\b", name_l):
            return "W75 (48)" if (draw or 0) >= 48 else "W75 (32)"
        if re.search(r"\bw50\b", name_l):
            return "W50 (48)" if (draw or 0) >= 48 else "W50 (32)"
        if re.search(r"\bw35\b", name_l):
            return "W35 (48)" if (draw or 0) >= 48 else "W35 (32)"
        if re.search(r"\bw15\b", name_l):
            return "W15 (32)"

    return level if level else "Unknown"


def points_for_level_round(circuit: str, level_canonical: str, round_label_value: str, won_final: bool = False) -> int:
    table = POINTS_BY_LEVEL.get(level_canonical)
    if not table:
        return 0

    # Finals/round-robin events require a bit of special handling.
    if level_canonical in {"ATP Finals", "WTA Finals"}:
        if round_label_value in {"F", "MS001", "LS001"}:
            return table.get("W_max", 0) if won_final else table.get("F_max", 0)
        if round_label_value in {"SF", "MS002", "LS002"}:
            key = "SF_win" if circuit == "ATP" else "SF_max"
            return table.get(key, 0)
        if round_label_value == "RR":
            return table.get("RR_win", 0)
        return 0

    if round_label_value in table:
        return table[round_label_value]

    # Fallback for numeric match codes such as MS008 / LS008.
    m = re.search(r"(\d{1,3})", round_label_value)
    if m:
        n = int(m.group(1))
        # Rough mapping: 001 = final, 002 = semi, 003 = QF, ...
        if n <= 1:
            return table.get("W", 0) if won_final else table.get("F", 0)
        if n == 2:
            return table.get("SF", table.get("SF_max", table.get("SF_win", 0)))
        if n == 3:
            return table.get("QF", 0)
        if n == 4:
            return table.get("R16", 0)
        if n == 5:
            return table.get("R32", 0)
        if n == 6:
            return table.get("R64", 0)
        if n == 7:
            return table.get("R128", 0)
    return 0


def significance_threshold(rank: Optional[int], thresholds: Sequence[Tuple[int, int]]) -> Optional[int]:
    if rank is None:
        return None
    for up_to, delta in thresholds:
        if rank <= up_to:
            return delta
    return thresholds[-1][1] if thresholds else None


def is_significant_win(player_rank: Optional[int], opponent_rank: Optional[int], very: bool = False) -> bool:
    if not player_rank or not opponent_rank:
        return False
    if opponent_rank <= player_rank:
        return False
    gap = opponent_rank - player_rank
    threshold = significance_threshold(player_rank, VERY_SIGNIFICANT_WIN_THRESHOLDS if very else SIGNIFICANT_WIN_THRESHOLDS)
    return bool(threshold is not None and gap >= threshold)


def is_significant_loss(player_rank: Optional[int], opponent_rank: Optional[int], very: bool = False) -> bool:
    if not player_rank or not opponent_rank:
        return False
    if opponent_rank >= player_rank:
        return False
    gap = player_rank - opponent_rank
    threshold = significance_threshold(player_rank, VERY_SIGNIFICANT_WIN_THRESHOLDS if very else SIGNIFICANT_WIN_THRESHOLDS)
    return bool(threshold is not None and gap >= threshold)


def is_significant_evolution(current_rank: Optional[int], evolution: Optional[int], very: bool = False) -> bool:
    if current_rank is None or evolution is None:
        return False
    threshold = significance_threshold(current_rank, VERY_SIGNIFICANT_RANK_CHANGE_THRESHOLDS if very else SIGNIFICANT_RANK_CHANGE_THRESHOLDS)
    return bool(threshold is not None and abs(evolution) >= threshold)


def evolution_direction(evolution: Optional[int]) -> str:
    if evolution is None or evolution == 0:
        return "stable"
    if RANK_EVOLUTION_POSITIVE_IS_RISE:
        return "rise" if evolution > 0 else "drop"
    return "rise" if evolution < 0 else "drop"


# ---------------------------------------------------------------------------
# Ranking loading.
# ---------------------------------------------------------------------------


def load_ranking_json(path: Path) -> Dict[str, Dict[str, Any]]:
    if not path.exists():
        raise FileNotFoundError(f"Ranking file not found: {path}")
    with path.open("r", encoding="utf-8") as fh:
        data = json.load(fh)
    result: Dict[str, Dict[str, Any]] = {}
    for item in data:
        player_id = clean_str(item.get("player_id"))
        if not player_id:
            continue
        result[player_id] = item
    return result


# ---------------------------------------------------------------------------
# CSV loading and normalisation.
# ---------------------------------------------------------------------------


def detect_circuit_from_path(path: Path) -> str:
    p = str(path).lower()
    if "atp" in p and "wta" not in p:
        return "ATP"
    if "wta" in p and "atp" not in p:
        return "WTA"
    # Fallback: inspect headers later if needed.
    return "ATP"


def iter_csv_files(root_dir: Path, circuit: str, year: int) -> List[Path]:
    base = root_dir / "docs" / "matches" / f"{circuit.lower()}_matches"
    if not base.exists():
        return []
    year_str = str(year)
    paths = [p for p in base.rglob("*.csv") if year_str in p.name]
    return sorted(paths)


def find_first(row: Dict[str, Any], candidates: Sequence[str]) -> Any:
    for key in candidates:
        if key in row and str(row[key]).strip() != "":
            return row[key]
    return None


def derive_match_date(row: Dict[str, Any]) -> Optional[date]:
    for key in ["match_date", "date", "start_date"]:
        d = parse_date(row.get(key))
        if d is not None:
            return d
    return None


def derive_event_key(row: Dict[str, Any], source_file: Path) -> str:
    event_id = clean_str(find_first(row, ["event_id", "tourney_id"]))
    event_year = clean_str(find_first(row, ["event_year", "tourney_year"]))
    if event_id:
        return f"{event_id}-{event_year or ''}"
    if clean_str(row.get("tourney_name")):
        return f"{row.get('tourney_name')}::{row.get('start_date')}::{row.get('end_date')}"
    return source_file.stem


def match_key_for_row(row: Dict[str, Any], source_file: Path) -> str:
    match_id = clean_str(row.get("match_id"))
    event_key = derive_event_key(row, source_file)
    if match_id:
        return f"{event_key}::{match_id}"
    # fall back to date + players
    player_a = clean_str(find_first(row, ["player_winner", "winner", "player_a", "winner_player_name"]))
    player_b = clean_str(find_first(row, ["player_loser", "loser", "player_b", "loser_player_name"]))
    mdate = clean_str(find_first(row, ["match_date", "date"]))
    return f"{event_key}::{mdate}::{player_a}::{player_b}"


# ---------------------------------------------------------------------------
# Match row extraction.
# ---------------------------------------------------------------------------


def _side_suffix(circuit: str, side: str) -> str:
    return f"_{side}"


def side_candidates(circuit: str, side: str) -> Dict[str, List[str]]:
    # side is either "winner" / "loser" or "a" / "b" in some WTA files.
    if circuit == "ATP":
        suffix = f"_{side}"
        return {
            "player_name": [f"player_{side}", f"{side}_player_name", f"{side}"],
            "player_id": [f"player_id_{side}", f"PlayerID{side.upper()}", f"playerid_{side}", f"{side}_id"],
            "country": [f"country_{side}", f"{side}_country", f"{side}_country_code"],
            "seed": [f"seed_{side}", f"{side}_seed"],
            "aces": [f"aces_tot_{side}"],
            "doublefaults": [f"doublefaults_tot_{side}"],
            "firstserve_dividend": [f"firstserve_dividend_tot_{side}"],
            "firstserve_divisor": [f"firstserve_divisor_tot_{side}"],
            "firstserve_percent": [f"firstserve_percent_tot_{side}"],
            "firstservepointswon_dividend": [f"firstservepointswon_dividend_tot_{side}"],
            "firstservepointswon_divisor": [f"firstservepointswon_divisor_tot_{side}"],
            "firstservepointswon_percent": [f"firstservepointswon_percent_tot_{side}"],
            "secondservepointswon_dividend": [f"secondservepointswon_dividend_tot_{side}"],
            "secondservepointswon_divisor": [f"secondservepointswon_divisor_tot_{side}"],
            "secondservepointswon_percent": [f"secondservepointswon_percent_tot_{side}"],
            "breakpointsaved_dividend": [f"breakpointssaved_dividend_tot_{side}"],
            "breakpointsaved_divisor": [f"breakpointssaved_divisor_tot_{side}"],
            "breakpointsaved_percent": [f"breakpointssaved_percent_tot_{side}"],
            "servicegamesplayed": [f"servicegamesplayed_tot_{side}"],
            "totalservicepointswon_dividend": [f"totalservicepointswon_dividend_tot_{side}"],
            "totalservicepointswon_divisor": [f"totalservicepointswon_divisor_tot_{side}"],
            "totalservicepointswon_percent": [f"totalservicepointswon_percent_tot_{side}"],
            "totalreturnpointswon_dividend": [f"totalreturnpointswon_dividend_tot_{side}"],
            "totalreturnpointswon_divisor": [f"totalreturnpointswon_divisor_tot_{side}"],
            "totalreturnpointswon_percent": [f"totalreturnpointswon_percent_tot_{side}"],
            "totalpointswon_dividend": [f"totalpointswon_dividend_tot_{side}"],
            "totalpointswon_divisor": [f"totalpointswon_divisor_tot_{side}"],
            "totalpointswon_percent": [f"totalpointswon_percent_tot_{side}"],
            "settime": [f"settime_tot_{side}"],
            "tiebreak1": [f"tiebreak_set1_{side}"],
            "tiebreak2": [f"tiebreak_set2_{side}"],
            "tiebreak3": [f"tiebreak_set3_{side}"],
        }
    # WTA and mixed formats
    if side in {"winner", "loser"}:
        suffix = f"_{side}"
        return {
            "player_name": [f"player_{side}", f"{side}_player_name", side],
            "player_id": [f"player_id_{side}", f"PlayerID{side.upper()}", f"PlayerID{side[0].upper()}", f"playerid_{side}"],
            "country": [f"country_{side}", f"{side}_country", f"{side}_country_code"],
            "seed": [f"seed_{side}", f"{side}_seed"],
            "aces": [f"aces_tot_{side}"],
            "doublefaults": [f"doublefaults_tot_{side}"],
            "firstserve_dividend": [f"firstserve_dividend_tot_{side}"],
            "firstserve_divisor": [f"firstserve_divisor_tot_{side}"],
            "firstserve_percent": [f"firstserve_percent_tot_{side}"],
            "firstservepointswon_dividend": [f"firstservepointswon_dividend_tot_{side}"],
            "firstservepointswon_divisor": [f"firstservepointswon_divisor_tot_{side}"],
            "firstservepointswon_percent": [f"firstservepointswon_percent_tot_{side}"],
            "secondservepointswon_dividend": [f"secondservepointswon_dividend_tot_{side}"],
            "secondservepointswon_divisor": [f"secondservepointswon_divisor_tot_{side}"],
            "secondservepointswon_percent": [f"secondservepointswon_percent_tot_{side}"],
            "breakpointsaved_dividend": [f"breakpointssaved_dividend_tot_{side}"],
            "breakpointsaved_divisor": [f"breakpointssaved_divisor_tot_{side}"],
            "breakpointsaved_percent": [f"breakpointssaved_percent_tot_{side}"],
            "servicegamesplayed": [f"servicegamesplayed_tot_{side}"],
            "totalservicepointswon_dividend": [f"totalservicepointswon_dividend_tot_{side}"],
            "totalservicepointswon_divisor": [f"totalservicepointswon_divisor_tot_{side}"],
            "totalservicepointswon_percent": [f"totalservicepointswon_percent_tot_{side}"],
            "totalreturnpointswon_dividend": [f"totalreturnpointswon_dividend_tot_{side}"],
            "totalreturnpointswon_divisor": [f"totalreturnpointswon_divisor_tot_{side}"],
            "totalreturnpointswon_percent": [f"totalreturnpointswon_percent_tot_{side}"],
            "totalpointswon_dividend": [f"totalpointswon_dividend_tot_{side}"],
            "totalpointswon_divisor": [f"totalpointswon_divisor_tot_{side}"],
            "totalpointswon_percent": [f"totalpointswon_percent_tot_{side}"],
            "settime": [f"settime_tot_{side}", f"match_time_total"],
            "tiebreak1": [f"tiebreak_set1_{side}"],
            "tiebreak2": [f"tiebreak_set2_{side}"],
            "tiebreak3": [f"tiebreak_set3_{side}"],
        }
    return {}


def get_side_value(row: Dict[str, Any], keys: Sequence[str]) -> Optional[float]:
    v = find_first(row, keys)
    return parse_float(v)


def get_side_int(row: Dict[str, Any], keys: Sequence[str]) -> Optional[int]:
    v = find_first(row, keys)
    return parse_int(v)


def derive_stats_for_side(row: Dict[str, Any], circuit: str, side: str) -> Dict[str, Optional[float]]:
    cand = side_candidates(circuit, side)
    aces = get_side_float_or_none(row, cand["aces"])
    doublefaults = get_side_float_or_none(row, cand["doublefaults"])
    fs_dividend = get_side_float_or_none(row, cand["firstserve_dividend"])
    fs_divisor = get_side_float_or_none(row, cand["firstserve_divisor"])
    fs_pct = get_side_float_or_none(row, cand["firstserve_percent"])
    fsw_dividend = get_side_float_or_none(row, cand["firstservepointswon_dividend"])
    fsw_divisor = get_side_float_or_none(row, cand["firstservepointswon_divisor"])
    fsw_pct = get_side_float_or_none(row, cand["firstservepointswon_percent"])
    ssw_dividend = get_side_float_or_none(row, cand["secondservepointswon_dividend"])
    ssw_divisor = get_side_float_or_none(row, cand["secondservepointswon_divisor"])
    ssw_pct = get_side_float_or_none(row, cand["secondservepointswon_percent"])
    bps_saved_dividend = get_side_float_or_none(row, cand["breakpointsaved_dividend"])
    bps_saved_divisor = get_side_float_or_none(row, cand["breakpointsaved_divisor"])
    bps_saved_pct = get_side_float_or_none(row, cand["breakpointsaved_percent"])
    service_games_played = get_side_float_or_none(row, cand["servicegamesplayed"])
    tspw_dividend = get_side_float_or_none(row, cand["totalservicepointswon_dividend"])
    tspw_divisor = get_side_float_or_none(row, cand["totalservicepointswon_divisor"])
    tspw_pct = get_side_float_or_none(row, cand["totalservicepointswon_percent"])
    trpw_dividend = get_side_float_or_none(row, cand["totalreturnpointswon_dividend"])
    trpw_divisor = get_side_float_or_none(row, cand["totalreturnpointswon_divisor"])
    trpw_pct = get_side_float_or_none(row, cand["totalreturnpointswon_percent"])
    totpw_dividend = get_side_float_or_none(row, cand["totalpointswon_dividend"])
    totpw_divisor = get_side_float_or_none(row, cand["totalpointswon_divisor"])
    totpw_pct = get_side_float_or_none(row, cand["totalpointswon_percent"])
    settime = get_side_float_or_none(row, cand["settime"])

    # Breakpoints faced = saved + converted by opponent. We cannot see converted directly,
    # but saved_dividend / divisor are enough to infer the faced count; converted count is divisor - dividend.
    breakpoints_faced = bps_saved_divisor
    breakpoints_saved = bps_saved_dividend
    breakpoints_converted = None
    if breakpoints_faced is not None and breakpoints_saved is not None:
        breakpoints_converted = max(breakpoints_faced - breakpoints_saved, 0.0)
    breakpoints_converted_rate = None
    if breakpoints_faced not in {None, 0} and breakpoints_converted is not None:
        breakpoints_converted_rate = breakpoints_converted / breakpoints_faced

    service_games_lost_rate = None
    # A break for the opponent is a lost service game for this player.
    # We approximate this with the opponent's converted break points / service games played.
    if service_games_played not in {None, 0} and breakpoints_converted is not None:
        service_games_lost_rate = breakpoints_converted / service_games_played

    tie_breaks_won = 0.0
    tie_breaks_played = 0.0
    for key in ["tiebreak1", "tiebreak2", "tiebreak3"]:
        tb = get_side_float_or_none(row, cand[key])
        if tb is None:
            continue
        # In the data, a non-empty value indicates a tie-break set; the value is typically 1 when won, 0 when lost.
        tie_breaks_played += 1.0
        tie_breaks_won += 1.0 if tb > 0 else 0.0
    tie_breaks_win_rate = tie_breaks_won / tie_breaks_played if tie_breaks_played else None

    match_time_hours = None
    if settime is not None:
        # Values are usually in HH:MM:SS format; if numeric, preserve it.
        if settime > 1000:
            # likely minutes/seconds not hours; keep as hours if already encoded as decimal hours.
            match_time_hours = settime / 3600.0
        elif settime > 100:
            # likely seconds
            match_time_hours = settime / 3600.0
        else:
            match_time_hours = settime

    return {
        "number_of_aces": aces,
        "aces_per_service_point": (aces / tspw_divisor) if aces is not None and tspw_divisor not in {None, 0} else None,
        "number_of_double_faults": doublefaults,
        "double_faults_per_service_point": (doublefaults / tspw_divisor) if doublefaults is not None and tspw_divisor not in {None, 0} else None,
        "first_serve_percent": fs_pct if fs_pct is not None else (100.0 * fs_dividend / fs_divisor if fs_dividend is not None and fs_divisor not in {None, 0} else None),
        "first_serve_points_won_percent": fsw_pct if fsw_pct is not None else (100.0 * fsw_dividend / fsw_divisor if fsw_dividend is not None and fsw_divisor not in {None, 0} else None),
        "second_serve_points_won_percent": ssw_pct if ssw_pct is not None else (100.0 * ssw_dividend / ssw_divisor if ssw_dividend is not None and ssw_divisor not in {None, 0} else None),
        "service_points_won_percent": tspw_pct if tspw_pct is not None else (100.0 * tspw_dividend / tspw_divisor if tspw_dividend is not None and tspw_divisor not in {None, 0} else None),
        "return_points_won_percent": trpw_pct if trpw_pct is not None else (100.0 * trpw_dividend / trpw_divisor if trpw_dividend is not None and trpw_divisor not in {None, 0} else None),
        "breakpoints_faced": breakpoints_faced,
        "breakpoints_converted_count": breakpoints_converted,
        "breakpoints_converted_rate": breakpoints_converted_rate,
        "service_games_lost_rate": service_games_lost_rate,
        "tie_breaks_win_rate": tie_breaks_win_rate,
        "mean_match_time_hours": match_time_hours,
    }


def get_side_float_or_none(row: Dict[str, Any], keys: Sequence[str]) -> Optional[float]:
    return get_side_value(row, keys)


def normalize_match_row(row: Dict[str, Any], circuit: str, source_file: Path, period: str) -> Dict[str, Any]:
    if circuit == "ATP":
        winner_id = clean_str(find_first(row, ["player_id_winner", "winner_player_id", "PlayerIDA", "PlayerID_winner", "player_winner_id"]))
        loser_id = clean_str(find_first(row, ["player_id_loser", "loser_player_id", "PlayerIDB", "PlayerID_loser", "player_loser_id"]))
        winner_name = clean_str(find_first(row, ["player_winner", "winner_player_name", "winner"]))
        loser_name = clean_str(find_first(row, ["player_loser", "loser_player_name", "loser"]))
        winner_country = normalize_country(find_first(row, ["country_winner", "winner_country"]))
        loser_country = normalize_country(find_first(row, ["country_loser", "loser_country"]))
        winner_seed = get_side_int(row, ["seed_winner", "winner_seed"])
        loser_seed = get_side_int(row, ["seed_loser", "loser_seed"])
        round_raw = clean_str(find_first(row, ["round"]))
        draw_size = parse_int(find_first(row, ["singles_draw_size"]))
        event_year = parse_int(find_first(row, ["event_year", "tourney_year"])) or 0
        event_id = clean_str(find_first(row, ["event_id", "tourney_id"]))
        tourney_name = clean_str(find_first(row, ["tourney_name", "tournament_name", "tournament_title"]))
        start_date = parse_date(find_first(row, ["start_date"]))
        end_date = parse_date(find_first(row, ["end_date"]))
        match_date = derive_match_date(row)
        match_key = match_key_for_row(row, source_file)
        event_key = derive_event_key(row, source_file)
        level_raw = clean_str(find_first(row, ["level"]))
        surface = clean_str(find_first(row, ["surface"]))
        result = {
            "circuit": circuit,
            "period": period,
            "match_key": match_key,
            "event_key": event_key,
            "event_year": event_year,
            "event_id": event_id,
            "tourney_name": tourney_name,
            "level_raw": level_raw,
            "level_canonical": canonical_level(circuit, level_raw, tourney_name, draw_size),
            "start_date": start_date,
            "end_date": end_date,
            "match_date": match_date,
            "round_raw": round_raw,
            "round_order": round_order(round_raw),
            "round_label": round_label(round_raw),
            "surface": surface,
            "draw_size": draw_size,
            "winner": {
                "player_id": winner_id,
                "player_name": winner_name,
                "country_code": winner_country,
                "seed": winner_seed,
                "stats": derive_stats_for_side(row, circuit, "winner"),
            },
            "loser": {
                "player_id": loser_id,
                "player_name": loser_name,
                "country_code": loser_country,
                "seed": loser_seed,
                "stats": derive_stats_for_side(row, circuit, "loser"),
            },
        }
        return result

    # WTA and mixed naming.
    winner_name = clean_str(find_first(row, ["player_winner", "winner_player_name", "winner"]))
    loser_name = clean_str(find_first(row, ["player_loser", "loser_player_name", "loser"]))
    winner_id = clean_str(find_first(row, ["player_id_winner", "PlayerIDA", "PlayerIDA2", "winner_player_id"]))
    loser_id = clean_str(find_first(row, ["player_id_loser", "PlayerIDB", "PlayerIDB2", "loser_player_id"]))
    winner_country = normalize_country(find_first(row, ["country_winner", "winner_country", "country_a"]))
    loser_country = normalize_country(find_first(row, ["country_loser", "loser_country", "country_b"]))
    winner_seed = get_side_int(row, ["seed_winner", "winner_seed", "seed_a"])
    loser_seed = get_side_int(row, ["seed_loser", "loser_seed", "seed_b"])
    round_raw = clean_str(find_first(row, ["round"]))
    draw_size = parse_int(find_first(row, ["singles_draw_size"]))
    event_year = parse_int(find_first(row, ["event_year", "tourney_year"])) or 0
    event_id = clean_str(find_first(row, ["event_id", "tourney_id"]))
    tourney_name = clean_str(find_first(row, ["tourney_name", "tournament_name", "tournament_title", "city"]))
    start_date = parse_date(find_first(row, ["start_date"]))
    end_date = parse_date(find_first(row, ["end_date"]))
    match_date = derive_match_date(row)
    match_key = match_key_for_row(row, source_file)
    event_key = derive_event_key(row, source_file)
    level_raw = clean_str(find_first(row, ["level"]))
    surface = clean_str(find_first(row, ["surface"]))
    return {
        "circuit": circuit,
        "period": period,
        "match_key": match_key,
        "event_key": event_key,
        "event_year": event_year,
        "event_id": event_id,
        "tourney_name": tourney_name,
        "level_raw": level_raw,
        "level_canonical": canonical_level(circuit, level_raw, tourney_name, draw_size),
        "start_date": start_date,
        "end_date": end_date,
        "match_date": match_date,
        "round_raw": round_raw,
        "round_order": round_order(round_raw),
        "round_label": round_label(round_raw),
        "surface": surface,
        "draw_size": draw_size,
        "winner": {
            "player_id": winner_id,
            "player_name": winner_name,
            "country_code": winner_country,
            "seed": winner_seed,
            "stats": derive_stats_for_side(row, circuit, "winner"),
        },
        "loser": {
            "player_id": loser_id,
            "player_name": loser_name,
            "country_code": loser_country,
            "seed": loser_seed,
            "stats": derive_stats_for_side(row, circuit, "loser"),
        },
    }


def load_matches(root_dir: Path, circuit: str, year: int, period: str, start: Optional[date] = None, end: Optional[date] = None) -> List[Dict[str, Any]]:
    files = iter_csv_files(root_dir, circuit, year)
    matches: List[Dict[str, Any]] = []
    for path in files:
        with path.open("r", encoding="utf-8-sig", newline="") as fh:
            reader = csv.DictReader(fh)
            for row in reader:
                mdate = derive_match_date(row)
                if start and (mdate is None or mdate < start):
                    continue
                if end and (mdate is None or mdate >= end):
                    continue
                matches.append(normalize_match_row(row, circuit, path, period))
    return matches


# ---------------------------------------------------------------------------
# Participation generation and aggregation.
# ---------------------------------------------------------------------------


def make_participation(
    match: Dict[str, Any],
    side: str,
    ranking_map: Dict[str, Dict[str, Any]],
) -> Participation:
    player = match[side]
    opponent = match["loser" if side == "winner" else "winner"]
    player_id = clean_str(player.get("player_id"))
    opponent_id = clean_str(opponent.get("player_id"))
    player_rank = None
    opponent_rank = None
    ranking_entry = ranking_map.get(player_id)
    opponent_ranking_entry = ranking_map.get(opponent_id)
    if ranking_entry is not None:
        player_rank = parse_int(ranking_entry.get("ranking"))
    if opponent_ranking_entry is not None:
        opponent_rank = parse_int(opponent_ranking_entry.get("ranking"))

    level = match["level_canonical"]
    round_lbl = match["round_label"]
    won_final = bool(side == "winner" and round_lbl in {"F", "MS001", "LS001"})
    points = points_for_level_round(match["circuit"], level, round_lbl, won_final=won_final)

    stats = player["stats"].copy()
    significant_win = False
    significant_loss = False
    very_significant_win = False
    very_significant_loss = False

    if side == "winner":
        significant_win = is_significant_win(player_rank, opponent_rank, very=False)
        very_significant_win = is_significant_win(player_rank, opponent_rank, very=True)
    else:
        significant_loss = is_significant_loss(player_rank, opponent_rank, very=False)
        very_significant_loss = is_significant_loss(player_rank, opponent_rank, very=True)

    country = normalize_country(player.get("country_code") or (ranking_entry.get("country_name") if ranking_entry else ""))
    if not country:
        country = "UNK"
    country_name = clean_str((ranking_entry or {}).get("country_name"), default=country)
    if not country_name:
        country_name = country

    return Participation(
        circuit=match["circuit"],
        period=match["period"],
        match_key=match["match_key"],
        event_key=match["event_key"],
        event_year=match["event_year"],
        event_id=match["event_id"],
        tourney_name=match["tourney_name"],
        level_raw=match["level_raw"],
        level_canonical=level,
        start_date=match["start_date"],
        end_date=match["end_date"],
        match_date=match["match_date"],
        round_raw=match["round_raw"],
        round_order=match["round_order"],
        round_label=round_lbl,
        surface=match["surface"],
        draw_size=match["draw_size"],
        player_id=player_id,
        player_name=player.get("player_name") or player_id,
        opponent_id=opponent_id,
        opponent_name=opponent.get("player_name") or opponent_id,
        country_code=country,
        country_name=country_name,
        is_winner=side == "winner",
        player_rank=player_rank,
        opponent_rank=opponent_rank,
        points_earned=points,
        stats=stats,
        significant_win=significant_win,
        significant_loss=significant_loss,
        very_significant_win=very_significant_win,
        very_significant_loss=very_significant_loss,
    )


def build_participations(matches: List[Dict[str, Any]], ranking_map: Dict[str, Dict[str, Any]]) -> List[Participation]:
    participations: List[Participation] = []
    for match in matches:
        participations.append(make_participation(match, "winner", ranking_map))
        participations.append(make_participation(match, "loser", ranking_map))
    return participations


PLAYER_SCORE_WEIGHTS = {
    "win_rate": 35.0,
    "opponent_strength": 20.0,
    "level_points": 20.0,
    "significant_wins": 10.0,
    "significant_losses": -12.0,
    "rank_progress": 8.0,
    "volume": 7.0,
}


def _normalize_series(values: List[float]) -> List[float]:
    if not values:
        return []
    vmin = min(values)
    vmax = max(values)
    if math.isclose(vmin, vmax):
        return [50.0 for _ in values]
    return [100.0 * (v - vmin) / (vmax - vmin) for v in values]


def summarize_players(participations: List[Participation], ranking_map: Dict[str, Dict[str, Any]], period: str) -> Dict[str, PlayerSummary]:
    summaries: Dict[str, PlayerSummary] = {}
    # Aggregate per player.
    tournament_best: Dict[Tuple[str, str], Tuple[int, str, Dict[str, Any]]] = {}
    for p in participations:
        entry = ranking_map.get(p.player_id, {})
        if p.player_id not in summaries:
            summaries[p.player_id] = PlayerSummary(
                player_id=p.player_id,
                player_name=p.player_name,
                country_code=p.country_code or clean_str(entry.get("country_name")),
                country_name=clean_str(entry.get("country_name"), default=p.country_name or p.country_code),
                circuit=p.circuit,
                period=period,
                ranking=parse_int(entry.get("ranking")),
                ranked_last_week=parse_bool(entry.get("ranked_last_week")),
                ranked_last_year=parse_bool(entry.get("ranked_last_year")),
                ranked_beginning_year=parse_bool(entry.get("ranked_beginning_year")),
                ever_ranked=parse_bool(entry.get("ever_ranked")),
            )
        s = summaries[p.player_id]
        s.matches += 1
        s.wins += 1 if p.is_winner else 0
        s.losses += 0 if p.is_winner else 1
        s.points_earned += p.points_earned
        s.opponent_ranks.append(p.opponent_rank) if p.opponent_rank is not None else None
        s.level_points.append(p.points_earned)
        s.significant_wins += 1 if p.significant_win else 0
        s.significant_losses += 1 if p.significant_loss else 0
        s.very_significant_wins += 1 if p.very_significant_win else 0
        s.very_significant_losses += 1 if p.very_significant_loss else 0

        # Stats accumulation: counts are summed; percentages are weighted by their natural denominator if available,
        # otherwise by one match.
        weight = 1.0
        for k, v in p.stats.items():
            if v is None:
                continue
            if k in {"first_serve_percent", "first_serve_points_won_percent", "second_serve_points_won_percent", "service_points_won_percent", "return_points_won_percent", "breakpoints_converted_rate", "service_games_lost_rate", "tie_breaks_win_rate"}:
                s.stats_sums[k] += v
                s.stats_weights[k] += 1.0
            elif k == "mean_match_time_hours":
                s.stats_sums[k] += v
                s.stats_weights[k] += 1.0
            else:
                s.stats_sums[k] += v
                s.stats_weights[k] += 1.0

        # Tournament best round for the player.
        key = (p.player_id, p.event_key)
        current = tournament_best.get(key)
        candidate_order = p.round_order
        candidate_label = p.round_label
        candidate_meta = {
            "event_key": p.event_key,
            "event_id": p.event_id,
            "event_year": p.event_year,
            "tourney_name": p.tourney_name,
            "level": p.level_canonical,
            "round": p.round_raw,
            "best_round_order": candidate_order,
            "best_round_label": candidate_label,
            "match_key": p.match_key,
            "match_date": p.match_date.isoformat() if p.match_date else None,
        }
        if current is None or candidate_order < current[0]:
            tournament_best[key] = (candidate_order, candidate_label, candidate_meta)

    for pid, s in summaries.items():
        if s.opponent_ranks:
            s.avg_opponent_rank = sum(s.opponent_ranks) / len(s.opponent_ranks)
        if s.level_points:
            s.avg_tournament_points = sum(s.level_points) / len(s.level_points)
        # Build per-player tournament list.
        player_tournaments = [meta for (p_id, _), (_, _, meta) in tournament_best.items() if p_id == pid]
        player_tournaments.sort(key=lambda x: (x.get("best_round_order", 999), x.get("event_year", 0), x.get("tourney_name", "")))
        s.tournaments = player_tournaments
        s.unique_tournaments = len(player_tournaments)
        if player_tournaments:
            best = min(player_tournaments, key=lambda x: x.get("best_round_order", 999))
            s.best_round_order = best.get("best_round_order")
            s.best_round_label = best.get("best_round_label")

    # Raw score.
    raw_scores: List[Tuple[str, float]] = []
    ranks = [s.ranking for s in summaries.values() if s.ranking is not None]
    max_rank = max(ranks) if ranks else 1000
    for pid, s in summaries.items():
        matches = max(s.matches, 1)
        win_rate = s.wins / matches
        # Opponent strength: lower rank = stronger. Normalize on the current sample.
        opp = s.avg_opponent_rank if s.avg_opponent_rank is not None else float(max_rank)
        opponent_strength = 1.0 - min(opp / max(max_rank, 1), 1.0)
        level_points_score = min((s.avg_tournament_points or 0.0) / 1000.0, 1.0)
        significant_wins_score = min(s.significant_wins / 3.0, 1.0)
        significant_losses_score = min(s.significant_losses / 3.0, 1.0)
        # Positive if ranking moved up in the supplied ranking JSON.
        ranking_entry = ranking_map.get(pid, {})
        evolution_field = (
            parse_int(ranking_entry.get("evolution"))
            if period == "weekly"
            else parse_int(ranking_entry.get("evolution_this_year"))
        )
        rank_progress = 0.0
        if evolution_field is not None:
            rank_progress = max(min(evolution_field / 50.0, 1.0), -1.0)
        volume_score = min(math.log1p(matches) / math.log1p(25), 1.0)
        raw = (
            PLAYER_SCORE_WEIGHTS["win_rate"] * win_rate
            + PLAYER_SCORE_WEIGHTS["opponent_strength"] * opponent_strength
            + PLAYER_SCORE_WEIGHTS["level_points"] * level_points_score
            + PLAYER_SCORE_WEIGHTS["significant_wins"] * significant_wins_score
            + PLAYER_SCORE_WEIGHTS["significant_losses"] * (significant_losses_score * -1.0)
            + PLAYER_SCORE_WEIGHTS["rank_progress"] * rank_progress
            + PLAYER_SCORE_WEIGHTS["volume"] * volume_score
        )
        s.performance_index_raw = raw
        raw_scores.append((pid, raw))

    # Normalize to 0-100 within the circuit/period.
    normalized = _normalize_series([raw for _, raw in raw_scores])
    for (pid, _), norm in zip(raw_scores, normalized):
        summaries[pid].performance_index = norm

    return summaries


# ---------------------------------------------------------------------------
# Country aggregation.
# ---------------------------------------------------------------------------


def combine_stats(summaries: Iterable[PlayerSummary]) -> Dict[str, Any]:
    summaries = list(summaries)
    total_matches = sum(s.matches for s in summaries)
    total_wins = sum(s.wins for s in summaries)
    total_losses = sum(s.losses for s in summaries)
    total_points = sum(s.points_earned for s in summaries)

    stats_to_aggregate = [
        "number_of_aces",
        "number_of_double_faults",
        "breakpoints_faced",
        "breakpoints_converted_count",
    ]
    stats_to_average = [
        "aces_per_service_point",
        "double_faults_per_service_point",
        "first_serve_percent",
        "first_serve_points_won_percent",
        "second_serve_points_won_percent",
        "service_points_won_percent",
        "return_points_won_percent",
        "breakpoints_converted_rate",
        "service_games_lost_rate",
        "tie_breaks_win_rate",
        "mean_match_time_hours",
    ]

    agg = {
        "matches": total_matches,
        "wins": total_wins,
        "losses": total_losses,
        "win_rate": (total_wins / total_matches) if total_matches else None,
        "points_earned": total_points,
        "players_count": len(summaries),
        "players": [],
    }

    for metric in stats_to_aggregate:
        agg[metric] = sum(float(s.stats_sums.get(metric, 0.0)) for s in summaries)
    for metric in stats_to_average:
        values = []
        weights = []
        for s in summaries:
            if metric in s.stats_sums and s.stats_weights.get(metric, 0.0) > 0:
                values.append(s.stats_sums[metric] / s.stats_weights[metric])
                weights.append(s.stats_weights[metric])
        agg[metric] = weighted_average(values, weights) if values else None

    return agg


ROUND_RANKING_SORT = lambda item: (item.get("best_round_order", 999), item.get("ranking", 10**9), item.get("player_name", ""))


def player_summary_to_dict(summary: PlayerSummary) -> Dict[str, Any]:
    stats_means: Dict[str, Optional[float]] = {}
    stats_totals: Dict[str, float] = {}
    for key in summary.stats_sums:
        weight = summary.stats_weights.get(key, 0.0)
        stats_totals[key] = float(summary.stats_sums[key])
        stats_means[key] = summary.stats_sums[key] / weight if weight else None
    return {
        "player_id": summary.player_id,
        "player_name": summary.player_name,
        "country_code": summary.country_code,
        "country_name": summary.country_name,
        "circuit": summary.circuit,
        "period": summary.period,
        "ranking": summary.ranking,
        "ranked_last_week": summary.ranked_last_week,
        "ranked_last_year": summary.ranked_last_year,
        "ranked_beginning_year": summary.ranked_beginning_year,
        "ever_ranked": summary.ever_ranked,
        "matches": summary.matches,
        "wins": summary.wins,
        "losses": summary.losses,
        "win_rate": (summary.wins / summary.matches) if summary.matches else None,
        "unique_tournaments": summary.unique_tournaments,
        "tournaments": summary.tournaments,
        "best_round_order": summary.best_round_order,
        "best_round_label": summary.best_round_label,
        "points_earned": summary.points_earned,
        "avg_opponent_rank": summary.avg_opponent_rank,
        "avg_tournament_points": summary.avg_tournament_points,
        "significant_wins": summary.significant_wins,
        "significant_losses": summary.significant_losses,
        "very_significant_wins": summary.very_significant_wins,
        "very_significant_losses": summary.very_significant_losses,
        "performance_index_raw": summary.performance_index_raw,
        "performance_index": summary.performance_index,
        "stats": stats_means,
        "stats_totals": stats_totals,
        "stats_means": stats_means,
    }


def build_country_payload(
    country_code: str,
    country_name: str,
    by_circuit_period: Dict[str, Dict[str, List[PlayerSummary]]],
) -> Dict[str, Any]:
    payload: Dict[str, Any] = {
        "country_code": country_code,
        "country_name": country_name,
        "weekly": {},
        "current_year": {},
        "indices": {},
        "meta": {},
    }
    for period in ["weekly", "current_year"]:
        payload[period] = {}
        for circuit in ["ATP", "WTA"]:
            players = by_circuit_period.get(period, {}).get(circuit, [])
            if not players:
                payload[period][circuit] = {
                    "matches": 0,
                    "players_count": 0,
                    "players": [],
                    "top_players_by_matches": [],
                    "top_players_by_points": [],
                    "new_players": [],
                    "significant_wins": [],
                    "significant_losses": [],
                    "tournaments_won": [],
                    "ranked_players": [],
                    "stats": {},
                }
                continue
            players_sorted = sorted(players, key=lambda s: (-s.matches, s.player_name))
            top_by_points = sorted(players, key=lambda s: (-s.points_earned, s.player_name))
            ranked_players = [player_summary_to_dict(s) for s in sorted(players, key=lambda s: (s.ranking if s.ranking is not None else 10**9, s.player_name))]
            payload[period][circuit] = {
                "matches": sum(s.matches for s in players),
                "players_count": len(players),
                "players": ranked_players,
                "top_players_by_matches": [
                    {"player_id": s.player_id, "player_name": s.player_name, "matches": s.matches, "ranking": s.ranking}
                    for s in players_sorted[:5]
                ],
                "top_players_by_points": [
                    {"player_id": s.player_id, "player_name": s.player_name, "points_earned": s.points_earned, "ranking": s.ranking}
                    for s in top_by_points[:5]
                ],
                "new_players": [
                    {
                        "player_id": s.player_id,
                        "player_name": s.player_name,
                        "ranking": s.ranking,
                    }
                    for s in players
                    if (s.ever_ranked is False and s.ranked_last_week is False and s.ranked_last_year is False and s.ranked_beginning_year is False)
                ],
                "significant_wins": [
                    {
                        "player_id": s.player_id,
                        "player_name": s.player_name,
                        "ranking": s.ranking,
                        "opponent_rank": s.avg_opponent_rank,
                    }
                    for s in players
                    if s.significant_wins > 0
                ],
                "significant_losses": [
                    {
                        "player_id": s.player_id,
                        "player_name": s.player_name,
                        "ranking": s.ranking,
                        "opponent_rank": s.avg_opponent_rank,
                    }
                    for s in players
                    if s.significant_losses > 0
                ],
                "tournaments_won": [
                    {
                        "player_id": s.player_id,
                        "player_name": s.player_name,
                        "tournaments": [t for t in s.tournaments if t.get("best_round_label") in {"F", "MS001", "LS001"}],
                    }
                    for s in players
                    if any(t.get("best_round_label") in {"F", "MS001", "LS001"} for t in s.tournaments)
                ],
                "ranked_players": ranked_players,
                "stats": combine_stats(players),
            }
    # Combined helpers for the country.
    all_weekly_players = [p for circuit in by_circuit_period.get("weekly", {}).values() for p in circuit]
    all_year_players = [p for circuit in by_circuit_period.get("current_year", {}).values() for p in circuit]
    payload["meta"] = {
        "weekly_players": len(all_weekly_players),
        "current_year_players": len(all_year_players),
    }
    return payload


# ---------------------------------------------------------------------------
# Higher-level utilities used by the orchestrator.
# ---------------------------------------------------------------------------


def compute_per_period(
    matches: List[Dict[str, Any]],
    ranking_map: Dict[str, Dict[str, Any]],
    period: str,
) -> Tuple[Dict[str, PlayerSummary], Dict[str, Dict[str, List[PlayerSummary]]], Dict[str, Dict[str, Any]]]:
    participations = build_participations(matches, ranking_map)
    player_summaries = summarize_players(participations, ranking_map, period)

    # country -> circuit -> list of player summaries
    country_map: Dict[str, Dict[str, List[PlayerSummary]]] = defaultdict(lambda: defaultdict(list))
    country_name_map: Dict[str, str] = {}
    for summary in player_summaries.values():
        cc = summary.country_code or summary.country_name or "UNK"
        if not cc:
            cc = "UNK"
        country_map[cc][summary.circuit].append(summary)
        country_name_map.setdefault(cc, summary.country_name or cc)

    # Match-level tournament wins are already embedded in player summaries. Country winner list can be derived here.
    country_payloads: Dict[str, Dict[str, Any]] = {}
    for cc, per_circuit in country_map.items():
        country_payloads[cc] = build_country_payload(cc, country_name_map.get(cc, cc), {period: per_circuit})
    return player_summaries, country_map, country_payloads


def build_country_index_tables(
    all_year_country_maps: Dict[str, Dict[str, List[PlayerSummary]]],
) -> Dict[str, Dict[str, Any]]:
    # Compute country performance and coherence ranks based on year-to-date data only.
    country_rows: Dict[str, Dict[str, Any]] = {}
    for cc, per_circuit in all_year_country_maps.items():
        atp_players = per_circuit.get("ATP", [])
        wta_players = per_circuit.get("WTA", [])
        all_players = atp_players + wta_players
        if not all_players:
            continue

        mass = sum(p.performance_index for p in all_players)
        efficiency = (mass / len(all_players)) if all_players else None

        def circuit_vector(players: List[PlayerSummary]) -> Dict[str, float]:
            if not players:
                return {
                    "ranking": 1000.0,
                    "performance": 0.0,
                    "win_rate": 0.0,
                    "significant_win_rate": 0.0,
                    "avg_opponent_rank": 1000.0,
                    "points": 0.0,
                }
            return {
                "ranking": float(sum((p.ranking or 1000) for p in players) / len(players)),
                "performance": float(sum(p.performance_index for p in players) / len(players)),
                "win_rate": float(sum((p.wins / p.matches) if p.matches else 0.0 for p in players) / len(players)),
                "significant_win_rate": float(sum((p.significant_wins / p.matches) if p.matches else 0.0 for p in players) / len(players)),
                "avg_opponent_rank": float(sum((p.avg_opponent_rank or 1000.0) for p in players) / len(players)),
                "points": float(sum(p.points_earned for p in players) / len(players)),
            }

        atp_vec = circuit_vector(atp_players)
        wta_vec = circuit_vector(wta_players)
        coherence_distance = (
            abs(atp_vec["ranking"] - wta_vec["ranking"]) / 100.0
            + abs(atp_vec["performance"] - wta_vec["performance"]) / 100.0
            + abs(atp_vec["win_rate"] - wta_vec["win_rate"]) * 2.0
            + abs(atp_vec["significant_win_rate"] - wta_vec["significant_win_rate"]) * 2.0
            + abs(atp_vec["avg_opponent_rank"] - wta_vec["avg_opponent_rank"]) / 200.0
            + abs(atp_vec["points"] - wta_vec["points"]) / 1000.0
        )
        country_rows[cc] = {
            "country_code": cc,
            "mass": mass,
            "efficiency": efficiency,
            "coherence_distance": coherence_distance,
            "atp_players_count": len(atp_players),
            "wta_players_count": len(wta_players),
            "total_players_count": len(all_players),
            "atp_vector": atp_vec,
            "wta_vector": wta_vec,
        }

    # Rank them.
    mass_ranked = sorted(country_rows.values(), key=lambda x: (-x["mass"], x["country_code"]))
    efficiency_ranked = sorted(country_rows.values(), key=lambda x: (-(x["efficiency"] if x["efficiency"] is not None else -1e9), x["country_code"]))
    coherence_ranked = sorted(country_rows.values(), key=lambda x: (x["coherence_distance"], x["country_code"]))

    for idx, row in enumerate(mass_ranked, start=1):
        row["mass_rank"] = idx
    for idx, row in enumerate(efficiency_ranked, start=1):
        row["efficiency_rank"] = idx
    for idx, row in enumerate(coherence_ranked, start=1):
        row["coherence_rank"] = idx

    return country_rows


def serialise_country(
    country_code: str,
    country_name: str,
    weekly_map: Dict[str, Dict[str, List[PlayerSummary]]],
    year_map: Dict[str, Dict[str, List[PlayerSummary]]],
    ranks: Dict[str, Dict[str, Any]],
) -> Dict[str, Any]:
    payload = build_country_payload(country_code, country_name, {
        "weekly": weekly_map.get(country_code, {}),
        "current_year": year_map.get(country_code, {}),
    })
    rank_info = ranks.get(country_code, {})
    payload["indices"] = {
        "year_mass": rank_info.get("mass"),
        "year_efficiency": rank_info.get("efficiency"),
        "year_coherence_distance": rank_info.get("coherence_distance"),
        "year_mass_rank": rank_info.get("mass_rank"),
        "year_efficiency_rank": rank_info.get("efficiency_rank"),
        "year_coherence_rank": rank_info.get("coherence_rank"),
        "atp_vector": rank_info.get("atp_vector"),
        "wta_vector": rank_info.get("wta_vector"),
    }
    return payload

