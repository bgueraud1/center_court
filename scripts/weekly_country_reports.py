from __future__ import annotations

import argparse
import csv
import json
import math
import re
from collections import Counter, defaultdict
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta
from pathlib import Path
from statistics import mean
from typing import Any, DefaultDict, Dict, Iterable, List, Optional, Tuple
from zoneinfo import ZoneInfo


COPENHAGEN_TZ = ZoneInfo('Europe/Copenhagen')

ATP_RANKING_CANDIDATES = [
    Path('docs/tools/latest_atp_ranking.json'),
    Path('docs/Tools/latest_atp_ranking.json'),
]
WTA_RANKING_CANDIDATES = [
    Path('docs/tools/latest_wta_ranking.json'),
    Path('docs/Tools/latest_wta_ranking.json'),
]
ATP_MATCH_DIR = Path('docs/matches/atp_matches')
WTA_MATCH_DIR = Path('docs/matches/wta_matches')

ROUND_ORDER = {
    'F': 1,
    'SF': 2,
    'QF': 4,
    'R16': 8,
    'R32': 16,
    'R64': 32,
    'R128': 64,
    'RR': 100,
}

LEVEL_POINTS: Dict[str, Dict[str, int]] = {
    'Grand Slam (ATP)': {'W': 2000, 'F': 1300, 'SF': 800, 'QF': 400, 'R16': 200, 'R32': 100, 'R64': 50, 'R128': 10},
    'ATP Finals': {'W_max': 1500, 'F_max': 1000, 'RR_win': 200, 'RR_max': 600, 'SF_win': 400, 'W_bonus': 900},
    'ATP 1000 (96)': {'W': 1000, 'F': 650, 'SF': 400, 'QF': 200, 'R16': 100, 'R32': 50, 'R64': 30, 'R128': 10},
    'ATP 1000 (56)': {'W': 1000, 'F': 650, 'SF': 400, 'QF': 200, 'R16': 100, 'R32': 50, 'R64': 30, 'R128': 0},
    'ATP 500 (48)': {'W': 500, 'F': 330, 'SF': 200, 'QF': 100, 'R16': 50, 'R32': 25},
    'ATP 500 (32)': {'W': 500, 'F': 330, 'SF': 200, 'QF': 100, 'R16': 50, 'R32': 0},
    'ATP 250 (48)': {'W': 250, 'F': 165, 'SF': 100, 'QF': 50, 'R16': 25, 'R32': 13},
    'ATP 250 (32)': {'W': 250, 'F': 165, 'SF': 100, 'QF': 50, 'R16': 25, 'R32': 0},
    'Challenger 175': {'W': 175, 'F': 90, 'SF': 50, 'QF': 25, 'R16': 13, 'R32': 0},
    'Challenger 125': {'W': 125, 'F': 64, 'SF': 35, 'QF': 16, 'R16': 8, 'R32': 0},
    'Challenger 100': {'W': 100, 'F': 50, 'SF': 25, 'QF': 14, 'R16': 7, 'R32': 0},
    'Challenger 75': {'W': 75, 'F': 44, 'SF': 22, 'QF': 12, 'R16': 6, 'R32': 0},
    'Challenger 50': {'W': 50, 'F': 25, 'SF': 14, 'QF': 8, 'R16': 4, 'R32': 0},
    'Future M25': {'W': 25, 'F': 16, 'SF': 8, 'QF': 3, 'R16': 1, 'R32': 0},
    'Future M15': {'W': 15, 'F': 8, 'SF': 4, 'QF': 2, 'R16': 1, 'R32': 0},
    'Grand Slam (WTA)': {'W': 2000, 'F': 1300, 'SF': 780, 'QF': 430, 'R16': 240, 'R32': 130, 'R64': 70, 'R128': 10},
    'WTA Finals': {'W_max': 1500, 'F_max': 1000, 'SF_max': 600, 'RR_win': 200},
    'WTA 1000 (96)': {'W': 1000, 'F': 650, 'SF': 390, 'QF': 215, 'R16': 120, 'R32': 65, 'R64': 35, 'R128': 10},
    'WTA 1000 (56)': {'W': 1000, 'F': 650, 'SF': 390, 'QF': 215, 'R16': 120, 'R32': 65, 'R64': 10},
    'WTA 500 (48)': {'W': 500, 'F': 325, 'SF': 195, 'QF': 108, 'R16': 60, 'R32': 32, 'R64': 1},
    'WTA 500 (30)': {'W': 500, 'F': 325, 'SF': 195, 'QF': 108, 'R16': 60, 'R32': 1},
    'WTA 500 (28)': {'W': 500, 'F': 325, 'SF': 195, 'QF': 108, 'R16': 60, 'R32': 1},
    'WTA 250 (32)': {'W': 250, 'F': 163, 'SF': 98, 'QF': 54, 'R16': 30, 'R32': 1},
    'WTA 125 (32)': {'W': 125, 'F': 81, 'SF': 49, 'QF': 27, 'R16': 15, 'R32': 1},
    'W100 (48)': {'W': 100, 'F': 65, 'SF': 39, 'QF': 21, 'R16': 12, 'R32': 7, 'R64': 1},
    'W100 (32)': {'W': 100, 'F': 65, 'SF': 39, 'QF': 21, 'R16': 12, 'R32': 1},
    'W75 (48)': {'W': 75, 'F': 49, 'SF': 29, 'QF': 16, 'R16': 9, 'R32': 5, 'R64': 1},
    'W75 (32)': {'W': 75, 'F': 49, 'SF': 29, 'QF': 16, 'R16': 9, 'R32': 1},
    'W50 (48)': {'W': 50, 'F': 33, 'SF': 20, 'QF': 11, 'R16': 6, 'R32': 3, 'R64': 1},
    'W50 (32)': {'W': 50, 'F': 33, 'SF': 20, 'QF': 11, 'R16': 6, 'R32': 1},
    'W35 (48)': {'W': 35, 'F': 23, 'SF': 14, 'QF': 8, 'R16': 4, 'R32': 2, 'R64': 1},
    'W35 (32)': {'W': 35, 'F': 23, 'SF': 14, 'QF': 8, 'R16': 4, 'R32': 1},
    'W15 (32)': {'W': 15, 'F': 10, 'SF': 6, 'QF': 3, 'R16': 1},
}


@dataclass
class MatchRecord:
    circuit: str
    source_file: str
    event_id: str
    event_year: Optional[int]
    tourney_name: str
    level: str
    draw_size: Optional[int]
    match_id: str
    round_label: str
    round_rank: int
    match_date: Optional[date]
    score_string: str
    match_time_hours: Optional[float]
    winner_player_id: str
    loser_player_id: str
    winner_player_name: str
    loser_player_name: str
    winner_country: str
    loser_country: str
    winner_stats: Dict[str, Optional[float]]
    loser_stats: Dict[str, Optional[float]]


@dataclass
class PlayerAggregate:
    player_id: str
    player_name: str
    country: str
    circuit: str
    ranking: Optional[int]
    ranked_last_week: bool
    ranked_last_year: bool
    ranked_beginning_year: bool
    ever_ranked: bool
    evolution: Optional[int]
    evolution_year: Optional[int]
    evolution_this_year: Optional[int]
    matches_played: int = 0
    wins: int = 0
    losses: int = 0
    points_earned: int = 0
    significant_wins: int = 0
    significant_losses: int = 0
    best_round_rank: Optional[int] = None
    best_round_label: Optional[str] = None
    best_round_tourney: Optional[str] = None
    best_round_event_id: Optional[str] = None
    tournaments: Dict[str, Dict[str, Any]] = field(default_factory=dict)
    stats_values: DefaultDict[str, List[float]] = field(default_factory=lambda: defaultdict(list))


# ---------- generic helpers ----------


def load_json(path: Path) -> Any:
    with path.open('r', encoding='utf-8') as f:
        return json.load(f)


def resolve_existing_path(candidates: Iterable[Path]) -> Path:
    for candidate in candidates:
        if candidate.exists():
            return candidate
    raise FileNotFoundError('None of the candidate paths exist: ' + ', '.join(str(p) for p in candidates))


def safe_str(value: Any) -> str:
    if value is None:
        return ''
    return str(value).strip()


def safe_int(value: Any) -> Optional[int]:
    s = safe_str(value)
    if not s or s.lower() in {'nan', 'none', 'null'}:
        return None
    try:
        return int(float(s))
    except ValueError:
        return None


def safe_float(value: Any) -> Optional[float]:
    s = safe_str(value)
    if not s or s.lower() in {'nan', 'none', 'null'}:
        return None
    s = s.replace(',', '.')
    try:
        return float(s)
    except ValueError:
        return None


def normalize_name(value: Any) -> str:
    return re.sub(r'\s+', ' ', safe_str(value)).strip()


def normalize_country(value: Any) -> str:
    return safe_str(value).upper()


def parse_date(value: Any) -> Optional[date]:
    s = safe_str(value)
    if not s:
        return None
    s = s[:10]
    try:
        return datetime.strptime(s, '%Y-%m-%d').date()
    except ValueError:
        return None


def parse_duration_hours(value: Any) -> Optional[float]:
    s = safe_str(value)
    if not s:
        return None
    if re.fullmatch(r'\d+(?:\.\d+)?', s):
        return float(s)
    parts = s.split(':')
    if len(parts) != 3:
        return None
    try:
        h, m, sec = (float(p) for p in parts)
    except ValueError:
        return None
    return h + m / 60.0 + sec / 3600.0


def to_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    return safe_str(value).lower() in {'1', 'true', 'yes', 'y', 't'}


# ---------- ranking / points logic ----------


def load_rankings(path: Path, circuit: str) -> Dict[str, Dict[str, Any]]:
    items = load_json(path)
    out: Dict[str, Dict[str, Any]] = {}
    for item in items:
        player_id = safe_str(item.get('player_id'))
        if not player_id:
            continue
        out[player_id] = {
            'player_id': player_id,
            'player_name': normalize_name(item.get('full_name') or item.get('player_name') or ''),
            'country': normalize_country(item.get('country_name') or item.get('country_code') or item.get('country') or ''),
            'ranking': safe_int(item.get('ranking')),
            'points': safe_int(item.get('points')),
            'ranked_last_week': to_bool(item.get('ranked_last_week')),
            'ranked_last_year': to_bool(item.get('ranked_last_year')),
            'ranked_beginning_year': to_bool(item.get('ranked_beginning_year')),
            'ever_ranked': to_bool(item.get('ever_ranked')),
            'evolution': safe_int(item.get('evolution')),
            'evolution_year': safe_int(item.get('evolution_year')),
            'evolution_this_year': safe_int(item.get('evolution_this_year')),
            'circuit': circuit,
        }
    return out


def list_csv_files(directory: Path, year: int) -> List[Path]:
    if not directory.exists():
        return []
    return sorted([p for p in directory.glob('*.csv') if str(year) in p.name])


def detect_round_rank(match_id: str, round_label: str) -> int:
    m = re.search(r'(\d{3})$', safe_str(match_id))
    if m:
        return int(m.group(1))
    return ROUND_ORDER.get(safe_str(round_label).upper(), 999)


def canonical_level_key(circuit: str, level: str, tourney_name: str, draw_size: Optional[int]) -> Optional[str]:
    lvl = safe_str(level).upper()
    tourney = safe_str(tourney_name).upper()
    circuit = circuit.upper()

    if 'GRAND SLAM' in lvl or 'GRAND SLAM' in tourney:
        return f'Grand Slam ({circuit})'
    if 'FINALS' in lvl or 'FINALS' in tourney:
        return f'{circuit} Finals'

    direct_candidates = [lvl, tourney, f'{circuit} {lvl}', f'{circuit} {tourney}']
    for cand in direct_candidates:
        if cand in LEVEL_POINTS:
            return cand

    # numeric shorthand, e.g. 250 or W100 in tournament name
    n = None
    m = re.search(r'\b(1000|500|250|175|125|100|75|50|35|15|25)\b', lvl)
    if m:
        n = int(m.group(1))
    else:
        m = re.search(r'\b(1000|500|250|175|125|100|75|50|35|15|25)\b', tourney)
        if m:
            n = int(m.group(1))
    if n is None:
        return None

    def choose(base: str, options: List[int]) -> str:
        if draw_size is None:
            return f'{base} ({options[0]})'
        chosen = min(options, key=lambda x: abs(x - draw_size))
        return f'{base} ({chosen})'

    if circuit == 'ATP':
        if n == 1000:
            return choose('ATP 1000', [96, 56])
        if n == 500:
            return choose('ATP 500', [48, 32])
        if n == 250:
            return choose('ATP 250', [48, 32])
        if n == 175:
            return 'Challenger 175'
        if n == 125:
            return 'Challenger 125'
        if n == 100:
            return 'Challenger 100'
        if n == 75:
            return 'Challenger 75'
        if n == 50:
            return 'Challenger 50'
        if n == 25:
            return 'Future M25'
        if n == 15:
            return 'Future M15'
    else:
        if n == 1000:
            return choose('WTA 1000', [96, 56])
        if n == 500:
            return choose('WTA 500', [48, 30, 28])
        if n == 250:
            return 'WTA 250 (32)'
        if n == 125:
            return 'WTA 125 (32)'
        if n == 100:
            return choose('W100', [48, 32])
        if n == 75:
            return choose('W75', [48, 32])
        if n == 50:
            return choose('W50', [48, 32])
        if n == 35:
            return choose('W35', [48, 32])
        if n == 15:
            return 'W15 (32)'
    return None


def points_for_finish(circuit: str, level_key: Optional[str], round_rank: int, finals_bonus: bool = False) -> int:
    if not level_key or level_key not in LEVEL_POINTS:
        return 0
    points_map = LEVEL_POINTS[level_key]
    if 'Finals' in level_key:
        if finals_bonus:
            return points_map.get('W_bonus', 0)
        if round_rank <= 1:
            return points_map.get('W_max', 0)
        if round_rank <= 2:
            return points_map.get('F_max', 0)
        if round_rank <= 4:
            return points_map.get('SF_max', 0)
        return points_map.get('RR_max', 0)

    round_to_label = {1: 'W', 2: 'F', 4: 'SF', 8: 'QF', 16: 'R16', 32: 'R32', 64: 'R64', 128: 'R128'}
    label = round_to_label.get(round_rank)
    if label and label in points_map:
        return points_map[label]
    # if exact label is missing, take nearest available non-zero point value.
    return max(points_map.values()) if points_map else 0


# ---------- stats extraction ----------


def pick_first_numeric(row: Dict[str, Any], candidates: Iterable[str]) -> Optional[float]:
    for key in candidates:
        if key in row:
            v = safe_float(row.get(key))
            if v is not None:
                return v
    return None


def derive_tiebreak_stats(score_string: str, player_is_winner: bool) -> Tuple[Optional[int], Optional[int]]:
    s = safe_str(score_string)
    if not s:
        return None, None
    won = 0
    played = 0
    for token in [t.strip() for t in s.split(',') if t.strip()]:
        m = re.search(r'(\d+)-(\d+)\((\d+)\)', token)
        if not m:
            continue
        a = int(m.group(1))
        b = int(m.group(2))
        played += 1
        set_winner_is_first = a > b
        if player_is_winner == set_winner_is_first:
            won += 1
    return won, played


def extract_side_stats(row: Dict[str, Any], score_string: str, side: str) -> Dict[str, Optional[float]]:
    side = side.lower()
    prefix = f'_{side}'
    aces = pick_first_numeric(row, [f'aces_tot{prefix}', f'aces_{side}', f'{side}_aces'])
    double_faults = pick_first_numeric(row, [f'doublefaults_tot{prefix}', f'doublefaults_{side}', f'{side}_doublefaults'])
    first_serve_pct = pick_first_numeric(row, [f'firstserve_percent_tot{prefix}', f'firstserve_percent_{side}'])
    first_serve_points_won_pct = pick_first_numeric(row, [f'firstservepointswon_percent_tot{prefix}', f'firstservepointswon_percent_{side}'])
    second_serve_points_won_pct = pick_first_numeric(row, [f'secondservepointswon_percent_tot{prefix}', f'secondservepointswon_percent_{side}'])
    service_points_won_pct = pick_first_numeric(row, [f'totalservicepointswon_percent_tot{prefix}', f'totalservicepointswon_percent_{side}'])
    return_points_won_pct = pick_first_numeric(row, [f'totalreturnpointswon_percent_tot{prefix}', f'totalreturnpointswon_percent_{side}'])
    service_points_won = pick_first_numeric(row, [f'totalservicepointswon_dividend_tot{prefix}', f'totalservicepointswon_dividend_{side}'])
    service_points_total = pick_first_numeric(row, [f'totalservicepointswon_divisor_tot{prefix}', f'totalservicepointswon_divisor_{side}'])
    if service_points_total is None and service_points_won is not None and service_points_won_pct is not None and service_points_won_pct > 0:
        service_points_total = service_points_won / (service_points_won_pct / 100.0)
    breakpoints_saved = pick_first_numeric(row, [f'breakpointssaved_dividend_tot{prefix}', f'breakpointssaved_dividend_{side}'])
    breakpoints_faced = pick_first_numeric(row, [f'breakpointssaved_divisor_tot{prefix}', f'breakpointssaved_divisor_{side}'])
    if breakpoints_faced is None and breakpoints_saved is not None:
        breakpoints_faced = breakpoints_saved
    breakpoints_converted = None
    if breakpoints_faced is not None and breakpoints_saved is not None:
        breakpoints_converted = max(0.0, breakpoints_faced - breakpoints_saved)
    breakpoints_converted_rate = None
    if breakpoints_converted is not None and breakpoints_faced and breakpoints_faced > 0:
        breakpoints_converted_rate = breakpoints_converted / breakpoints_faced
    service_games_played = pick_first_numeric(row, [f'servicegamesplayed_tot{prefix}', f'servicegamesplayed_{side}'])
    service_games_lost_rate = pick_first_numeric(row, [f'servicegameslost_rate_tot{prefix}', f'servicegameslost_rate_{side}'])
    if service_games_lost_rate is None and service_games_played and service_games_played > 0 and breakpoints_converted is not None:
        service_games_lost_rate = breakpoints_converted / service_games_played
    player_is_winner = side == 'winner'
    tb_won, tb_played = derive_tiebreak_stats(score_string, player_is_winner)
    tb_win_rate = None
    if tb_won is not None and tb_played and tb_played > 0:
        tb_win_rate = tb_won / tb_played
    match_time_hours = parse_duration_hours(row.get('match_time_total') or row.get('match_timestamp'))
    return {
        'aces': aces,
        'double_faults': double_faults,
        'first_serve_pct': first_serve_pct,
        'first_serve_points_won_pct': first_serve_points_won_pct,
        'second_serve_points_won_pct': second_serve_points_won_pct,
        'service_points_won_pct': service_points_won_pct,
        'return_points_won_pct': return_points_won_pct,
        'breakpoints_faced': breakpoints_faced,
        'breakpoints_converted_count': breakpoints_converted,
        'breakpoints_converted_rate': breakpoints_converted_rate,
        'service_games_lost_rate': service_games_lost_rate,
        'tie_breaks_won': tb_won,
        'tie_breaks_played': tb_played,
        'tie_breaks_win_rate': tb_win_rate,
        'match_time_hours': match_time_hours,
        'service_points_played': service_points_total,
    }


# ---------- match loading ----------


def load_match_records(match_dir: Path, year: int, circuit: str) -> List[MatchRecord]:
    records: List[MatchRecord] = []
    for csv_path in list_csv_files(match_dir, year):
        with csv_path.open('r', encoding='utf-8-sig', newline='') as f:
            reader = csv.DictReader(f)
            for row in reader:
                match_date = parse_date(row.get('match_date') or row.get('date') or row.get('start_date'))
                if not match_date:
                    continue
                event_id = safe_str(row.get('event_id') or row.get('tourney_id'))
                event_year = safe_int(row.get('event_year') or row.get('tourney_year'))
                tourney_name = safe_str(row.get('tourney_name') or row.get('tournament_name') or row.get('tournament_title'))
                level = safe_str(row.get('level'))
                draw_size = safe_int(row.get('singles_draw_size'))
                match_id = safe_str(row.get('match_id'))
                round_label = safe_str(row.get('round'))
                round_rank = detect_round_rank(match_id, round_label)
                score_string = safe_str(row.get('score_string'))
                rec = MatchRecord(
                    circuit=circuit,
                    source_file=csv_path.name,
                    event_id=event_id,
                    event_year=event_year,
                    tourney_name=tourney_name,
                    level=level,
                    draw_size=draw_size,
                    match_id=match_id,
                    round_label=round_label,
                    round_rank=round_rank,
                    match_date=match_date,
                    score_string=score_string,
                    match_time_hours=parse_duration_hours(row.get('match_time_total') or row.get('match_timestamp')),
                    winner_player_id=safe_str(row.get('player_id_winner') or row.get('player_id_a') or row.get('PlayerIDA') or row.get('PlayerIDB')),
                    loser_player_id=safe_str(row.get('player_id_loser') or row.get('player_id_b') or row.get('PlayerIDA2') or row.get('PlayerIDB2')),
                    winner_player_name=normalize_name(row.get('winner_player_name') or row.get('winner') or row.get('player_winner') or row.get('player_a') or ''),
                    loser_player_name=normalize_name(row.get('loser_player_name') or row.get('loser') or row.get('player_loser') or row.get('player_b') or ''),
                    winner_country=normalize_country(row.get('winner_country') or row.get('country_winner') or row.get('country_a') or ''),
                    loser_country=normalize_country(row.get('loser_country') or row.get('country_loser') or row.get('country_b') or ''),
                    winner_stats=extract_side_stats(row, score_string, 'winner'),
                    loser_stats=extract_side_stats(row, score_string, 'loser'),
                )
                records.append(rec)
    return records


# ---------- significance / performance ----------


def significance_threshold(rank: Optional[int]) -> int:
    if rank is None:
        return 9999
    if rank <= 10:
        return 8
    if rank <= 25:
        return 12
    if rank <= 50:
        return 15
    if rank <= 100:
        return 20
    if rank <= 200:
        return 30
    if rank <= 300:
        return 40
    if rank <= 500:
        return 50
    return 75


def very_significance_threshold(rank: Optional[int]) -> int:
    return max(2, significance_threshold(rank) * 2)


def rank_change_label(delta: Optional[int]) -> Optional[str]:
    if delta is None or delta == 0:
        return None
    return 'rise' if delta > 0 else 'drop'


def is_significant_win(player_rank: Optional[int], opponent_rank: Optional[int]) -> bool:
    if player_rank is None or opponent_rank is None:
        return False
    if opponent_rank >= player_rank:
        return False
    return (player_rank - opponent_rank) >= significance_threshold(player_rank)


def is_significant_change(current_rank: Optional[int], delta: Optional[int], very: bool = False) -> bool:
    if current_rank is None or delta is None or delta == 0:
        return False
    threshold = very_significance_threshold(current_rank) if very else significance_threshold(current_rank)
    return abs(delta) >= threshold


# ---------- aggregation ----------


def update_stats_bucket(bucket: DefaultDict[str, List[float]], stats: Dict[str, Optional[float]]) -> None:
    for key, value in stats.items():
        if value is None:
            continue
        bucket[key].append(float(value))


def summarize(values: List[float]) -> Dict[str, Optional[float]]:
    if not values:
        return {'mean': None, 'sum': None, 'count': 0}
    return {'mean': sum(values) / len(values), 'sum': sum(values), 'count': len(values)}


def tournament_key(rec: MatchRecord) -> str:
    return rec.event_id or f'{rec.tourney_name}|{rec.match_date.isoformat() if rec.match_date else ""}'


def is_final_winner(rec: MatchRecord) -> bool:
    return rec.match_id.endswith('001') or rec.round_label.upper() in {'F', 'FINAL', 'FIN'}


def process_period(
    matches: List[MatchRecord],
    rankings: Dict[str, Dict[str, Any]],
    circuit: str,
    period_name: str,
    period_filter,
    week_start: date,
    week_end: date,
    current_year: int,
) -> Dict[str, Any]:
    filtered = [m for m in matches if m.match_date and period_filter(m.match_date)]
    if not filtered:
        return {
            'period': {
                'name': period_name,
                'start_date': week_start.isoformat() if period_name == 'last_week' else f'{current_year}-01-01',
                'end_date': week_end.isoformat() if period_name == 'last_week' else f'{current_year + 1}-01-01',
            },
            'match_counts': {'unique_matches': 0, 'player_appearances': 0},
            'tournaments': [],
            'most_played_tourney_name': None,
            'top_5_players_by_matches': [],
            'top_5_players_by_points_earned': [],
            'tournaments_won_by_country_players': [],
            'significant_wins': [],
            'significant_losses': [],
            'ranking_moves_last_week': [],
            'ranking_moves_last_year': [],
            'ranking_moves_beginning_year': [],
            'new_players': [],
            'ranked_players': [],
            'match_stats': {},
            'performance_index_ranking': [],
        }

    country_player_aggs: DefaultDict[str, Dict[str, PlayerAggregate]] = defaultdict(dict)
    country_match_ids: DefaultDict[str, set] = defaultdict(set)
    country_tournament_counter: DefaultDict[str, Counter] = defaultdict(Counter)
    country_significant_wins: DefaultDict[str, List[Dict[str, Any]]] = defaultdict(list)
    country_significant_losses: DefaultDict[str, List[Dict[str, Any]]] = defaultdict(list)
    country_tournament_wins: DefaultDict[str, List[Dict[str, Any]]] = defaultdict(list)
    for rec in filtered:
        winner_rank = rankings.get(rec.winner_player_id, {}).get('ranking')
        loser_rank = rankings.get(rec.loser_player_id, {}).get('ranking')
        for side in ('winner', 'loser'):
            player_id = rec.winner_player_id if side == 'winner' else rec.loser_player_id
            player_name = rec.winner_player_name if side == 'winner' else rec.loser_player_name
            player_country = rec.winner_country if side == 'winner' else rec.loser_country
            side_stats = rec.winner_stats if side == 'winner' else rec.loser_stats
            ranking = rankings.get(player_id)
            if ranking:
                player_country = ranking['country'] or player_country
            if not player_country:
                continue
            if player_id not in country_player_aggs[player_country]:
                base = ranking or {
                    'player_id': player_id,
                    'player_name': player_name,
                    'country': player_country,
                    'ranking': None,
                    'ranked_last_week': False,
                    'ranked_last_year': False,
                    'ranked_beginning_year': False,
                    'ever_ranked': False,
                    'evolution': None,
                    'evolution_year': None,
                    'evolution_this_year': None,
                }
                country_player_aggs[player_country][player_id] = PlayerAggregate(
                    player_id=player_id,
                    player_name=base['player_name'] or player_name,
                    country=player_country,
                    circuit=circuit,
                    ranking=base['ranking'],
                    ranked_last_week=base['ranked_last_week'],
                    ranked_last_year=base['ranked_last_year'],
                    ranked_beginning_year=base['ranked_beginning_year'],
                    ever_ranked=base['ever_ranked'],
                    evolution=base['evolution'],
                    evolution_year=base['evolution_year'],
                    evolution_this_year=base['evolution_this_year'],
                )
            agg = country_player_aggs[player_country][player_id]
            agg.matches_played += 1
            if side == 'winner':
                agg.wins += 1
            else:
                agg.losses += 1
            update_stats_bucket(agg.stats_values, side_stats)
            country_match_ids[player_country].add(tournament_key(rec))
            country_tournament_counter[player_country][rec.tourney_name] += 1

            opp_rank = loser_rank if side == 'winner' else winner_rank
            if side == 'winner' and is_significant_win(winner_rank, loser_rank):
                agg.significant_wins += 1
                country_significant_wins[player_country].append({
                    'player_id': player_id,
                    'player_name': agg.player_name,
                    'player_rank': winner_rank,
                    'opponent_id': rec.loser_player_id,
                    'opponent_name': rec.loser_player_name,
                    'opponent_rank': loser_rank,
                    'rank_gap': (winner_rank - loser_rank) if winner_rank is not None and loser_rank is not None else None,
                    'tourney_name': rec.tourney_name,
                    'level': rec.level,
                    'round': rec.round_label,
                    'match_id': rec.match_id,
                    'match_date': rec.match_date.isoformat() if rec.match_date else None,
                })
            if side == 'loser' and is_significant_win(loser_rank, winner_rank):
                agg.significant_losses += 1
                country_significant_losses[player_country].append({
                    'player_id': player_id,
                    'player_name': agg.player_name,
                    'player_rank': loser_rank,
                    'opponent_id': rec.winner_player_id,
                    'opponent_name': rec.winner_player_name,
                    'opponent_rank': winner_rank,
                    'rank_gap': (loser_rank - winner_rank) if loser_rank is not None and winner_rank is not None else None,
                    'tourney_name': rec.tourney_name,
                    'level': rec.level,
                    'round': rec.round_label,
                    'match_id': rec.match_id,
                    'match_date': rec.match_date.isoformat() if rec.match_date else None,
                })

            # ranking evolution buckets
            if is_significant_change(agg.ranking, agg.evolution, very=False):
                country_rank_moves[player_country]['last_week'].append({
                    'player_id': player_id,
                    'player_name': agg.player_name,
                    'ranking': agg.ranking,
                    'delta': agg.evolution,
                    'direction': rank_change_label(agg.evolution),
                })
            if is_significant_change(agg.ranking, agg.evolution_year, very=True):
                country_rank_moves[player_country]['last_year'].append({
                    'player_id': player_id,
                    'player_name': agg.player_name,
                    'ranking': agg.ranking,
                    'delta': agg.evolution_year,
                    'direction': rank_change_label(agg.evolution_year),
                })
            if is_significant_change(agg.ranking, agg.evolution_this_year, very=True):
                country_rank_moves[player_country]['beginning_year'].append({
                    'player_id': player_id,
                    'player_name': agg.player_name,
                    'ranking': agg.ranking,
                    'delta': agg.evolution_this_year,
                    'direction': rank_change_label(agg.evolution_this_year),
                })

            if not (agg.ever_ranked or agg.ranked_last_week or agg.ranked_last_year or agg.ranked_beginning_year):
                # record once per player
                pass

            # tournament participation tracking
            tkey = tournament_key(rec)
            entry = agg.tournaments.setdefault(
                tkey,
                {
                    'event_id': rec.event_id,
                    'event_year': rec.event_year,
                    'tourney_name': rec.tourney_name,
                    'level': rec.level,
                    'draw_size': rec.draw_size,
                    'best_round_label': rec.round_label,
                    'best_round_rank': rec.round_rank,
                    'matches_played': 0,
                    'wins': 0,
                    'losses': 0,
                    'points_earned': 0,
                },
            )
            entry['matches_played'] += 1
            entry['wins'] += 1 if side == 'winner' else 0
            entry['losses'] += 1 if side == 'loser' else 0
            if rec.round_rank < entry['best_round_rank']:
                entry['best_round_rank'] = rec.round_rank
                entry['best_round_label'] = rec.round_label

            # tournament wins: winner of a final or equivalent
            if side == 'winner' and is_final_winner(rec):
                country_tournament_wins[player_country].append({
                    'player_id': player_id,
                    'player_name': agg.player_name,
                    'tourney_name': rec.tourney_name,
                    'level': rec.level,
                    'event_id': rec.event_id,
                    'match_id': rec.match_id,
                    'round': rec.round_label,
                    'match_date': rec.match_date.isoformat() if rec.match_date else None,
                })

    # Finalize per player: assign points by best round in each tournament.
    for country_code, players in country_player_aggs.items():
        for agg in players.values():
            agg.points_earned = 0
            for tournament in agg.tournaments.values():
                level_key = canonical_level_key(circuit, tournament['level'], tournament['tourney_name'], tournament.get('draw_size'))
                points = points_for_finish(circuit, level_key, tournament['best_round_rank'])
                tournament['level_key'] = level_key
                tournament['points_earned'] = points
                agg.points_earned += points
                if agg.best_round_rank is None or tournament['best_round_rank'] < agg.best_round_rank:
                    agg.best_round_rank = tournament['best_round_rank']
                    agg.best_round_label = tournament['best_round_label']
                    agg.best_round_tourney = tournament['tourney_name']
                    agg.best_round_event_id = tournament['event_id']

    # Build country summaries.
    country_summaries: Dict[str, Dict[str, Any]] = {}
    for country_code, players in country_player_aggs.items():
        player_list = list(players.values())
        match_appearances = sum(p.matches_played for p in player_list)
        unique_matches = len(country_match_ids[country_code])
        most_played_tourney_name = country_tournament_counter[country_code].most_common(1)[0][0] if country_tournament_counter[country_code] else None

        top_by_matches = sorted(player_list, key=lambda p: (-p.matches_played, p.ranking if p.ranking is not None else 999999, p.player_name))[:5]
        top_by_points = sorted(player_list, key=lambda p: (-p.points_earned, p.ranking if p.ranking is not None else 999999, p.player_name))[:5]

        # ranked players ordered by ranking asc, each with tournaments and highest round reached.
        ranked_players = []
        for p in sorted([x for x in player_list if x.ranking is not None], key=lambda x: (x.ranking if x.ranking is not None else 999999, x.player_name)):
            ranked_players.append({
                'player_id': p.player_id,
                'player_name': p.player_name,
                'ranking': p.ranking,
                'country': p.country,
                'tournaments': sorted(p.tournaments.values(), key=lambda t: (t['best_round_rank'], t['tourney_name'])),
                'highest_round_reached': {
                    'round': p.best_round_label,
                    'round_rank': p.best_round_rank,
                    'tourney_name': p.best_round_tourney,
                    'event_id': p.best_round_event_id,
                },
            })

        # stats across appearances
        stat_bucket: DefaultDict[str, List[float]] = defaultdict(list)
        for p in player_list:
            for key, values in p.stats_values.items():
                stat_bucket[key].extend(values)

        def s(key: str) -> Dict[str, Optional[float]]:
            return summarize(stat_bucket.get(key, []))

        # performance index
        max_points = max((p.points_earned for p in player_list), default=0) or 1
        perf = []
        for p in player_list:
            win_rate = p.wins / p.matches_played if p.matches_played else 0.0
            points_component = p.points_earned / max_points
            sig_win_component = min(p.significant_wins / max(1, p.matches_played), 1.0)
            sig_loss_component = min(p.significant_losses / max(1, p.matches_played), 1.0)
            # Assumption: positive evolution means rank improvement.
            momentum_values = [x for x in [p.evolution, p.evolution_year, p.evolution_this_year] if x is not None]
            ranking_momentum = 0.0
            if momentum_values:
                ranking_momentum = max(-1.0, min(1.0, sum(max(-100.0, min(100.0, float(v))) for v in momentum_values) / (300.0)))
            level_bonus = 0.0
            if p.best_round_rank is not None:
                level_bonus = max(0.0, 1.0 - min(p.best_round_rank, 128) / 128.0)
            score = 100.0 * (
                0.38 * win_rate +
                0.28 * points_component +
                0.14 * sig_win_component -
                0.14 * sig_loss_component +
                0.06 * ((ranking_momentum + 1.0) / 2.0) +
                0.04 * level_bonus
            )
            perf.append({
                'player_id': p.player_id,
                'player_name': p.player_name,
                'ranking': p.ranking,
                'country': p.country,
                'matches_played': p.matches_played,
                'wins': p.wins,
                'losses': p.losses,
                'points_earned': p.points_earned,
                'significant_wins': p.significant_wins,
                'significant_losses': p.significant_losses,
                'performance_index': round(max(0.0, min(100.0, score)), 2),
                'performance_breakdown': {
                    'win_rate': round(win_rate, 4),
                    'points_component': round(points_component, 4),
                    'significant_win_component': round(sig_win_component, 4),
                    'significant_loss_component': round(sig_loss_component, 4),
                    'ranking_momentum': round(ranking_momentum, 4),
                    'level_bonus': round(level_bonus, 4),
                },
            })
        perf.sort(key=lambda x: (-x['performance_index'], x['ranking'] if x['ranking'] is not None else 999999, x['player_name']))

        # ranking-evolution / new-player lists
        last_week_moves = [
            {'player_id': p.player_id, 'player_name': p.player_name, 'ranking': p.ranking, 'delta': p.evolution, 'direction': rank_change_label(p.evolution), 'country': p.country}
            for p in player_list if is_significant_change(p.ranking, p.evolution, very=False)
        ]
        last_year_moves = [
            {'player_id': p.player_id, 'player_name': p.player_name, 'ranking': p.ranking, 'delta': p.evolution_year, 'direction': rank_change_label(p.evolution_year), 'country': p.country}
            for p in player_list if is_significant_change(p.ranking, p.evolution_year, very=True)
        ]
        beginning_year_moves = [
            {'player_id': p.player_id, 'player_name': p.player_name, 'ranking': p.ranking, 'delta': p.evolution_this_year, 'direction': rank_change_label(p.evolution_this_year), 'country': p.country}
            for p in player_list if is_significant_change(p.ranking, p.evolution_this_year, very=True)
        ]
        new_players = [
            {
                'player_id': p.player_id,
                'player_name': p.player_name,
                'ranking': p.ranking,
                'country': p.country,
                'ever_ranked': p.ever_ranked,
                'ranked_last_week': p.ranked_last_week,
                'ranked_last_year': p.ranked_last_year,
                'ranked_beginning_year': p.ranked_beginning_year,
            }
            for p in player_list if not p.ever_ranked and not p.ranked_last_week and not p.ranked_last_year and not p.ranked_beginning_year
        ]

        country_summaries[country_code] = {
            'period': {
                'name': period_name,
                'start_date': week_start.isoformat() if period_name == 'last_week' else f'{current_year}-01-01',
                'end_date': week_end.isoformat() if period_name == 'last_week' else f'{current_year + 1}-01-01',
            },
            'match_counts': {
                'unique_matches': unique_matches,
                'player_appearances': match_appearances,
            },
            'tournaments': [{'tourney_name': name, 'appearances': count} for name, count in country_tournament_counter[country_code].most_common()],
            'most_played_tourney_name': most_played_tourney_name,
            'top_5_players_by_matches': [
                {'player_id': p.player_id, 'player_name': p.player_name, 'ranking': p.ranking, 'matches_played': p.matches_played, 'wins': p.wins, 'losses': p.losses, 'country': p.country}
                for p in top_by_matches
            ],
            'top_5_players_by_points_earned': [
                {'player_id': p.player_id, 'player_name': p.player_name, 'ranking': p.ranking, 'points_earned': p.points_earned, 'matches_played': p.matches_played, 'country': p.country}
                for p in top_by_points
            ],
            'tournaments_won_by_country_players': country_tournament_wins[country_code],
            'significant_wins': country_significant_wins[country_code],
            'significant_losses': country_significant_losses[country_code],
            'ranking_moves_last_week': last_week_moves,
            'ranking_moves_last_year': last_year_moves,
            'ranking_moves_beginning_year': beginning_year_moves,
            'new_players': new_players,
            'ranked_players': ranked_players,
            'match_stats': {
                'number_of_aces': s('aces'),
                'aces_per_service_point': s('aces_per_service_point'),
                'number_of_double_faults': s('double_faults'),
                'double_faults_per_service_point': s('double_faults_per_service_point'),
                'first_serve_pct': s('first_serve_pct'),
                'first_serve_points_won_pct': s('first_serve_points_won_pct'),
                'second_serve_points_won_pct': s('second_serve_points_won_pct'),
                'service_points_won_pct': s('service_points_won_pct'),
                'return_points_won_pct': s('return_points_won_pct'),
                'breakpoints_faced': s('breakpoints_faced'),
                'breakpoints_converted_count': s('breakpoints_converted_count'),
                'breakpoints_converted_rate': s('breakpoints_converted_rate'),
                'service_games_lost_rate': s('service_games_lost_rate'),
                'tie_breaks_win_rate': s('tie_breaks_win_rate'),
                'mean_match_time_hours': s('match_time_hours'),
            },
            'performance_index_ranking': perf,
        }

    return country_summaries


def build_country_reports(
    atp_ranking_path: Path,
    wta_ranking_path: Path,
    atp_match_dir: Path,
    wta_match_dir: Path,
    output_dir: Path,
    today: Optional[date] = None,
) -> Dict[str, Path]:
    today = today or datetime.now(COPENHAGEN_TZ).date()
    week_start = today - timedelta(days=7)
    week_end = today
    current_year = today.year

    atp_rankings = load_rankings(atp_ranking_path, 'ATP')
    wta_rankings = load_rankings(wta_ranking_path, 'WTA')
    atp_matches = load_match_records(atp_match_dir, current_year, 'ATP')
    wta_matches = load_match_records(wta_match_dir, current_year, 'WTA')

    # Build the full country set from ranking files and from match participants.
    all_countries = set()
    for rankings in (atp_rankings, wta_rankings):
        for item in rankings.values():
            if item['country']:
                all_countries.add(item['country'])
    for matches in (atp_matches, wta_matches):
        for rec in matches:
            if rec.winner_country:
                all_countries.add(rec.winner_country)
            if rec.loser_country:
                all_countries.add(rec.loser_country)

    output_dir.mkdir(parents=True, exist_ok=True)
    generated: Dict[str, Path] = {}

    def week_filter(d: date) -> bool:
        return week_start <= d < week_end

    def year_filter(d: date) -> bool:
        return d.year == current_year

    for country in sorted(all_countries):
        country_payload = {
            'generated_at': datetime.now(COPENHAGEN_TZ).isoformat(),
            'country_code': country,
            'year': current_year,
            'meta': {
                'week_window': {'start_date': week_start.isoformat(), 'end_date': week_end.isoformat()},
                'source': 'weekly_update country report generator',
            },
            'atp': {
                'last_week': process_period(atp_matches, atp_rankings, 'ATP', 'last_week', week_filter, week_start, week_end, current_year),
                'current_year': process_period(atp_matches, atp_rankings, 'ATP', 'current_year', year_filter, week_start, week_end, current_year),
            },
            'wta': {
                'last_week': process_period(wta_matches, wta_rankings, 'WTA', 'last_week', week_filter, week_start, week_end, current_year),
                'current_year': process_period(wta_matches, wta_rankings, 'WTA', 'current_year', year_filter, week_start, week_end, current_year),
            },
        }
        out_path = output_dir / f'{country}.json'
        with out_path.open('w', encoding='utf-8') as f:
            json.dump(country_payload, f, ensure_ascii=False, indent=2)
        generated[country] = out_path

    index = {
        'generated_at': datetime.now(COPENHAGEN_TZ).isoformat(),
        'year': current_year,
        'week_window': {'start_date': week_start.isoformat(), 'end_date': week_end.isoformat()},
        'countries': sorted(generated.keys()),
        'files': {country: str(path) for country, path in generated.items()},
    }
    with (output_dir / 'index.json').open('w', encoding='utf-8') as f:
        json.dump(index, f, ensure_ascii=False, indent=2)
    return generated


# ---------- CLI ----------


def main() -> None:
    parser = argparse.ArgumentParser(description='Generate per-country ATP/WTA weekly and YTD reports.')
    parser.add_argument('--root-dir', type=Path, default=Path('.'), help='Repository root containing docs/.')
    parser.add_argument('--output-dir', type=Path, default=None, help='Where country JSON files are written.')
    parser.add_argument('--today', type=str, default=None, help='Override the current date (YYYY-MM-DD).')
    args = parser.parse_args()

    root_dir = args.root_dir.resolve()
    output_dir = (args.output_dir or (root_dir / 'docs' / 'reports' / 'country_reports')).resolve()
    today = datetime.strptime(args.today, '%Y-%m-%d').date() if args.today else None

    atp_ranking_path = resolve_existing_path([root_dir / p for p in ATP_RANKING_CANDIDATES])
    wta_ranking_path = resolve_existing_path([root_dir / p for p in WTA_RANKING_CANDIDATES])

    build_country_reports(
        atp_ranking_path=atp_ranking_path,
        wta_ranking_path=wta_ranking_path,
        atp_match_dir=root_dir / ATP_MATCH_DIR,
        wta_match_dir=root_dir / WTA_MATCH_DIR,
        output_dir=output_dir,
        today=today,
    )


if __name__ == '__main__':
    main()
