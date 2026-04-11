from __future__ import annotations

import csv
import json
import re
import unicodedata
from collections import defaultdict
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple
from zoneinfo import ZoneInfo

import tennis_performance_regional as tp

PARIS_TZ = ZoneInfo('Europe/Paris')


@dataclass(frozen=True)
class RegionConfig:
    region_code: str
    region_name: str
    countries: Tuple[str, ...]
    output_filename: str


def current_period_dates(today: Optional[datetime] = None) -> Tuple[date, date, date]:
    now = today or datetime.now(tz=PARIS_TZ)
    current_monday = now.date() - timedelta(days=now.weekday())
    last_monday = current_monday - timedelta(days=7)
    return last_monday, current_monday, date(now.year, 1, 1)


def norm(text: Any) -> str:
    s = '' if text is None else str(text).strip()
    if not s:
        return ''
    s = unicodedata.normalize('NFKD', s)
    s = ''.join(c for c in s if not unicodedata.combining(c))
    return s.lower()


def load_json(path: Path) -> Any:
    with path.open('r', encoding='utf-8') as fh:
        return json.load(fh)


def write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open('w', encoding='utf-8') as fh:
        json.dump(payload, fh, ensure_ascii=False, indent=2)


def load_latest_rankings(root_dir: Path) -> Dict[str, Dict[str, Dict[str, Any]]]:
    paths = {
        'ATP': [root_dir / 'docs' / 'tools' / 'latest_atp_ranking.json', root_dir / 'docs' / 'Tools' / 'latest_atp_ranking.json', root_dir / 'docs' / 'latest_atp_ranking.json'],
        'WTA': [root_dir / 'docs' / 'tools' / 'latest_wta_ranking.json', root_dir / 'docs' / 'Tools' / 'latest_wta_ranking.json', root_dir / 'docs' / 'latest_wta_ranking.json'],
    }
    out: Dict[str, Dict[str, Dict[str, Any]]] = {}
    for circuit, candidates in paths.items():
        p = next((x for x in candidates if x.exists()), None)
        if p is None:
            raise FileNotFoundError(f'Ranking latest introuvable pour {circuit}')
        out[circuit] = tp.load_ranking_json(p)
    return out


def load_country_reverse_map(root_dir: Path) -> Dict[str, str]:
    p = root_dir / 'docs' / 'tools' / 'country_to_ioc.json'
    if not p.exists():
        p = root_dir / 'docs' / 'Tools' / 'country_to_ioc.json'
    if not p.exists():
        return {}
    data = load_json(p)
    return {norm(country): code.upper() for country, code in data.items()}


def country_from_location(location: Any, reverse_map: Dict[str, str]) -> str:
    s = tp.clean_str(location)
    if not s:
        return ''
    candidate = s.split(',')[-1].strip() if ',' in s else s
    return reverse_map.get(norm(candidate), '')


def load_atp_tournament_locations(root_dir: Path, year: int) -> Dict[str, Dict[str, str]]:
    p = root_dir / 'docs' / f'atp_tournaments_{year}.json'
    if not p.exists():
        return {}
    reverse_map = load_country_reverse_map(root_dir)
    data = load_json(p)
    out: Dict[str, Dict[str, str]] = {}
    for block in data.get('TournamentDates', []):
        for t in block.get('Tournaments', []):
            event_id = tp.clean_str(t.get('Id'))
            if not event_id:
                continue
            location = tp.clean_str(t.get('Location'))
            out[event_id] = {
                'country_code': country_from_location(location, reverse_map),
                'country_name': location.split(',')[-1].strip() if location and ',' in location else location,
            }
    return out


def _slug_like(value: Any) -> str:
    return re.sub(r'[^a-z0-9]+', '', norm(value))


def _load_wta_tournament_locations(root_dir: Path, year: int) -> Dict[str, Dict[str, str]]:
    candidates = [
        root_dir / 'docs' / f'wta_tournaments_{year}.json',
        root_dir / 'docs' / 'Tools' / f'wta_tournaments_{year}.json',
        root_dir / 'docs' / 'tools' / f'wta_tournaments_{year}.json',
    ]
    p = next((x for x in candidates if x.exists()), None)
    if p is None:
        return {}

    reverse_map = load_country_reverse_map(root_dir)
    data = load_json(p)
    out: Dict[str, Dict[str, str]] = {}

    for item in data.get('content', []):
        location = tp.clean_str(item.get('country') or item.get('location') or item.get('city'))
        name = tp.clean_str(item.get('title') or item.get('name') or item.get('tournamentName') or item.get('tournament_name'))
        start_date = tp.clean_str(item.get('startDate') or item.get('start_date'))
        end_date = tp.clean_str(item.get('endDate') or item.get('end_date'))
        country_code = ''
        country_name = ''

        if location and len(location) == 3 and location.isalpha():
            country_code = location.upper()
            country_name = country_code
        elif location:
            candidate = location.split(',')[-1].strip() if ',' in location else location
            country_code = reverse_map.get(norm(candidate), '') or reverse_map.get(norm(location), '')
            country_name = candidate or location

        if not country_code and name:
            tail = name.split(',')[-1].strip() if ',' in name else name.split('-')[-1].strip()
            country_code = reverse_map.get(norm(tail), '') or country_code
            if not country_name:
                country_name = tail or name

        keys = {
            tp.clean_str(item.get('liveScoringId')),
            tp.clean_str(item.get('id')),
            tp.clean_str(item.get('tournamentGroup', {}).get('id') if isinstance(item.get('tournamentGroup'), dict) else ''),
            tp.clean_str(item.get('tournamentLink')),
            _slug_like(name),
            _slug_like(item.get('tournamentLink')),
            _slug_like(item.get('venue_name')),
            _slug_like(item.get('city')),
            _slug_like(f'{name} {start_date} {end_date}'),
        }
        for key in keys:
            if not key:
                continue
            out[key] = {
                'country_code': country_code,
                'country_name': country_name or country_code,
            }
    return out


def load_match_location_map(root_dir: Path, circuit: str, year: int, reverse_map: Dict[str, str]) -> Dict[str, Dict[str, Any]]:
    matches = tp.load_matches(root_dir, circuit, year, 'historical')

    if circuit.upper() == 'ATP':
        tournament_locations = load_atp_tournament_locations(root_dir, year)
    else:
        tournament_locations = _load_wta_tournament_locations(root_dir, year)

    out: Dict[str, Dict[str, Any]] = {}
    for match in matches:
        match_key = tp.clean_str(match.get('match_key'))
        if not match_key:
            continue

        code = tp.clean_str(match.get('event_country_code')).upper()
        name = tp.clean_str(match.get('event_country_name'))

        if not code or code in {'UNK', 'NONE', 'NULL'}:
            event_id = tp.clean_str(match.get('event_id'))
            event_key = tp.clean_str(match.get('event_key'))
            tourney_name = tp.clean_str(match.get('tourney_name'))
            start_date = match.get('start_date')
            end_date = match.get('end_date')

            candidates = [event_id, event_key, _slug_like(tourney_name)]
            if start_date or end_date:
                candidates.append(_slug_like(f'{tourney_name} {start_date} {end_date}'))

            found = None
            for key in candidates:
                if key and key in tournament_locations:
                    found = tournament_locations[key]
                    break

            if found is not None:
                code = tp.clean_str(found.get('country_code')).upper()
                name = tp.clean_str(found.get('country_name')) or name

        if not code or code in {'UNK', 'NONE', 'NULL'}:
            raw = tp.clean_str(match.get('country') or match.get('country_name') or match.get('location_country') or match.get('venue_country'))
            if raw:
                code = reverse_map.get(norm(raw), '') or raw.upper()
                name = raw

        out[match_key] = {
            'country_code': code,
            'country_name': name or code,
            'event_id': tp.clean_str(match.get('event_id')),
            'event_key': tp.clean_str(match.get('event_key')),
            'tourney_name': tp.clean_str(match.get('tourney_name')),
            'start_date': match.get('start_date').isoformat() if match.get('start_date') else None,
            'end_date': match.get('end_date').isoformat() if match.get('end_date') else None,
        }
    return out


def iter_files(root_dir: Path, circuit: str, pattern: str) -> List[Path]:
    base = root_dir / 'docs' / 'matches' / f'{circuit.lower()}_matches'
    if not base.exists():
        return []
    return sorted([p for p in base.rglob(pattern) if p.is_file()])


def years_from_files(files: Sequence[Path]) -> List[int]:
    years = set()
    for p in files:
        years.update(int(m.group(1)) for m in re.finditer(r'(20\d{2})', p.name))
    return sorted(years)


def load_all_matches(root_dir: Path, circuit: str) -> List[Dict[str, Any]]:
    files = iter_files(root_dir, circuit, '*.csv')
    years = years_from_files(files)
    matches: List[Dict[str, Any]] = []
    for year in years:
        try:
            matches.extend(tp.load_matches(root_dir, circuit, year, 'historical'))
        except FileNotFoundError:
            pass
    # Deduplicate by match_key.
    seen = set()
    out = []
    for m in matches:
        mk = m.get('match_key')
        if mk in seen:
            continue
        seen.add(mk)
        out.append(m)
    return out


def load_period_matches(root_dir: Path, circuit: str, year: int, period: str, start: Optional[date], end: Optional[date]) -> List[Dict[str, Any]]:
    return tp.load_matches(root_dir, circuit, year, period, start=start, end=end)


def filter_participations(participations: Iterable[tp.Participation], countries: Sequence[str]) -> List[tp.Participation]:
    allowed = {c.upper() for c in countries}
    return [p for p in participations if tp.clean_str(p.country_code).upper() in allowed]


def group_by_country_summaries(summaries: Dict[str, tp.PlayerSummary], participations: Iterable[tp.Participation]) -> Dict[str, Dict[str, tp.PlayerSummary]]:
    out: Dict[str, Dict[str, tp.PlayerSummary]] = defaultdict(dict)
    for p in participations:
        s = summaries.get(p.player_id)
        if s is None:
            continue
        out[tp.clean_str(s.country_code).upper()][p.player_id] = s
    return out


def load_ranking_history(root_dir: Path, circuit: str, latest_map: Dict[str, Dict[str, Any]]) -> Dict[str, List[Dict[str, Any]]]:
    base_candidates = [
        root_dir / 'docs' / f'{circuit.lower()}_rankings',
        root_dir / 'docs' / f'{circuit.upper()}_rankings',
        root_dir / 'docs' / 'rankings' / f'{circuit.lower()}_rankings',
    ]
    base = next((p for p in base_candidates if p.exists()), None)
    if base is None:
        return {}
    name_to_pid = {norm(tp.clean_str(row.get('full_name') or row.get('player_name'))): pid for pid, row in latest_map.items()}
    history: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    for path in sorted(base.rglob('*.csv')):
        with path.open('r', encoding='utf-8-sig', newline='') as fh:
            reader = csv.DictReader(fh)
            for row in reader:
                ranking = tp.parse_int(row.get('ranking'))
                if ranking is None:
                    continue
                full_name = tp.clean_str(row.get('full_name') or row.get('player_name'))
                player_id = tp.clean_str(row.get('player_id')) or name_to_pid.get(norm(full_name), '')
                if not player_id:
                    continue
                history[player_id].append({
                    'date': tp.clean_str(row.get('date')),
                    'ranking': ranking,
                    'points': tp.parse_int(row.get('points')) or 0,
                    'full_name': full_name,
                    'source_file': path.name,
                })
    for pid in history:
        history[pid].sort(key=lambda x: (x.get('date') or '', x.get('ranking') or 10**9))
    return history


def current_ranking_players(latest_map: Dict[str, Dict[str, Any]], countries: Sequence[str]) -> List[Dict[str, Any]]:
    allowed = {c.upper() for c in countries}
    rows = []
    for pid, row in latest_map.items():
        cc = tp.clean_str(row.get('country_code')).upper()
        if cc not in allowed:
            continue
        rows.append({
            'player_id': pid,
            'player_name': tp.clean_str(row.get('full_name') or row.get('player_name')),
            'ranking': tp.parse_int(row.get('ranking')),
            'points': tp.parse_int(row.get('points')),
            'country_code': cc,
            'country_name': tp.clean_str(row.get('country_name'), default=cc),
            'evolution': tp.parse_int(row.get('evolution')),
            'evolution_year': tp.parse_int(row.get('evolution_year')),
            'evolution_this_year': tp.parse_int(row.get('evolution_this_year')),
            'ranked_last_week': tp.parse_bool(row.get('ranked_last_week')),
            'ranked_last_year': tp.parse_bool(row.get('ranked_last_year')),
            'ranked_beginning_year': tp.parse_bool(row.get('ranked_beginning_year')),
            'ever_ranked': tp.parse_bool(row.get('ever_ranked')),
            'circuit': tp.clean_str(row.get('circuit')),
        })
    rows.sort(key=lambda x: (x['country_code'], x['ranking'] if x['ranking'] is not None else 10**9, x['player_name']))
    return rows


def special_rank_events(player: tp.PlayerSummary, latest_row: Dict[str, Any], history: Dict[str, List[Dict[str, Any]]]) -> List[Dict[str, Any]]:
    out = []
    current = tp.parse_int(latest_row.get('ranking'))
    if current is None:
        return out
    prior_rows = history.get(player.player_id, [])
    for threshold, label in [(1, 'top_1'), (10, 'top_10'), (50, 'top_50'), (100, 'top_100')]:
        if current <= threshold and not any((tp.parse_int(r.get('ranking')) or 10**9) <= threshold for r in prior_rows if r.get('date') != latest_row.get('date')):
            out.append({
                'event_type': 'ranking_milestone',
                'tier': label,
                'player_id': player.player_id,
                'player_name': player.player_name,
                'country_code': player.country_code,
                'circuit': player.circuit,
                'current_ranking': current,
                'special_national': True,
                'special_regional': True,
                'regional_count': 0,
                'national_count': 0,
            })
    return out


def build_special_events(region: RegionConfig, weekly_participations: Sequence[tp.Participation], all_historical_participations: Sequence[tp.Participation], latest_rankings: Dict[str, Dict[str, Dict[str, Any]]], ranking_histories: Dict[str, Dict[str, List[Dict[str, Any]]]]) -> List[Dict[str, Any]]:
    region_set = {c.upper() for c in region.countries}
    hist_sets: Dict[str, set[str]] = defaultdict(set)
    hist_sets_country: Dict[str, Dict[str, set[str]]] = defaultdict(lambda: defaultdict(set))
    for p in all_historical_participations:
        cc = tp.clean_str(p.country_code).upper()
        if cc not in region_set:
            continue
        if p.round_label in {'MS001', 'LS001'}:
            key = f"final_{p.level_canonical}"
            hist_sets[key].add(p.player_id)
            hist_sets_country[key][cc].add(p.player_id)
        if p.round_label in {'MS002', 'MS003'}:
            key = f"semi_{p.level_canonical}"
            hist_sets[key].add(p.player_id)
            hist_sets_country[key][cc].add(p.player_id)
        if p.is_winner and p.opponent_rank is not None:
            for t in (10, 50, 100):
                if p.opponent_rank <= t:
                    key = f"beat_top_{t}"
                    hist_sets[key].add(p.player_id)
                    hist_sets_country[key][cc].add(p.player_id)

    latest_all = {**latest_rankings['ATP'], **latest_rankings['WTA']}
    events: List[Dict[str, Any]] = []
    for p in weekly_participations:
        cc = tp.clean_str(p.country_code).upper()
        if cc not in region_set:
            continue
        latest_row = latest_all.get(p.player_id, {})
        if p.round_label in {'MS001', 'LS001'}:
            event_type = 'tournament_win' if p.is_winner else 'tournament_final'
            key = f"{'final' if not p.is_winner else 'final'}_{p.level_canonical}"
            regional_count = len(hist_sets.get(key, set()))
            national_count = len(hist_sets_country.get(key, {}).get(cc, set()))
            events.append({
                'event_type': event_type,
                'player_id': p.player_id,
                'player_name': p.player_name,
                'country_code': cc,
                'country_name': p.country_name,
                'circuit': p.circuit,
                'tourney_name': p.tourney_name,
                'level': p.level_canonical,
                'round': p.round_label,
                'match_key': p.match_key,
                'match_date': p.match_date.isoformat() if p.match_date else None,
                'special_regional': regional_count < 10,
                'special_national': national_count < 10,
                'regional_count': regional_count,
                'national_count': national_count,
            })
        if p.round_label in {'MS002', 'MS003'}:
            key = f"semi_{p.level_canonical}"
            regional_count = len(hist_sets.get(key, set()))
            national_count = len(hist_sets_country.get(key, {}).get(cc, set()))
            events.append({
                'event_type': 'tournament_semifinal',
                'player_id': p.player_id,
                'player_name': p.player_name,
                'country_code': cc,
                'country_name': p.country_name,
                'circuit': p.circuit,
                'tourney_name': p.tourney_name,
                'level': p.level_canonical,
                'round': p.round_label,
                'match_key': p.match_key,
                'match_date': p.match_date.isoformat() if p.match_date else None,
                'special_regional': regional_count < 10,
                'special_national': national_count < 10,
                'regional_count': regional_count,
                'national_count': national_count,
            })
        if p.is_winner and p.opponent_rank is not None:
            for t in (10, 50, 100):
                if p.opponent_rank <= t:
                    key = f"beat_top_{t}"
                    regional_count = len(hist_sets.get(key, set()))
                    national_count = len(hist_sets_country.get(key, {}).get(cc, set()))
                    events.append({
                        'event_type': f'beat_top_{t}',
                        'player_id': p.player_id,
                        'player_name': p.player_name,
                        'country_code': cc,
                        'country_name': p.country_name,
                        'circuit': p.circuit,
                        'tourney_name': p.tourney_name,
                        'opponent_id': p.opponent_id,
                        'opponent_name': p.opponent_name,
                        'opponent_rank': p.opponent_rank,
                        'level': p.level_canonical,
                        'round': p.round_label,
                        'match_key': p.match_key,
                        'match_date': p.match_date.isoformat() if p.match_date else None,
                        'special_regional': regional_count < 10,
                        'special_national': national_count < 10,
                        'regional_count': regional_count,
                        'national_count': national_count,
                    })
        # ranking milestone events
        if p.player_id in latest_all:
            for ev in special_rank_events(p, latest_all[p.player_id], ranking_histories.get(p.circuit, {})):
                events.append(ev)

    dedup: List[Dict[str, Any]] = []
    seen = set()
    for ev in events:
        key = (ev.get('event_type'), ev.get('player_id'), ev.get('tourney_name'), ev.get('round'), ev.get('level'), ev.get('match_key'), ev.get('tier'))
        if key in seen:
            continue
        seen.add(key)
        dedup.append(ev)
    dedup.sort(key=lambda x: (x.get('country_code', ''), x.get('event_type', ''), x.get('player_name', ''), x.get('match_date') or ''))
    return dedup


def attach_countries_played(payloads: Dict[str, Dict[str, Any]], weekly_participations: Dict[str, List[tp.Participation]], yearly_participations: Dict[str, List[tp.Participation]], weekly_match_locations: Dict[str, Dict[str, Dict[str, Any]]], yearly_match_locations: Dict[str, Dict[str, Dict[str, Any]]], latest_rankings: Dict[str, Dict[str, Dict[str, Any]]], region_codes: Sequence[str]) -> None:
    region_set = {c.upper() for c in region_codes}

    def build_maps(participations: List[tp.Participation], locs: Dict[str, Dict[str, Any]]) -> Dict[str, Dict[str, Dict[str, Any]]]:
        # country played in -> player country -> stats
        out: Dict[str, Dict[str, Dict[str, Any]]] = defaultdict(lambda: defaultdict(lambda: {'matches': 0, 'wins': 0}))
        for p in participations:
            if tp.clean_str(p.country_code).upper() not in region_set:
                continue
            loc = locs.get(p.match_key, {})
            played_cc = tp.clean_str(loc.get('country_code')).upper()
            if not played_cc:
                continue
            out[played_cc][tp.clean_str(p.country_code).upper()]['matches'] += 1
            out[played_cc][tp.clean_str(p.country_code).upper()]['wins'] += 1 if p.is_winner else 0
        final: Dict[str, Dict[str, Any]] = {}
        for played_cc, by_player_country in out.items():
            total_matches = sum(v['matches'] for v in by_player_country.values())
            total_wins = sum(v['wins'] for v in by_player_country.values())
            final[played_cc] = {
                'country_code': played_cc,
                'matches': total_matches,
                'wins': total_wins,
                'win_rate': (total_wins / total_matches) if total_matches else None,
                'by_player_country': by_player_country,
            }
        return final

    def build_opponent_map(participations: List[tp.Participation], latest_map: Dict[str, Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
        buckets: Dict[str, Dict[str, int]] = defaultdict(lambda: {'matches': 0, 'wins': 0})
        for p in participations:
            if tp.clean_str(p.country_code).upper() not in region_set:
                continue
            opp = latest_map.get(p.opponent_id, {})
            opp_cc = tp.clean_str(opp.get('country_code')).upper()
            if not opp_cc:
                continue
            buckets[opp_cc]['matches'] += 1
            buckets[opp_cc]['wins'] += 1 if p.is_winner else 0
        return {cc: {'country_code': cc, 'matches': v['matches'], 'wins': v['wins'], 'win_rate': (v['wins'] / v['matches']) if v['matches'] else None} for cc, v in buckets.items()}

    weekly_atp = build_maps(weekly_participations['ATP'], weekly_match_locations['ATP'])
    weekly_wta = build_maps(weekly_participations['WTA'], weekly_match_locations['WTA'])
    yearly_atp = build_maps(yearly_participations['ATP'], yearly_match_locations['ATP'])
    yearly_wta = build_maps(yearly_participations['WTA'], yearly_match_locations['WTA'])

    weekly_opp_atp = build_opponent_map(weekly_participations['ATP'], latest_rankings['ATP'])
    weekly_opp_wta = build_opponent_map(weekly_participations['WTA'], latest_rankings['WTA'])
    yearly_opp_atp = build_opponent_map(yearly_participations['ATP'], latest_rankings['ATP'])
    yearly_opp_wta = build_opponent_map(yearly_participations['WTA'], latest_rankings['WTA'])

    for cc, payload in payloads.items():
        payload['weekly']['ATP']['matches_played_in_countries'] = weekly_atp
        payload['weekly']['WTA']['matches_played_in_countries'] = weekly_wta
        payload['current_year']['ATP']['matches_played_in_countries'] = yearly_atp
        payload['current_year']['WTA']['matches_played_in_countries'] = yearly_wta
        payload['weekly']['ATP']['opponent_country_map'] = weekly_opp_atp
        payload['weekly']['WTA']['opponent_country_map'] = weekly_opp_wta
        payload['current_year']['ATP']['opponent_country_map'] = yearly_opp_atp
        payload['current_year']['WTA']['opponent_country_map'] = yearly_opp_wta


def build_region_payload(root_dir: Path, region: RegionConfig) -> Dict[str, Any]:
    latest = load_latest_rankings(root_dir)
    year = datetime.now(tz=PARIS_TZ).year
    week_start, week_end, year_start = current_period_dates()
    region_codes = [c.upper() for c in region.countries]

    weekly_matches = {
        'ATP': load_period_matches(root_dir, 'ATP', year, 'weekly', week_start, week_end),
        'WTA': load_period_matches(root_dir, 'WTA', year, 'weekly', week_start, week_end),
    }
    yearly_matches = {
        'ATP': load_period_matches(root_dir, 'ATP', year, 'current_year', year_start, week_end),
        'WTA': load_period_matches(root_dir, 'WTA', year, 'current_year', year_start, week_end),
    }
    historical_matches = {
        'ATP': load_all_matches(root_dir, 'ATP'),
        'WTA': load_all_matches(root_dir, 'WTA'),
    }

    weekly_parts = {c: filter_participations(tp.build_participations(ms, latest[c]), region_codes) for c, ms in weekly_matches.items()}
    yearly_parts = {c: filter_participations(tp.build_participations(ms, latest[c]), region_codes) for c, ms in yearly_matches.items()}
    historical_parts = {c: filter_participations(tp.build_participations(ms, latest[c]), region_codes) for c, ms in historical_matches.items()}

    weekly_summaries = {c: tp.summarize_players(weekly_parts[c], latest[c], 'weekly') for c in ('ATP', 'WTA')}
    yearly_summaries = {c: tp.summarize_players(yearly_parts[c], latest[c], 'current_year') for c in ('ATP', 'WTA')}

    weekly_by_country: Dict[str, Dict[str, tp.PlayerSummary]] = defaultdict(dict)
    yearly_by_country: Dict[str, Dict[str, tp.PlayerSummary]] = defaultdict(dict)
    for c in ('ATP', 'WTA'):
        for pid, s in weekly_summaries[c].items():
            weekly_by_country[tp.clean_str(s.country_code).upper()][pid] = s
        for pid, s in yearly_summaries[c].items():
            yearly_by_country[tp.clean_str(s.country_code).upper()][pid] = s

    country_payloads: Dict[str, Dict[str, Any]] = {}
    country_names: Dict[str, str] = {}
    for c in ('ATP', 'WTA'):
        for p in weekly_parts[c] + yearly_parts[c]:
            country_names.setdefault(tp.clean_str(p.country_code).upper(), p.country_name or p.country_code)
    for cc in region_codes:
        payload = tp.build_country_payload(
            cc,
            country_names.get(cc, cc),
            {
                'weekly': {
                    'ATP': [s for s in weekly_by_country.get(cc, {}).values() if s.circuit == 'ATP'],
                    'WTA': [s for s in weekly_by_country.get(cc, {}).values() if s.circuit == 'WTA'],
                },
                'current_year': {
                    'ATP': [s for s in yearly_by_country.get(cc, {}).values() if s.circuit == 'ATP'],
                    'WTA': [s for s in yearly_by_country.get(cc, {}).values() if s.circuit == 'WTA'],
                },
            },
            {
                'weekly': weekly_parts,
                'current_year': yearly_parts,
            },
        )
        country_payloads[cc] = payload

    reverse_map = load_country_reverse_map(root_dir)
    weekly_loc = {
        'ATP': load_match_location_map(root_dir, 'ATP', year, reverse_map),
        'WTA': load_match_location_map(root_dir, 'WTA', year, reverse_map),
    }
    yearly_loc = weekly_loc
    attach_countries_played(country_payloads, weekly_parts, yearly_parts, weekly_loc, yearly_loc, latest, region_codes)

    latest_all = {**latest['ATP'], **latest['WTA']}
    ranking_histories = {
        'ATP': load_ranking_history(root_dir, 'ATP', latest['ATP']),
        'WTA': load_ranking_history(root_dir, 'WTA', latest['WTA']),
    }
    special_events = build_special_events(region, [p for parts in weekly_parts.values() for p in parts], [p for parts in historical_parts.values() for p in parts], latest, ranking_histories)

    region_payload: Dict[str, Any] = {
        'region_code': region.region_code,
        'region_name': region.region_name,
        'countries': country_payloads,
        'weekly_players': {
            'ATP': [tp.player_summary_to_dict(s) for s in weekly_summaries['ATP'].values() if tp.clean_str(s.country_code).upper() in region_codes and s.ranking is not None],
            'WTA': [tp.player_summary_to_dict(s) for s in weekly_summaries['WTA'].values() if tp.clean_str(s.country_code).upper() in region_codes and s.ranking is not None],
        },
        'current_year_players': {
            'ATP': [tp.player_summary_to_dict(s) for s in yearly_summaries['ATP'].values() if tp.clean_str(s.country_code).upper() in region_codes and s.ranking is not None],
            'WTA': [tp.player_summary_to_dict(s) for s in yearly_summaries['WTA'].values() if tp.clean_str(s.country_code).upper() in region_codes and s.ranking is not None],
        },
        'special_events_weekly': special_events,
        'meta': {
            'week_start': week_start.isoformat(),
            'week_end': week_end.isoformat(),
            'year_start': year_start.isoformat(),
            'weekly_participations': {c: len(weekly_parts[c]) for c in ('ATP', 'WTA')},
            'current_year_participations': {c: len(yearly_parts[c]) for c in ('ATP', 'WTA')},
        },
    }

    # Region-level country contribution and density.
    country_rows: Dict[str, Dict[str, Any]] = {}
    for cc, payload in country_payloads.items():
        atp_rows = payload['current_year'].get('ATP', {}).get('ranked_players', [])
        wta_rows = payload['current_year'].get('WTA', {}).get('ranked_players', [])
        all_rows = atp_rows + wta_rows
        if not all_rows:
            continue
        mass = sum((r.get('performance_index') or 0.0) for r in all_rows)
        eff = mass / len(all_rows) if all_rows else None
        country_rows[cc] = {
            'country_code': cc,
            'mass': mass,
            'efficiency': eff,
            'atp_players_count': len(atp_rows),
            'wta_players_count': len(wta_rows),
            'total_players_count': len(all_rows),
        }
    country_rows_sorted = sorted(country_rows.values(), key=lambda x: (-x['mass'], x['country_code']))
    for i, row in enumerate(country_rows_sorted, start=1):
        row['mass_rank'] = i
    for i, row in enumerate(sorted(country_rows.values(), key=lambda x: (-(x['efficiency'] or -1e9), x['country_code'])), start=1):
        row['efficiency_rank'] = i
    region_payload['country_contribution'] = {
        cc: {**row, 'share_of_region_mass': (row['mass'] / sum(r['mass'] for r in country_rows.values())) if country_rows else None}
        for cc, row in country_rows.items()
    }
    region_payload['country_rankings'] = {'countries': [{ 'country_code': cc, **info } for cc, info in sorted(region_payload['country_contribution'].items(), key=lambda item: item[1].get('mass_rank', 10**9))]}
    region_payload['ranking_density'] = {
        cc: {
            'country_code': cc,
            'players_count': len((payload['current_year'].get('ATP', {}).get('ranked_players', []) or []) + (payload['current_year'].get('WTA', {}).get('ranked_players', []) or [])),
            'bins': {
                'top_10': sum(1 for r in (payload['current_year'].get('ATP', {}).get('ranked_players', []) + payload['current_year'].get('WTA', {}).get('ranked_players', [])) if (r.get('ranking') or 10**9) <= 10),
                'top_50': sum(1 for r in (payload['current_year'].get('ATP', {}).get('ranked_players', []) + payload['current_year'].get('WTA', {}).get('ranked_players', [])) if 10 < (r.get('ranking') or 10**9) <= 50),
                'top_100': sum(1 for r in (payload['current_year'].get('ATP', {}).get('ranked_players', []) + payload['current_year'].get('WTA', {}).get('ranked_players', [])) if 50 < (r.get('ranking') or 10**9) <= 100),
                'top_250': sum(1 for r in (payload['current_year'].get('ATP', {}).get('ranked_players', []) + payload['current_year'].get('WTA', {}).get('ranked_players', [])) if 100 < (r.get('ranking') or 10**9) <= 250),
                '250_plus': sum(1 for r in (payload['current_year'].get('ATP', {}).get('ranked_players', []) + payload['current_year'].get('WTA', {}).get('ranked_players', [])) if (r.get('ranking') or 10**9) > 250),
                'unranked': sum(1 for r in (payload['current_year'].get('ATP', {}).get('ranked_players', []) + payload['current_year'].get('WTA', {}).get('ranked_players', [])) if r.get('ranking') is None),
            },
        }
        for cc, payload in country_payloads.items()
    }
    return region_payload


def run_region_report(root_dir: Path, output_dir: Path, region: RegionConfig) -> Path:
    payload = build_region_payload(root_dir, region)
    output_dir.mkdir(parents=True, exist_ok=True)
    out = output_dir / region.output_filename
    write_json(out, payload)
    return out
