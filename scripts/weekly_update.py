from __future__ import annotations

import argparse
from collections import Counter, defaultdict
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple
from zoneinfo import ZoneInfo

import tennis_performance as tp

PARIS_TZ = ZoneInfo("Europe/Paris")

STAT_RANKING_DIRECTIONS: Dict[str, bool] = {
    "number_of_aces": True,
    "aces_per_service_point": True,
    "number_of_double_faults": False,
    "double_faults_per_service_point": False,
    "first_serve_percent": True,
    "first_serve_points_won_percent": True,
    "second_serve_points_won_percent": True,
    "service_points_won_percent": True,
    "return_points_won_percent": True,
    "breakpoints_faced": True,
    "breakpoints_converted_count": True,
    "breakpoints_converted_rate": True,
    "service_games_lost_rate": False,
    "tie_breaks_win_rate": True,
}


def current_period_dates(today: Optional[datetime] = None) -> Tuple[date, date, date]:
    now = today or datetime.now(tz=PARIS_TZ)
    current_monday = now.date() - timedelta(days=now.weekday())
    last_monday = current_monday - timedelta(days=7)
    return last_monday, current_monday, date(now.year, 1, 1)


def load_rankings(root_dir: Path) -> Tuple[Dict[str, Dict[str, Any]], Dict[str, Dict[str, Any]]]:
    candidates_atp = [
        root_dir / "docs" / "tools" / "latest_atp_ranking.json",
        root_dir / "docs" / "Tools" / "latest_atp_ranking.json",
        root_dir / "docs" / "latest_atp_ranking.json",
    ]
    candidates_wta = [
        root_dir / "docs" / "tools" / "latest_wta_ranking.json",
        root_dir / "docs" / "Tools" / "latest_wta_ranking.json",
        root_dir / "docs" / "latest_wta_ranking.json",
    ]
    atp_path = next((p for p in candidates_atp if p.exists()), None)
    wta_path = next((p for p in candidates_wta if p.exists()), None)
    if atp_path is None:
        raise FileNotFoundError("Impossible de trouver docs/tools/latest_atp_ranking.json")
    if wta_path is None:
        raise FileNotFoundError("Impossible de trouver docs/tools/latest_wta_ranking.json")
    return tp.load_ranking_json(atp_path), tp.load_ranking_json(wta_path)


def ranking_map_by_circuit(atp: Dict[str, Dict[str, Any]], wta: Dict[str, Dict[str, Any]]) -> Dict[str, Dict[str, Dict[str, Any]]]:
    return {"ATP": atp, "WTA": wta}


def _safe_country_code(value: Any) -> str:
    code = tp.normalize_country(value)
    return code if code else "UNK"


def _evolution_ranks(current_rank: Optional[int], evolution: Optional[int]) -> Tuple[Optional[int], Optional[int]]:
    if current_rank is None or evolution is None:
        return None, None
    if tp.RANK_EVOLUTION_POSITIVE_IS_RISE:
        start_rank = current_rank + evolution
    else:
        start_rank = current_rank - evolution
    return start_rank, current_rank


def _relation_rows(participations: Iterable[tp.Participation], attr_country_code: str, attr_country_name: str) -> List[Dict[str, Any]]:
    buckets: Dict[str, Dict[str, Any]] = {}
    for p in participations:
        code = _safe_country_code(getattr(p, attr_country_code, ""))
        if not code or code == "UNK":
            continue
        row = buckets.setdefault(
            code,
            {
                "country_code": code,
                "country_name": getattr(p, attr_country_name, "") or code,
                "matches": 0,
                "wins": 0,
            },
        )
        row["matches"] += 1
        if p.is_winner:
            row["wins"] += 1

    rows = list(buckets.values())
    for row in rows:
        row["win_rate"] = (row["wins"] / row["matches"]) if row["matches"] else None
    rows.sort(key=lambda r: (-r["matches"], -r["wins"], r["country_code"]))
    return rows


def _stat_rankings_for_block(country_payloads: Dict[str, Dict[str, Any]], period: str, circuit: str) -> Dict[str, Dict[str, Any]]:
    rows: List[Tuple[str, Dict[str, Any]]] = []
    for cc, payload in country_payloads.items():
        block = payload.get(period, {}).get(circuit, {})
        stats = block.get("stats", {})
        if stats:
            rows.append((cc, stats))

    rankings: Dict[str, Dict[str, Any]] = {cc: {} for cc, _ in rows}
    for metric, higher_is_better in STAT_RANKING_DIRECTIONS.items():
        metric_rows = []
        for cc, stats in rows:
            value = stats.get(metric)
            if value is None:
                continue
            metric_rows.append((cc, float(value)))
        metric_rows.sort(key=lambda x: (-x[1], x[0]) if higher_is_better else (x[1], x[0]))

        rank = 0
        last_value = None
        for idx, (cc, value) in enumerate(metric_rows, start=1):
            if last_value is None or value != last_value:
                rank = idx
                last_value = value
            rankings.setdefault(cc, {})[metric] = rank
            rankings[cc][f"{metric}_direction"] = "desc" if higher_is_better else "asc"
    return rankings


def aggregate_country_circuit(
    country_code: str,
    circuit: str,
    participations: List[tp.Participation],
    summaries: Dict[str, tp.PlayerSummary],
    ranking_map: Dict[str, Dict[str, Any]],
) -> Dict[str, Any]:
    player_ids = {p.player_id for p in participations}
    player_summaries = [summaries[pid] for pid in player_ids if pid in summaries]
    player_summaries_ranked = [s for s in player_summaries if s.ranking is not None]
    player_summaries_ranked.sort(key=lambda s: (s.ranking or 10**9, s.player_name))
    player_summaries_by_matches = sorted(player_summaries, key=lambda s: (-s.matches, s.player_name))
    player_summaries_by_points = sorted(player_summaries, key=lambda s: (-s.points_earned, s.player_name))

    unique_matches = len({p.match_key for p in participations})
    tournament_counter = Counter(p.tourney_name for p in participations if p.tourney_name)
    top_tourney_name = tournament_counter.most_common(1)[0][0] if tournament_counter else None

    significant_wins: List[Dict[str, Any]] = []
    very_significant_wins: List[Dict[str, Any]] = []
    significant_losses: List[Dict[str, Any]] = []
    very_significant_losses: List[Dict[str, Any]] = []
    tournaments_won: List[Dict[str, Any]] = []

    for p in participations:
        base = {
            "player_id": p.player_id,
            "player_name": p.player_name,
            "opponent_id": p.opponent_id,
            "opponent_name": p.opponent_name,
            "player_rank": p.player_rank,
            "opponent_rank": p.opponent_rank,
            "tourney_name": p.tourney_name,
            "level": p.level_canonical,
            "round": p.round_raw,
            "match_date": p.match_date.isoformat() if p.match_date else None,
            "match_key": p.match_key,
        }
        if p.is_winner and p.significant_win:
            significant_wins.append(base)
        if p.is_winner and p.very_significant_win:
            very_significant_wins.append(base)
        if p.is_winner and tp.is_final_match_code(p.round_code):
            tournaments_won.append({
                "player_id": p.player_id,
                "player_name": p.player_name,
                "tourney_name": p.tourney_name,
                "level": p.level_canonical,
                "round": p.round_raw,
                "match_date": p.match_date.isoformat() if p.match_date else None,
                "match_key": p.match_key,
            })
        if (not p.is_winner) and p.significant_loss:
            significant_losses.append(base)
        if (not p.is_winner) and p.very_significant_loss:
            very_significant_losses.append(base)

    relevant_players = {
        p.player_id: ranking_map.get(p.player_id, {})
        for p in participations
        if p.player_id in ranking_map
    }

    def evolution_bucket(field_name: str, very: bool = False) -> Dict[str, List[Dict[str, Any]]]:
        bucket = {"rise": [], "drop": []}
        for pid, entry in relevant_players.items():
            summary = summaries.get(pid)
            if summary is None or summary.ranking is None:
                continue
            evo = tp.parse_int(entry.get(field_name))
            if evo is None or evo == 0:
                continue
            is_sig = tp.is_significant_evolution(summary.ranking, evo, very=very)
            if not is_sig:
                continue
            direction = tp.evolution_direction(evo)
            ranking_from, ranking_to = _evolution_ranks(summary.ranking, evo)
            item = {
                "player_id": pid,
                "player_name": summary.player_name,
                "ranking_from": ranking_from,
                "ranking_to": ranking_to,
                "ranking": summary.ranking,
                "evolution": evo,
                "direction": direction,
                "ranked_last_week": summary.ranked_last_week,
                "ranked_last_year": summary.ranked_last_year,
                "ranked_beginning_year": summary.ranked_beginning_year,
                "ever_ranked": summary.ever_ranked,
            }
            bucket[direction].append(item)
        bucket["rise"].sort(key=lambda x: (-abs(x["evolution"]), x["ranking_to"] or 10**9, x["player_name"]))
        bucket["drop"].sort(key=lambda x: (-abs(x["evolution"]), x["ranking_to"] or 10**9, x["player_name"]))
        return bucket

    ranking_evolutions = {
        "last_week": {
            "significant": evolution_bucket("evolution", very=False),
            "very_significant": evolution_bucket("evolution", very=True),
        },
        "last_year": {
            "significant": evolution_bucket("evolution_year", very=False),
            "very_significant": evolution_bucket("evolution_year", very=True),
        },
        "beginning_year": {
            "significant": evolution_bucket("evolution_this_year", very=False),
            "very_significant": evolution_bucket("evolution_this_year", very=True),
        },
    }

    new_players = []
    for pid, _entry in relevant_players.items():
        summary = summaries.get(pid)
        if not summary:
            continue
        if (
            summary.ever_ranked is False
            and summary.ranked_last_week is False
            and summary.ranked_last_year is False
            and summary.ranked_beginning_year is False
        ):
            new_players.append({
                "player_id": pid,
                "player_name": summary.player_name,
                "ranking": summary.ranking,
            })

    ranked_players = [tp.player_summary_to_dict(s) for s in player_summaries_ranked]
    stats = tp.combine_stats(player_summaries)
    stats["unique_matches"] = unique_matches
    stats["player_match_entries"] = sum(s.matches for s in player_summaries)
    stats["top_tourney_name"] = top_tourney_name

    opponent_countries = _relation_rows(participations, "opponent_country_code", "opponent_country_name")
    played_countries = _relation_rows(participations, "event_country_code", "event_country_name")

    return {
        "matches": unique_matches,
        "player_match_entries": stats["player_match_entries"],
        "top_tourney_name": top_tourney_name,
        "players_count": len(player_summaries),
        "players": ranked_players,
        "ranked_players": ranked_players,
        "top_players_by_matches": [
            {"player_id": s.player_id, "player_name": s.player_name, "ranking": s.ranking, "matches": s.matches}
            for s in player_summaries_by_matches[:5]
        ],
        "top_players_by_points": [
            {"player_id": s.player_id, "player_name": s.player_name, "ranking": s.ranking, "points_earned": s.points_earned}
            for s in player_summaries_by_points[:5]
        ],
        "tournaments_won": tournaments_won,
        "significant_wins": significant_wins,
        "very_significant_wins": very_significant_wins,
        "significant_losses": significant_losses,
        "very_significant_losses": very_significant_losses,
        "ranking_evolutions": ranking_evolutions,
        "new_players": new_players,
        "opponent_countries": opponent_countries,
        "played_countries": played_countries,
        "stats": stats,
    }


def aggregate_country_payload(
    country_code: str,
    country_name: str,
    period_participations: Dict[str, List[tp.Participation]],
    period_summaries: Dict[str, Dict[str, tp.PlayerSummary]],
    ranking_maps: Dict[str, Dict[str, Dict[str, Any]]],
) -> Dict[str, Any]:
    payload: Dict[str, Any] = {
        "country_code": country_code,
        "country_name": country_name,
        "weekly": {},
        "current_year": {},
    }
    for period in ["weekly", "current_year"]:
        period_part = period_participations.get(period, [])
        period_sum = period_summaries.get(period, {})
        for circuit in ["ATP", "WTA"]:
            participations = [p for p in period_part if p.circuit == circuit and p.country_code == country_code]
            payload[period][circuit] = aggregate_country_circuit(
                country_code,
                circuit,
                participations,
                period_sum,
                ranking_maps[circuit],
            )
    return payload


def build_country_rank_tables(
    year_country_participations: Dict[str, List[tp.Participation]],
    year_country_summaries: Dict[str, Dict[str, tp.PlayerSummary]],
) -> Dict[str, Dict[str, Any]]:
    rows: Dict[str, Dict[str, Any]] = {}
    for country_code, participations in year_country_participations.items():
        summaries = year_country_summaries.get(country_code, {})
        player_summaries = [summaries[pid] for pid in {p.player_id for p in participations} if pid in summaries]
        if not player_summaries:
            continue
        atp = [s for s in player_summaries if s.circuit == "ATP"]
        wta = [s for s in player_summaries if s.circuit == "WTA"]
        all_players = atp + wta
        mass = sum(p.performance_index for p in all_players)
        efficiency = mass / len(all_players) if all_players else None

        def vector(players: List[tp.PlayerSummary]) -> Dict[str, float]:
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
                "ranking": sum((p.ranking or 1000) for p in players) / len(players),
                "performance": sum(p.performance_index for p in players) / len(players),
                "win_rate": sum((p.wins / p.matches) if p.matches else 0.0 for p in players) / len(players),
                "significant_win_rate": sum((p.significant_wins / p.matches) if p.matches else 0.0 for p in players) / len(players),
                "avg_opponent_rank": sum((p.avg_opponent_rank or 1000.0) for p in players) / len(players),
                "points": sum(p.points_earned for p in players) / len(players),
            }

        atp_vec = vector(atp)
        wta_vec = vector(wta)
        coherence_distance = (
            abs(atp_vec["ranking"] - wta_vec["ranking"]) / 100.0
            + abs(atp_vec["performance"] - wta_vec["performance"]) / 100.0
            + abs(atp_vec["win_rate"] - wta_vec["win_rate"]) * 2.0
            + abs(atp_vec["significant_win_rate"] - wta_vec["significant_win_rate"]) * 2.0
            + abs(atp_vec["avg_opponent_rank"] - wta_vec["avg_opponent_rank"]) / 200.0
            + abs(atp_vec["points"] - wta_vec["points"]) / 1000.0
        )
        rows[country_code] = {
            "country_code": country_code,
            "mass": mass,
            "efficiency": efficiency,
            "coherence_distance": coherence_distance,
            "atp_players_count": len(atp),
            "wta_players_count": len(wta),
            "total_players_count": len(all_players),
            "atp_vector": atp_vec,
            "wta_vector": wta_vec,
        }

    mass_ranked = sorted(rows.values(), key=lambda x: (-x["mass"], x["country_code"]))
    efficiency_ranked = sorted(rows.values(), key=lambda x: (-(x["efficiency"] if x["efficiency"] is not None else -1e9), x["country_code"]))
    coherence_ranked = sorted(rows.values(), key=lambda x: (x["coherence_distance"], x["country_code"]))
    for i, row in enumerate(mass_ranked, start=1):
        row["mass_rank"] = i
    for i, row in enumerate(efficiency_ranked, start=1):
        row["efficiency_rank"] = i
    for i, row in enumerate(coherence_ranked, start=1):
        row["coherence_rank"] = i

    return rows


def _ranking_export_rows(
    country_payloads: Dict[str, Dict[str, Any]],
    country_rankings: Dict[str, Dict[str, Any]],
    rank_key: str,
    value_key: str,
) -> Dict[str, Any]:
    rows: List[Dict[str, Any]] = []
    for cc, info in country_rankings.items():
        payload = country_payloads.get(cc, {})
        rows.append(
            {
                "country_code": cc,
                "country_name": payload.get("country_name", cc),
                "rank": info.get(rank_key),
                "points": info.get(value_key),
            }
        )
    rows.sort(key=lambda row: (row["rank"] if row["rank"] is not None else 10**9, row["country_code"]))
    return {
        "ranking": value_key,
        "countries": rows,
        "count": len(rows),
    }


def _inject_stat_rankings(country_payloads: Dict[str, Dict[str, Any]]) -> None:
    for period in ["weekly", "current_year"]:
        for circuit in ["ATP", "WTA"]:
            rankings = _stat_rankings_for_block(country_payloads, period, circuit)
            for cc, payload in country_payloads.items():
                block = payload.get(period, {}).get(circuit, {})
                stats = block.get("stats")
                if not isinstance(stats, dict):
                    continue
                stats.setdefault("country_ranks", {})
                stats.setdefault("country_rank_directions", {})
                stats["country_ranks"].update(rankings.get(cc, {}))
                # Keep only the direction keys relevant for metrics.
                for metric in STAT_RANKING_DIRECTIONS:
                    direction_key = f"{metric}_direction"
                    if direction_key in rankings.get(cc, {}):
                        stats["country_rank_directions"][metric] = rankings[cc][direction_key]


def write_outputs(
    root_dir: Path,
    output_dir: Path,
    country_payloads: Dict[str, Dict[str, Any]],
    country_rankings: Dict[str, Dict[str, Any]],
    weekly_player_summaries: Dict[str, Dict[str, tp.PlayerSummary]],
    year_player_summaries: Dict[str, Dict[str, tp.PlayerSummary]],
) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)
    countries_dir = output_dir / "countries"
    players_dir = output_dir / "players"
    countries_dir.mkdir(parents=True, exist_ok=True)
    players_dir.mkdir(parents=True, exist_ok=True)

    _inject_stat_rankings(country_payloads)

    for country_code, payload in sorted(country_payloads.items()):
        ranking_info = country_rankings.get(country_code, {})
        payload["indices"] = {
            "year_mass": ranking_info.get("mass"),
            "year_efficiency": ranking_info.get("efficiency"),
            "year_coherence_distance": ranking_info.get("coherence_distance"),
            "year_mass_rank": ranking_info.get("mass_rank"),
            "year_efficiency_rank": ranking_info.get("efficiency_rank"),
            "year_coherence_rank": ranking_info.get("coherence_rank"),
            "atp_vector": ranking_info.get("atp_vector"),
            "wta_vector": ranking_info.get("wta_vector"),
            "atp_players_count": ranking_info.get("atp_players_count"),
            "wta_players_count": ranking_info.get("wta_players_count"),
            "total_players_count": ranking_info.get("total_players_count"),
        }
        tp.json_dump(countries_dir / f"{tp.safe_filename_slug(country_code)}.json", payload)

    def player_table(period_map: Dict[str, Dict[str, tp.PlayerSummary]]) -> Dict[str, Any]:
        rows: List[Dict[str, Any]] = []
        for circuit, player_map in period_map.items():
            for summary in player_map.values():
                rows.append(tp.player_summary_to_dict(summary))
        rows.sort(key=lambda x: (x["circuit"], x["ranking"] if x["ranking"] is not None else 10**9, x["player_name"]))
        return {"players": rows, "count": len(rows)}

    tp.json_dump(players_dir / "weekly_players.json", player_table(weekly_player_summaries))
    tp.json_dump(players_dir / "current_year_players.json", player_table(year_player_summaries))
    tp.json_dump(
        output_dir / "country_rankings.json",
        {
            "countries": [
                {"country_code": cc, **info}
                for cc, info in sorted(country_rankings.items(), key=lambda item: item[1].get("mass_rank", 10**9))
            ]
        },
    )

    ranking_exports = {
        "country_rankings_by_mass.json": _ranking_export_rows(country_payloads, country_rankings, "mass_rank", "mass"),
        "country_rankings_by_efficiency.json": _ranking_export_rows(country_payloads, country_rankings, "efficiency_rank", "efficiency"),
        "country_rankings_by_coherence.json": _ranking_export_rows(country_payloads, country_rankings, "coherence_rank", "coherence_distance"),
    }
    for filename, payload in ranking_exports.items():
        tp.json_dump(output_dir / filename, payload)


def main() -> None:
    parser = argparse.ArgumentParser(description="Weekly ATP/WTA country statistics generator")
    parser.add_argument("--root-dir", type=Path, default=Path("."), help="Project root containing docs/")
    parser.add_argument("--output-dir", type=Path, default=Path("docs/generated/weekly_update"), help="Output directory")
    args = parser.parse_args()

    root_dir = args.root_dir.resolve()
    output_dir = args.output_dir.resolve()

    atp_rankings, wta_rankings = load_rankings(root_dir)
    ranking_maps = ranking_map_by_circuit(atp_rankings, wta_rankings)

    year = datetime.now(tz=PARIS_TZ).year
    week_start, week_end, year_start = current_period_dates()

    weekly_matches = {
        "ATP": tp.load_matches(root_dir, "ATP", year, "weekly", start=week_start, end=week_end),
        "WTA": tp.load_matches(root_dir, "WTA", year, "weekly", start=week_start, end=week_end),
    }
    weekly_participations = {
        circuit: tp.build_participations(matches, ranking_maps[circuit])
        for circuit, matches in weekly_matches.items()
    }
    weekly_summaries = {
        circuit: tp.summarize_players(participations, ranking_maps[circuit], "weekly")
        for circuit, participations in weekly_participations.items()
    }

    ytd_matches = {
        "ATP": tp.load_matches(root_dir, "ATP", year, "current_year", start=year_start, end=week_end),
        "WTA": tp.load_matches(root_dir, "WTA", year, "current_year", start=year_start, end=week_end),
    }
    ytd_participations = {
        circuit: tp.build_participations(matches, ranking_maps[circuit])
        for circuit, matches in ytd_matches.items()
    }
    ytd_summaries = {
        circuit: tp.summarize_players(participations, ranking_maps[circuit], "current_year")
        for circuit, participations in ytd_participations.items()
    }

    all_country_codes = set()
    country_name_lookup: Dict[str, str] = {}
    for circuit in ["ATP", "WTA"]:
        for p in weekly_participations[circuit]:
            if p.country_code:
                all_country_codes.add(p.country_code)
                country_name_lookup.setdefault(p.country_code, p.country_name or p.country_code)
            if p.opponent_country_code:
                country_name_lookup.setdefault(p.opponent_country_code, p.opponent_country_name or p.opponent_country_code)
            if p.event_country_code:
                country_name_lookup.setdefault(p.event_country_code, p.event_country_name or p.event_country_code)
        for p in ytd_participations[circuit]:
            if p.country_code:
                all_country_codes.add(p.country_code)
                country_name_lookup.setdefault(p.country_code, p.country_name or p.country_code)
            if p.opponent_country_code:
                country_name_lookup.setdefault(p.opponent_country_code, p.opponent_country_name or p.opponent_country_code)
            if p.event_country_code:
                country_name_lookup.setdefault(p.event_country_code, p.event_country_name or p.event_country_code)

    weekly_by_country: Dict[str, List[tp.Participation]] = defaultdict(list)
    ytd_by_country: Dict[str, List[tp.Participation]] = defaultdict(list)
    weekly_summaries_by_country: Dict[str, Dict[str, tp.PlayerSummary]] = defaultdict(dict)
    ytd_summaries_by_country: Dict[str, Dict[str, tp.PlayerSummary]] = defaultdict(dict)

    for circuit in ["ATP", "WTA"]:
        for p in weekly_participations[circuit]:
            weekly_by_country[p.country_code].append(p)
        for p in ytd_participations[circuit]:
            ytd_by_country[p.country_code].append(p)
        for pid, summary in weekly_summaries[circuit].items():
            weekly_summaries_by_country[summary.country_code][pid] = summary
        for pid, summary in ytd_summaries[circuit].items():
            ytd_summaries_by_country[summary.country_code][pid] = summary

    country_payloads: Dict[str, Dict[str, Any]] = {}
    for cc in sorted(all_country_codes):
        country_name = country_name_lookup.get(cc, cc)
        payload = aggregate_country_payload(
            cc,
            country_name,
            {"weekly": weekly_by_country.get(cc, []), "current_year": ytd_by_country.get(cc, [])},
            {"weekly": weekly_summaries_by_country.get(cc, {}), "current_year": ytd_summaries_by_country.get(cc, {})},
            ranking_maps,
        )
        country_payloads[cc] = payload

    country_rankings = build_country_rank_tables(ytd_by_country, ytd_summaries_by_country)

    write_outputs(
        root_dir=root_dir,
        output_dir=output_dir,
        country_payloads=country_payloads,
        country_rankings=country_rankings,
        weekly_player_summaries=weekly_summaries,
        year_player_summaries=ytd_summaries,
    )


if __name__ == "__main__":
    main()
