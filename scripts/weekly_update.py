from __future__ import annotations

import argparse
import json
from collections import Counter, defaultdict
from dataclasses import asdict
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple
from zoneinfo import ZoneInfo

import tennis_performance as tp

PARIS_TZ = ZoneInfo("Europe/Paris")


def current_period_dates(today: Optional[datetime] = None) -> Tuple[date, date, date]:
    now = today or datetime.now(tz=PARIS_TZ)
    current_monday = (now.date() - timedelta(days=now.weekday()))
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


def load_matches_and_participations(
    root_dir: Path,
    circuit: str,
    period: str,
    ranking_map: Dict[str, Dict[str, Any]],
    year: int,
    week_start: Optional[date] = None,
    week_end: Optional[date] = None,
) -> Tuple[List[Dict[str, Any]], List[tp.Participation], Dict[str, tp.PlayerSummary]]:
    matches = tp.load_matches(root_dir, circuit, year, period, start=week_start, end=week_end)
    participations = tp.build_participations(matches, ranking_map)
    summaries = tp.summarize_players(participations, ranking_map, period)
    return matches, participations, summaries


def entry_matches_for_country(participations: Iterable[tp.Participation], country_code: str, circuit: str) -> List[tp.Participation]:
    return [p for p in participations if p.country_code == country_code and p.circuit == circuit]


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
    tournament_counter = Counter(p.tourney_name for p in participations)
    top_tourney_name = tournament_counter.most_common(1)[0][0] if tournament_counter else None

    # Match-level notable events.
    significant_wins = []
    very_significant_wins = []
    significant_losses = []
    very_significant_losses = []
    tournaments_won = []
    for p in participations:
        if p.is_winner and p.significant_win:
            item = {
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
            significant_wins.append(item)
        if p.is_winner and p.very_significant_win:
            very_significant_wins.append({
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
            })
        if (p.is_winner and p.round_order <= 1) or p.round_label in {"F", "MS001", "LS001"}:
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
            significant_losses.append({
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
            })
        if (not p.is_winner) and p.very_significant_loss:
            very_significant_losses.append({
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
            })

    # Ranking evolutions for the players present in the country and circuit.
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
            if very:
                is_sig = tp.is_significant_evolution(summary.ranking, evo, very=True)
            else:
                is_sig = tp.is_significant_evolution(summary.ranking, evo, very=False)
            if not is_sig:
                continue
            direction = tp.evolution_direction(evo)
            item = {
                "player_id": pid,
                "player_name": summary.player_name,
                "ranking": summary.ranking,
                "evolution": evo,
                "direction": direction,
                "ranked_last_week": summary.ranked_last_week,
                "ranked_last_year": summary.ranked_last_year,
                "ranked_beginning_year": summary.ranked_beginning_year,
                "ever_ranked": summary.ever_ranked,
            }
            bucket[direction].append(item)
        bucket["rise"].sort(key=lambda x: (-abs(x["evolution"]), x["ranking"]))
        bucket["drop"].sort(key=lambda x: (-abs(x["evolution"]), x["ranking"]))
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
    for pid, entry in relevant_players.items():
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
        "stats": stats,
    }


def aggregate_country_payload(
    country_code: str,
    country_name: str,
    period_participations: Dict[str, List[tp.Participation]],
    period_summaries: Dict[str, Dict[str, tp.PlayerSummary]],
    ranking_maps: Dict[str, Dict[str, Dict[str, Any]]],
) -> Dict[str, Any]:
    payload = {
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


def build_country_rank_tables(year_country_participations: Dict[str, List[tp.Participation]], year_country_summaries: Dict[str, Dict[str, tp.PlayerSummary]]) -> Dict[str, Dict[str, Any]]:
    # Build one coherent list of players per country across both circuits for the year.
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

    # Player-level tables are also useful for debugging and downstream ranking.
    def player_table(period_map: Dict[str, Dict[str, tp.PlayerSummary]]) -> Dict[str, Any]:
        rows: List[Dict[str, Any]] = []
        for circuit, player_map in period_map.items():
            for summary in player_map.values():
                rows.append(tp.player_summary_to_dict(summary))
        rows.sort(key=lambda x: (x["circuit"], x["ranking"] if x["ranking"] is not None else 10**9, x["player_name"]))
        return {"players": rows, "count": len(rows)}

    tp.json_dump(players_dir / "weekly_players.json", player_table(weekly_player_summaries))
    tp.json_dump(players_dir / "current_year_players.json", player_table(year_player_summaries))
    tp.json_dump(output_dir / "country_rankings.json", {
        "countries": [
            {"country_code": cc, **info}
            for cc, info in sorted(country_rankings.items(), key=lambda item: item[1].get("mass_rank", 10**9))
        ]
    })


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

    # Weekly period: previous ISO week.
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

    # Year-to-date period.
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

    # Country collections.
    all_country_codes = set()
    for circuit in ["ATP", "WTA"]:
        all_country_codes.update(p.country_code for p in weekly_participations[circuit] if p.country_code)
        all_country_codes.update(p.country_code for p in ytd_participations[circuit] if p.country_code)
        all_country_codes.update(code for code in (tp.clean_str(entry.get("country_name"), default="") for entry in ranking_maps[circuit].values()) if code)

    # Build by country, by period.
    weekly_by_country: Dict[str, List[tp.Participation]] = defaultdict(list)
    ytd_by_country: Dict[str, List[tp.Participation]] = defaultdict(list)
    weekly_summaries_by_country: Dict[str, Dict[str, tp.PlayerSummary]] = defaultdict(dict)
    ytd_summaries_by_country: Dict[str, Dict[str, tp.PlayerSummary]] = defaultdict(dict)
    country_name_lookup: Dict[str, str] = {}

    for circuit in ["ATP", "WTA"]:
        for p in weekly_participations[circuit]:
            weekly_by_country[p.country_code].append(p)
            country_name_lookup.setdefault(p.country_code, p.country_name)
        for p in ytd_participations[circuit]:
            ytd_by_country[p.country_code].append(p)
            country_name_lookup.setdefault(p.country_code, p.country_name)
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

    # Year-only rankings.
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
