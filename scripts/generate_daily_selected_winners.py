#!/usr/bin/env python3
from __future__ import annotations

import argparse
import copy
import json
import random
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Set, Tuple


def read_json(path: Path) -> Any:
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)


def write_json(path: Path, data: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")


def load_players(path: Path, source: str) -> List[Dict[str, Any]]:
    """
    Charge une liste de joueurs/joueuses.
    Supporte:
      - une liste de dicts
      - un dict indexé par player_id
    """
    data = read_json(path)

    out: List[Dict[str, Any]] = []

    if isinstance(data, list):
        for item in data:
            if isinstance(item, dict):
                player = copy.deepcopy(item)
                player.setdefault("source", source)
                out.append(player)
        return out

    if isinstance(data, dict):
        for k, v in data.items():
            if isinstance(v, dict):
                player = copy.deepcopy(v)
                player.setdefault("player_id", str(k))
                player.setdefault("source", source)
                out.append(player)
        return out

    raise ValueError(f"Format JSON inattendu dans {path}")


def normalize_text(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, (int, float, bool)):
        return str(value).strip().lower()
    if isinstance(value, str):
        return value.strip().lower()
    return str(value).strip().lower()


def iter_tournaments(player: Dict[str, Any]) -> List[Dict[str, Any]]:
    tournaments = player.get("tournaments", [])
    if not isinstance(tournaments, list):
        return []
    return [t for t in tournaments if isinstance(t, dict)]


def tournament_has_non_empty_geocode(tournament: Dict[str, Any]) -> bool:
    geocode = tournament.get("geocode")
    if isinstance(geocode, list):
        return len([x for x in geocode if normalize_text(x)]) > 0
    if isinstance(geocode, dict):
        return any(normalize_text(v) for v in geocode.values())
    return bool(normalize_text(geocode))


def eligible_player(player: Dict[str, Any]) -> bool:
    tournaments = iter_tournaments(player)
    count = sum(1 for t in tournaments if tournament_has_non_empty_geocode(t))
    return count >= 3


def filter_eligible_players(players: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    return [p for p in players if eligible_player(p)]


def player_key(player: Dict[str, Any]) -> str:
    return str(
        player.get("player_id")
        or player.get("id")
        or player.get("slug")
        or player.get("name")
        or player.get("full_name")
        or id(player)
    )


def extract_locations_from_tournament(tournament: Dict[str, Any]) -> Set[str]:
    locations: Set[str] = set()

    for key in ("location", "city", "country", "venue", "place", "name"):
        val = tournament.get(key)
        if isinstance(val, str) and val.strip():
            locations.add(normalize_text(val))

    geocode = tournament.get("geocode")
    if isinstance(geocode, list):
        for item in geocode:
            if isinstance(item, dict):
                for k in ("city", "country", "name", "label", "location", "venue"):
                    v = item.get(k)
                    if isinstance(v, str) and v.strip():
                        locations.add(normalize_text(v))
                for v in item.values():
                    if isinstance(v, str) and v.strip():
                        locations.add(normalize_text(v))
            else:
                txt = normalize_text(item)
                if txt:
                    locations.add(txt)

    elif isinstance(geocode, dict):
        for v in geocode.values():
            txt = normalize_text(v)
            if txt:
                locations.add(txt)

    elif geocode is not None:
        txt = normalize_text(geocode)
        if txt:
            locations.add(txt)

    return locations


def extract_level_from_tournament(tournament: Dict[str, Any]) -> str:
    for key in ("level", "tour_level", "tournament_level", "category", "tier", "grade"):
        val = tournament.get(key)
        txt = normalize_text(val)
        if txt:
            return txt
    return ""


def player_profile(player: Dict[str, Any]) -> Dict[str, Any]:
    tournaments = iter_tournaments(player)

    locations: Set[str] = set()
    levels: Set[str] = set()

    valid_tournaments = 0
    for t in tournaments:
        if tournament_has_non_empty_geocode(t):
            valid_tournaments += 1
        locations.update(extract_locations_from_tournament(t))
        lvl = extract_level_from_tournament(t)
        if lvl:
            levels.add(lvl)

    return {
        "tournament_count": valid_tournaments,
        "locations": locations,
        "levels": levels,
    }


def similarity_score(a: Dict[str, Any], b: Dict[str, Any]) -> Tuple[int, Dict[str, Any]]:
    pa = player_profile(a)
    pb = player_profile(b)

    count_a = pa["tournament_count"]
    count_b = pb["tournament_count"]
    diff = abs(count_a - count_b)

    common_locations = sorted(pa["locations"] & pb["locations"])
    common_levels = sorted(pa["levels"] & pb["levels"])

    score = 0
    score += max(0, 30 - diff * 3)
    score += min(20, len(common_locations) * 8)
    score += min(15, len(common_levels) * 5)

    reasons = {
        "tournament_count_a": count_a,
        "tournament_count_b": count_b,
        "count_diff": diff,
        "common_locations": common_locations,
        "common_levels": common_levels,
    }

    return score, reasons


def pick_selection_split(
    eligible_atp: List[Dict[str, Any]],
    eligible_wta: List[Dict[str, Any]],
    rng: random.Random,
) -> Tuple[int, int]:
    options = [(3, 2), (2, 3)]
    rng.shuffle(options)

    for nb_atp, nb_wta in options:
        if len(eligible_atp) >= nb_atp and len(eligible_wta) >= nb_wta:
            return nb_atp, nb_wta

    raise ValueError(
        "Impossible de sélectionner 5 personnes au total: "
        f"ATP éligibles={len(eligible_atp)}, WTA éligibles={len(eligible_wta)}."
    )


def select_mixed_five(
    eligible_atp: List[Dict[str, Any]],
    eligible_wta: List[Dict[str, Any]],
    rng: random.Random,
) -> Tuple[List[Dict[str, Any]], Dict[str, int]]:
    nb_atp, nb_wta = pick_selection_split(eligible_atp, eligible_wta, rng)

    selected_atp = rng.sample(eligible_atp, nb_atp)
    selected_wta = rng.sample(eligible_wta, nb_wta)

    selected = selected_atp + selected_wta
    rng.shuffle(selected)

    return selected, {"atp": nb_atp, "wta": nb_wta}


def find_close_players(
    player: Dict[str, Any],
    pool: List[Dict[str, Any]],
    excluded_keys: Set[str],
    rng: random.Random,
    top_n: int = 3,
) -> List[Dict[str, Any]]:
    candidates: List[Dict[str, Any]] = []
    self_key = player_key(player)

    for other in pool:
        other_key = player_key(other)
        if other_key == self_key:
            continue
        if other_key in excluded_keys:
            continue

        score, reasons = similarity_score(player, other)
        candidate = copy.deepcopy(other)
        candidate["similarity_score"] = score
        candidate["similarity_reasons"] = reasons
        candidates.append(candidate)

    if len(candidates) < top_n:
        raise ValueError(
            f"Pas assez de profils proches disponibles pour {self_key}: "
            f"{len(candidates)} trouvés, {top_n} requis."
        )

    rng.shuffle(candidates)
    candidates.sort(key=lambda x: x["similarity_score"], reverse=True)

    return candidates[:top_n]


def attach_close_players(
    selected: List[Dict[str, Any]],
    eligible_atp: List[Dict[str, Any]],
    eligible_wta: List[Dict[str, Any]],
    rng: random.Random,
) -> List[Dict[str, Any]]:
    selected_keys = {player_key(p) for p in selected}
    out: List[Dict[str, Any]] = []

    for player in selected:
        source = player.get("source")
        if source == "ATP":
            pool = eligible_atp
        elif source == "WTA":
            pool = eligible_wta
        else:
            pool = eligible_atp + eligible_wta

        close_players = find_close_players(
            player=player,
            pool=pool,
            excluded_keys=selected_keys,
            rng=rng,
            top_n=3,
        )

        item = copy.deepcopy(player)
        item["close_players"] = close_players
        out.append(item)

    return out


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--wta", required=True, help="JSON source WTA, ex: docs/tools/wta_players_tournaments_win.json")
    ap.add_argument("--atp", required=True, help="JSON source ATP, ex: docs/tools/atp_players_tournaments_win.json")
    ap.add_argument("--out", required=True, help="JSON de sortie, ex: ${BUILD_DIR}/tools/daily_selected_players.json")
    args = ap.parse_args()

    wta_path = Path(args.wta)
    atp_path = Path(args.atp)
    out_path = Path(args.out)

    rng = random.SystemRandom()

    wta_players = load_players(wta_path, "WTA")
    atp_players = load_players(atp_path, "ATP")

    eligible_wta = filter_eligible_players(wta_players)
    eligible_atp = filter_eligible_players(atp_players)

    selected, split = select_mixed_five(eligible_atp, eligible_wta, rng)
    selected_with_close_players = attach_close_players(selected, eligible_atp, eligible_wta, rng)

    payload = {
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "selection_split": split,
        "selected_players": selected_with_close_players,
    }

    write_json(out_path, payload)
    print(f"[OK] Écrit: {out_path}")


if __name__ == "__main__":
    main()