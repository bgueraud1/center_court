#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import random
from collections import defaultdict
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo
from typing import Any, Dict, List, Optional, Tuple

PARIS_TZ = ZoneInfo("Europe/Paris")


def read_json(path: Path) -> Any:
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)


def write_json(path: Path, data: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(data, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )


def paris_day_seed() -> str:
    return datetime.now(PARIS_TZ).strftime("%Y-%m-%d")


def load_champion_from_tournament(data: Dict[str, Any]) -> Optional[Tuple[str, str]]:
    """
    Retourne (player_id, player_name) du champion.
    Essaie d'abord meta.champion, puis fallback sur le match final.
    """
    meta = data.get("meta") or {}
    champ = meta.get("champion") or {}
    if isinstance(champ, dict):
        pid = str(champ.get("player_id_winner", "")).strip()
        name = str(champ.get("winner_player_name", "")).strip()
        if pid and name:
            return pid, name

    matches = data.get("matches") or []
    if not isinstance(matches, list):
        return None

    # fallback : match final
    source = str(meta.get("source", "")).upper().strip()
    final_id = "LS001" if source == "WTA" else "MS001"

    for m in matches:
        if not isinstance(m, dict):
            continue
        if m.get("match_id") == final_id:
            pid = str(m.get("player_id_winner", "")).strip()
            name = str(m.get("winner_player_name", "")).strip()
            if pid and name:
                return pid, name

    return None


def collect_players(json_base: Path) -> Dict[str, Dict[str, Any]]:
    """
    Construit:
      {
        "ATP": {
          player_id: {
            "player_id": ...,
            "player_name": ...,
            "tournaments": [...]
          }
        },
        "WTA": {...}
      }
    """
    out: Dict[str, Dict[str, Dict[str, Any]]] = {
        "ATP": {},
        "WTA": {},
    }

    files = sorted(json_base.rglob("tournament.json"))
    for tpath in files:
        try:
            data = read_json(tpath)
        except Exception:
            continue

        if not isinstance(data, dict):
            continue

        meta = data.get("meta") or {}
        if not isinstance(meta, dict):
            continue

        source = str(meta.get("source", "")).upper().strip()
        if source not in {"ATP", "WTA"}:
            continue

        champion = load_champion_from_tournament(data)
        if not champion:
            continue

        player_id, player_name = champion

        geocode = meta.get("geocode")
        if not (isinstance(geocode, (list, tuple)) and len(geocode) >= 2):
            continue

        try:
            lat = float(geocode[0])
            lon = float(geocode[1])
        except Exception:
            continue

        tournament_entry = {
            "tourney_id": str(meta.get("tourney_id", "")).strip(),
            "tourney_name": str(meta.get("tourney_name", "")).strip(),
            "city": str(meta.get("city", "")).strip(),
            "country": str(meta.get("country", "")).strip(),  # IOC code 3 lettres le plus souvent
            "geocode": [lat, lon],
            "year": meta.get("year", None),
            "level": meta.get("level", ""),
            "surface": meta.get("surface", ""),
        }

        if player_id not in out[source]:
            out[source][player_id] = {
                "player_id": player_id,
                "player_name": player_name,
                "tournaments": [],
            }

        out[source][player_id]["tournaments"].append(tournament_entry)

    return out


def distinct_countries(tournaments: List[Dict[str, Any]]) -> List[str]:
    countries = []
    for t in tournaments:
        c = str(t.get("country", "")).strip().upper()
        if c:
            countries.append(c)
    return sorted(set(countries))


def choose_player(players: Dict[str, Dict[str, Any]], seed: str, circuit: str) -> Dict[str, Any]:
    """
    Sélection déterministe par jour.
    On privilégie les joueurs avec plusieurs pays distincts, sinon fallback.
    """
    all_players = list(players.values())
    if not all_players:
        raise ValueError(f"Aucun joueur trouvé pour {circuit}")

    enriched = []
    for p in all_players:
        tc = distinct_countries(p.get("tournaments", []))
        enriched.append((p, len(tc), len(p.get("tournaments", []))))

    # On essaie de garder des profils intéressants
    preferred = [x for x in enriched if x[1] >= 2]
    pool = preferred if preferred else enriched

    # Trier de façon stable puis mélanger avec seed
    pool = sorted(pool, key=lambda x: (x[0]["player_name"].lower(), x[0]["player_id"]))
    rng = random.Random(f"{seed}:{circuit}")
    rng.shuffle(pool)

    return pool[0][0]


def build_payload(player: Dict[str, Any], circuit: str) -> Dict[str, Any]:
    tournaments = sorted(
        player.get("tournaments", []),
        key=lambda t: (
            str(t.get("year", 9999)),
            str(t.get("tourney_name", "")),
            str(t.get("tourney_id", "")),
        )
    )
    countries = distinct_countries(tournaments)

    return {
        "player_id": player["player_id"],
        "player_name": player["player_name"],
        "countries_count": len(countries),
        "countries": countries,
        "tournaments": tournaments,
        "circuit": circuit,
    }


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument(
        "--json-base",
        required=True,
        help="Base des tournament.json, ex: docs/data/tournaments/json_by_tournaments"
    )
    ap.add_argument(
        "--out",
        required=True,
        help="Fichier de sortie, ex: ${BUILD_DIR}/tools/daily_country_guess.json"
    )
    args = ap.parse_args()

    json_base = Path(args.json_base)
    out_path = Path(args.out)

    if not json_base.exists():
        raise SystemExit(f"[ERROR] json-base introuvable: {json_base}")

    players_by_circuit = collect_players(json_base)

    seed = paris_day_seed()

    atp_player = choose_player(players_by_circuit["ATP"], seed, "ATP")
    wta_player = choose_player(players_by_circuit["WTA"], seed, "WTA")

    payload = {
        "game": "daily_country_guess",
        "generated_at_paris": datetime.now(PARIS_TZ).isoformat(),
        "date_paris": seed,
        "atp": build_payload(atp_player, "ATP"),
        "wta": build_payload(wta_player, "WTA"),
        "scoring": {
            "max_points_total": 100,
            "max_points_per_circuit": 50,
            "points_per_correct_country_formula": "50 / n where n = number of distinct countries for that circuit"
        }
    }

    write_json(out_path, payload)
    print(f"[OK] Écrit: {out_path}")


if __name__ == "__main__":
    main()