#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import re
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple


def read_json_tolerant(path: Path) -> Any:
    if not path.exists():
        return None
    text = path.read_text(encoding="utf-8")
    try:
        return json.loads(text)
    except Exception:
        try:
            sanitized = re.sub(r",\s*(\]|})", r"\1", text)
            sanitized = re.sub(r"^[ \t]*,[ \t]*$", "", sanitized, flags=re.MULTILINE)
            return json.loads(sanitized)
        except Exception as e:
            print(f"[WARN] Impossible de parser {path}: {e}")
            return None


def write_json(path: Path, data: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(data, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )


def load_existing_players(path: Path) -> Dict[str, Dict[str, Any]]:
    """
    Supporte:
      - une liste [{"player_id":..., ...}, ...]
      - un dict {"player_id": {...}, ...}
      - fichier absent => {}
    """
    raw = read_json_tolerant(path)
    players: Dict[str, Dict[str, Any]] = {}

    if raw is None:
        return players

    if isinstance(raw, list):
        iterable = raw
    elif isinstance(raw, dict):
        # Si c'est déjà indexé par player_id
        if all(isinstance(v, dict) for v in raw.values()):
            iterable = []
            for k, v in raw.items():
                item = dict(v)
                item.setdefault("player_id", str(k))
                iterable.append(item)
        else:
            iterable = []
    else:
        return players

    for item in iterable:
        if not isinstance(item, dict):
            continue
        pid = str(item.get("player_id", "")).strip()
        if not pid:
            continue
        pname = str(item.get("player_name", "")).strip()

        tournaments = item.get("tournaments", [])
        if not isinstance(tournaments, list):
            tournaments = []

        normalized_tournaments = []
        seen = set()
        for t in tournaments:
            if not isinstance(t, dict):
                continue
            tid = str(t.get("tourney_id", "")).strip()
            year = t.get("year", None)
            key = (tid, str(year) if year is not None else "")
            if key in seen:
                continue
            seen.add(key)
            normalized_tournaments.append(t)

        players[pid] = {
            "player_id": pid,
            "player_name": pname,
            "tournaments": normalized_tournaments,
            "_seen": seen,
        }

    return players


def final_match_id_for_source(source: str) -> str:
    source = (source or "").upper().strip()
    if source == "ATP":
        return "MS001"
    return "LS001"


def extract_winner_and_tournament(data: Dict[str, Any]) -> Optional[Tuple[str, str, str, Dict[str, Any]]]:
    meta = data.get("meta") or {}
    source = str(meta.get("source", "")).upper().strip()
    if source not in {"ATP", "WTA"}:
        return None

    target_match_id = final_match_id_for_source(source)
    matches = data.get("matches", [])
    if not isinstance(matches, list):
        return None

    final_match = None
    for match in matches:
        if isinstance(match, dict) and match.get("match_id") == target_match_id:
            final_match = match
            break

    if not final_match:
        return None

    winner_id = str(final_match.get("player_id_winner", "")).strip()
    winner_name = str(final_match.get("winner_player_name", "")).strip()
    if not winner_id or not winner_name:
        return None

    tournament_entry: Dict[str, Any] = {
        "tourney_id": str(meta.get("tourney_id", "")).strip(),
        "tourney_name": meta.get("tourney_name") or "",
        "level": meta.get("level") or "",
        "geocode": meta.get("geocode", None),
    }

    # Ajout de year pour éviter les collisions entre saisons.
    # Si tu ne veux pas le garder en sortie, supprime ces 4 lignes.
    year = meta.get("year", None)
    if year is not None and str(year).strip() != "":
        try:
            tournament_entry["year"] = int(year)
        except Exception:
            tournament_entry["year"] = year

    return source, winner_id, winner_name, tournament_entry


def merge_entry(players: Dict[str, Dict[str, Any]], player_id: str, player_name: str, tournament_entry: Dict[str, Any]) -> None:
    if player_id not in players:
        players[player_id] = {
            "player_id": player_id,
            "player_name": player_name,
            "tournaments": [],
            "_seen": set(),
        }

    record = players[player_id]
    if not record.get("player_name") and player_name:
        record["player_name"] = player_name

    tid = str(tournament_entry.get("tourney_id", "")).strip()
    year = tournament_entry.get("year", None)
    key = (tid, str(year) if year is not None else "")

    if key in record["_seen"]:
        return

    record["_seen"].add(key)
    record["tournaments"].append(tournament_entry)


def sort_tournaments(tournaments: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    def key_fn(t: Dict[str, Any]):
        year = t.get("year", 10**9)
        try:
            year = int(year)
        except Exception:
            year = 10**9
        return (
            year,
            str(t.get("tourney_id", "")),
            str(t.get("tourney_name", "")),
        )

    return sorted(tournaments, key=key_fn)


def process_base(json_base: Path) -> Dict[str, Dict[str, Any]]:
    players_by_source: Dict[str, Dict[str, Dict[str, Any]]] = {
        "ATP": {},
        "WTA": {},
    }

    tournament_files = sorted(json_base.rglob("tournament.json"))

    for tpath in tournament_files:
        data = read_json_tolerant(tpath)
        if not isinstance(data, dict):
            continue

        extracted = extract_winner_and_tournament(data)
        if not extracted:
            continue

        source, winner_id, winner_name, tournament_entry = extracted
        if source not in players_by_source:
            continue

        merge_entry(players_by_source[source], winner_id, winner_name, tournament_entry)

    return players_by_source


def finalize_players_map(players_map: Dict[str, Dict[str, Any]]) -> List[Dict[str, Any]]:
    out = []
    for pid, record in players_map.items():
        tournaments = sort_tournaments(record.get("tournaments", []))
        out.append(
            {
                "player_id": record.get("player_id", pid),
                "player_name": record.get("player_name", ""),
                "tournaments": tournaments,
            }
        )

    out.sort(key=lambda x: (str(x.get("player_name", "")).lower(), str(x.get("player_id", ""))))
    return out


def update_output_file(existing_path: Path, players_map: Dict[str, Dict[str, Any]]) -> None:
    existing_players = load_existing_players(existing_path)

    # Fusion des données existantes + nouvelles
    for pid, record in existing_players.items():
        if pid not in players_map:
            players_map[pid] = record
            continue

        # Merge des entrées existantes
        if not players_map[pid].get("player_name") and record.get("player_name"):
            players_map[pid]["player_name"] = record["player_name"]

        for t in record.get("tournaments", []):
            if not isinstance(t, dict):
                continue
            tid = str(t.get("tourney_id", "")).strip()
            year = t.get("year", None)
            key = (tid, str(year) if year is not None else "")
            if key not in players_map[pid]["_seen"]:
                players_map[pid]["_seen"].add(key)
                players_map[pid]["tournaments"].append(t)

    final_list = finalize_players_map(players_map)
    write_json(existing_path, final_list)
    print(f"[OK] Écrit: {existing_path} ({len(final_list)} joueurs)")


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--json-base", required=True, help="Base des tournament.json, ex: docs/data/tournaments/json_by_tournaments")
    ap.add_argument("--out-wta", required=True, help="Sortie WTA, ex: docs/tools/wta_players_tournaments_win.json")
    ap.add_argument("--out-atp", required=True, help="Sortie ATP, ex: docs/tools/atp_players_tournaments_win.json")
    args = ap.parse_args()

    json_base = Path(args.json_base)
    out_wta = Path(args.out_wta)
    out_atp = Path(args.out_atp)

    if not json_base.exists():
        raise SystemExit(f"[ERROR] json-base introuvable: {json_base}")

    players_by_source = process_base(json_base)

    update_output_file(out_wta, players_by_source["WTA"])
    update_output_file(out_atp, players_by_source["ATP"])


if __name__ == "__main__":
    main()