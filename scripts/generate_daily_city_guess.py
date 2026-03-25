#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import random
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo


PARIS_TZ = ZoneInfo("Europe/Paris")


def read_json(path: Path):
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)


def write_json(path: Path, data):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")


def paris_day_key() -> str:
    return datetime.now(PARIS_TZ).strftime("%Y-%m-%d")


def tournament_entries(json_base: Path):
    """
    Parcourt tous les tournament.json sous json_base et retourne une liste d’entrées normalisées.
    On garde seulement les tournois ayant un geocode exploitable.
    """
    entries = []

    for tpath in sorted(json_base.rglob("tournament.json")):
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

        geocode = meta.get("geocode")
        if not (isinstance(geocode, (list, tuple)) and len(geocode) >= 2):
            continue

        try:
            lat = float(geocode[0])
            lon = float(geocode[1])
        except Exception:
            continue

        tourney_id = str(meta.get("tourney_id", "")).strip()
        year = meta.get("year", None)
        tourney_name = str(meta.get("tourney_name", "")).strip()
        city_raw = str(meta.get("city", "")).strip()

        # Pour le jeu, on affiche la ville; si elle est vide, on prend le nom du tournoi
        city = city_raw or tourney_name or ""

        entries.append({
            "source": source,
            "tourney_id": tourney_id,
            "year": year,
            "tourney_name": tourney_name,
            "city": city,
            "city_raw": city_raw,
            "level": meta.get("level", ""),
            "surface": meta.get("surface", ""),
            "geocode": [lat, lon],
        })

    return entries


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument(
        "--json-base",
        required=True,
        help="Base des per-tournament JSON, ex: docs/data/tournaments/json_by_tournaments"
    )
    ap.add_argument(
        "--out",
        required=True,
        help="Fichier de sortie, ex: ${BUILD_DIR}/tools/daily_city_guess.json"
    )
    ap.add_argument(
        "--count",
        type=int,
        default=5,
        help="Nombre de tournois à sélectionner (défaut: 5)"
    )
    args = ap.parse_args()

    json_base = Path(args.json_base)
    out_path = Path(args.out)

    if not json_base.exists():
        raise SystemExit(f"[ERROR] json-base introuvable: {json_base}")

    pool = tournament_entries(json_base)
    if not pool:
        raise SystemExit("[ERROR] Aucun tournoi exploitable trouvé (source/geo manquants).")

    # Sélection déterministe par jour (Europe/Paris)
    rng = random.Random(paris_day_key())
    rng.shuffle(pool)
    selected = pool[: min(args.count, len(pool))]

    payload = {
        "generated_at_paris": datetime.now(PARIS_TZ).isoformat(),
        "date_paris": paris_day_key(),
        "game": "daily_city_guess",
        "rounds_total": len(selected),
        "scoring": {
            "max_points_per_round": 20,
            "distance_full_points_km": 20,
            "distance_zero_points_km": 5000
        },
        "rounds": []
    }

    for idx, item in enumerate(selected, start=1):
        payload["rounds"].append({
            "round_index": idx,
            "source": item["source"],
            "tourney_id": item["tourney_id"],
            "year": item["year"],
            "tourney_name": item["tourney_name"],
            "city": item["city"],
            "city_raw": item["city_raw"],
            "level": item["level"],
            "surface": item["surface"],
            "geocode": item["geocode"]
        })

    write_json(out_path, payload)
    print(f"[OK] Écrit: {out_path} ({len(selected)} tournois)")


if __name__ == "__main__":
    main()