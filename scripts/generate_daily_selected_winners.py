#!/usr/bin/env python3
from __future__ import annotations

import argparse
import copy
import json
import random
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List


DIFFICULTIES = ["Easy", "Medium", "Hard"]


def read_json(path: Path) -> Any:
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)


def write_json(path: Path, data: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")


def load_players(path: Path) -> List[Dict[str, Any]]:
    data = read_json(path)
    if isinstance(data, list):
        return [x for x in data if isinstance(x, dict)]
    if isinstance(data, dict):
        # supporte aussi un dict indexé par player_id
        out = []
        for k, v in data.items():
            if isinstance(v, dict):
                item = copy.deepcopy(v)
                item.setdefault("player_id", str(k))
                out.append(item)
        return out
    raise ValueError(f"Format JSON inattendu dans {path}")


def pick_three_with_difficulties(players: List[Dict[str, Any]], rng: random.Random) -> List[Dict[str, Any]]:
    if len(players) < 3:
        raise ValueError(f"Il faut au moins 3 joueurs/joueuses, trouvé {len(players)}")

    selected = rng.sample(players, 3)
    difficulties = DIFFICULTIES[:]
    rng.shuffle(difficulties)

    out = []
    for player, difficulty in zip(selected, difficulties):
        item = copy.deepcopy(player)
        item["difficulty"] = difficulty
        out.append(item)

    return out


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--wta", required=True, help="JSON source WTA, ex: docs/tools/wta_players_tournaments_win.json")
    ap.add_argument("--atp", required=True, help="JSON source ATP, ex: docs/tools/atp_players_tournaments_win.json")
    ap.add_argument("--out", required=True, help="JSON de sortie, ex: ${BUILD_DIR}/tools/daily_selected_winners.json")
    args = ap.parse_args()

    wta_path = Path(args.wta)
    atp_path = Path(args.atp)
    out_path = Path(args.out)

    rng = random.SystemRandom()

    wta_players = load_players(wta_path)
    atp_players = load_players(atp_path)

    payload = {
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "wta": pick_three_with_difficulties(wta_players, rng),
        "atp": pick_three_with_difficulties(atp_players, rng),
    }

    write_json(out_path, payload)
    print(f"[OK] Écrit: {out_path}")


if __name__ == "__main__":
    main()